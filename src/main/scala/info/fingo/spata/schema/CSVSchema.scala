/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import scala.annotation.unused
import scala.reflect.{classTag, ClassTag}
import cats.data.{NonEmptyList, Validated}
import cats.data.Validated.{Invalid, Valid}
import fs2.{Pipe, Stream}
import info.fingo.spata.Record
import info.fingo.spata.schema.error.TypeError
import info.fingo.spata.schema.validator.Validator
import info.fingo.spata.text.StringParser
import info.fingo.spata.util.Logger

/** CSV schema definition and validation utility.
  *
  * Schema declares fields which are expected in CSV stream - their names and types.
  * Fields with optional values have to be defined as `Option`s:
  * ```
  * val schema = CSVSchema()
  *   .add[String]("name")
  *   .add[Option[LocalDate]]("birthday")
  * ```
  * Optional fields still have to exist in source data, only their values may be empty.
  * Not all fields have to be declared by schema, any subset of them is sufficient.
  *
  * In case of header mapping through [[CSVConfig.mapHeader]],
  * the names provided in schema are the final ones, after mapping.
  *
  * Additionally, it is possible to specify per-field validators, posing additional requirements on CSV data:
  * ```
  * val schema = CSVSchema()
  *   .add[String]("code", RegexValidator("[A-Z][A-Z0-9]+"))
  *   .add[BigDecimal]("price", MinValidator(0.01))
  * ```
  * For more information on available, built-in validators
  * or how to create additional ones see [[validator.Validator Validator]].
  *
  * CSV schema is verified through its [[validate]] method. It may yield an [[InvalidRecord]],
  * containing validation error together with original [[Record]] data or a [[TypedRecord]],
  * containing selected, strongly typed data - in both cases wrapped in [[cats.data.Validated]].
  *
  * @param columns the typle containing typed columns with optional validators
  * @tparam T tuple encoding the schema
  */
final class CSVSchema[T <: Tuple] private (columns: T):

  /** Gets string representation of schema.
    *
    * @return short schema description
    */
  override def toString: String = "CSVSchema" + columns.toString()

  /** Adds field definition to schema.
    *
    * Field definition consists of field name and its type. A set of field definitions constitutes a schema definition.
    * A collection of additional [[validator.Validator Validator]]s may be added to a field.
    * When validating schema, validators are checked after field type verification
    * and receive already parsed value of type declared for a field.
    *
    * To get value of proper type from a field, an implicit [[text.StringParser StringParser]] is required.
    * Parsers for basic types and formats are available through [[text.StringParser$ StringParser]] object.
    * Additional ones may be provided by implementing the `StringParser` trait.
    *
    * Optional values should be denoted by providing `Option[A]` as field type value.
    * Note, that even optionals require the field to be present in the source data,
    * only its values may be missing (empty).
    *
    * The same validators, which are used to validate plain values, may be used to verify optional values.
    * Missing value (`None`) is assumed correct in such a case.
    *
    * This is a chaining method which allows starting with an empty schema
    * and extending it through subsequent calls to `add`:
    * ```
    * val schema = CSVSchema()
    *   .add[Double]("latitude", RangeValidator(-90.0, 90.0))
    *   .add[Double]("longitude", RangeValidator(-180.0, 180.0))
    * ```
    *
    * @param key unique field name - a singleton string
    * @param validators optional validators to check that field values comply with additional rules
    * @param ev evidence that the key is unique - it is not present in the schema yet
    * @tparam V field value type
    * @return new schema definition with column (field definition) added to it
    */
  def add[V: ClassTag: StringParser](key: Key, validators: Validator[V]*)(using
    @unused ev: NotPresent[key.type, T]
  ): CSVSchema[Column[key.type, V] *: T] =
    new CSVSchema[Column[key.type, V] *: T](Column(key, validators) *: columns)

  /** Adds field definition to schema. Does not support attaching additional validators.
    *
    * @see [[add(key:Key,* add[V](key: Key, validators: Validator[V]*)]]
    * @param key unique field name - a singleton string
    * @param ev evidence that the key is unique - it is not present in the schema yet
    * @tparam V field value type
    * @return new schema definition with column (field definition) added to it
    */
  def add[V: ClassTag: StringParser](key: Key)(using
    @unused ev: NotPresent[key.type, T]
  ): CSVSchema[Column[key.type, V] *: T] =
    new CSVSchema[Column[key.type, V] *: T](Column(key) *: columns)

  /** Validates CSV stream against schema.
    *
    * For each input record the validation process:
    *  - parses all fields defined by schema to the declared type and, if successful,
    *  - runs provided validators with parsed values.
    * The process is successful and creates a [[TypedRecord]] if values of all fields defined in schema
    * are correctly parsed and positively validated. If any of these operations fails, an [[InvalidRecord]] is yielded.
    *
    * If there are many validators defined for single field, the validation stops at first invalid result.
    * Validation is nonetheless executed for all fields and collects errors from all of them.
    *
    * CSV values which are not declared in schema are omitted.
    * At the extremes, empty schema always proves valid, although yields empty typed records.
    *
    * @param enforcer given value to recursively do the validation, provided by spata
    * @tparam F the effect type, with a type class providing support for logging (provided internally by spata)
    * @return a pipe to validate [[Record]]s and turn them into [[ValidatedRecord]]s
    */
  def validate[F[_]: Logger](using enforcer: SchemaEnforcer[T]): Pipe[F, Record, enforcer.Out] =
    in => in.through(loggingPipe).map(r => validateRecord(r, enforcer))

  /* Validate single record against schema. */
  private def validateRecord(r: Record, enforcer: SchemaEnforcer[T]): enforcer.Out = enforcer(columns, r)

  /* Add logging information to validation pipe. */
  private def loggingPipe[F[_]: Logger]: Pipe[F, Record, Record] = in =>
    if (Logger[F].isDebug)
      // interleave is used to insert validation log entry after entries from CSVParser, >> inserts it at the beginning
      in.interleaveAll(Stream.exec(Logger[F].debug(s"Validating CSV with $this")).covaryOutput[Record])
    else in

/** [[CSVSchema]] companion with convenience method to create empty schema. */
object CSVSchema:

  /** Creates empty schema.
    *
    * Schema created this way should be extended using [[CSVSchema.add add]].
    *
    * @return schema with no field definition
    */
  def apply(): CSVSchema[EmptyTuple] = new CSVSchema(Tuple())

/** CSV column representing schema field definition.
  *
  * Columns are created by the schema [[CSVSchema.add add]] method.
  *
  * @see [[CSVSchema]]
  * @param name the name of column - CSV field name
  * @param validators collection of optional validators for field value
  * @tparam K type of column name - the singleton string
  * @tparam V column type - CSV field value type
  */
final class Column[K <: Key, V: StringParser: ClassTag] private[schema] (val name: K, validators: Seq[Validator[V]]):

  /* Constructor for column without custom validators. */
  private[schema] def this(name: K) = this(name, Seq.empty)

  /** Gets string representation of column.
    *
    * @return short column description
    */
  override def toString: String =
    val vInfo = if (validators.isEmpty) "" else validators.map(_.name).mkString(" +", "+", "")
    s"'$name' -> ${classTag[V]}$vInfo"

  /* Validates field. Positively validated values are returned as pair (Tuple2) to encode both, key and value types
   * and to be easily accommodated as typed records. */
  private[schema] def validate(record: Record): Validated[FieldFlaw, (K, V)] =
    // get parsed value from CSV record and convert parsing error to FieldFlaw
    val typeValidated =
      Validated.fromEither(record.get(name)).leftMap(e => FieldFlaw(name, TypeError(e)))
    // call each validator, convert error to FieldFlaw and chain results, short-circuiting on error
    val fullyValidated = typeValidated.andThen(v =>
      validators
        .map(validator => validator(v).leftMap(FieldFlaw(name, _)))
        .foldLeft(typeValidated)((prev, curr) => prev.andThen(_ => curr))
    )
    // convert value to tuple
    fullyValidated.map((name, _))

/** Type alias for validation result. */
type ValidatedRecord[KS <: Tuple, VS <: Tuple] = Validated[InvalidRecord, TypedRecord[KS, VS]]

/** Actual verifier of CSV data. Checks if CSV record is congruent with schema.
  *
  * The verification is achieved through recursive implicits for tuple of columns (as input type)
  * and tuple of typed record data (as output type).
  *
  * @tparam T the tuple (of columns) representing schema
  */
sealed trait SchemaEnforcer[T <: Tuple]:

  /** Output type of the apply method - schema validation core result type: `ValidatedRecord[Tuple, Tuple]`. */
  type Out

  /** Checks if record meets the type and validation rules imposed by column definitions.
    * It is used by [[CSVSchema.validate]] method.
    * Its instances are provided as recursive givens.
    *
    * @param columns the tuple of columns containng schema definition
    * @parem record the CSV record being checked
    * @return [[ValidatedRecord]] paramatrized according to schema definition
    */
  def apply(columns: T, record: Record): Out

/** Givens for [[SchemaEnforcer]]. */
object SchemaEnforcer:

  /** Schema enforcer for empty tuple of columns. */
  given emptyEnforcer: SchemaEnforcer[EmptyTuple] with

    override type Out = ValidatedRecord[EmptyTuple, EmptyTuple]

    override def apply(columns: EmptyTuple, record: Record): Out = empty(record)

  /** Recursive schema enforcer for tuple of columns.
    *
    * @param tailEnforcer schema enforcer for column tuple tail
    * @tparam K type of field name (key) of both, head of column tuple and head of typed record's data
    * @tparam V type of field value of both, head of column tuple and head of typed record's data
    * @tparam KT type of field names (keys) of record's data tail
    * @tparam VT type of values of record's data tail
    * @tparam CT type of column tuple tail
    * @return schema enforcer for tuple
    */
  given recursiveEnforcer[K <: Key, V, KT <: Tuple, VT <: Tuple, CT <: Tuple](using
    tailEnforcer: SchemaEnforcer[CT] { type Out = ValidatedRecord[KT, VT] },
    ev1: Tuple.Size[K *: KT] =:= Tuple.Size[V *: VT],
    ev2: Tuple.Union[K *: KT] <:< Key
  ): SchemaEnforcer[Column[K, V] *: CT] with

    override type Out = ValidatedRecord[K *: KT, V *: VT]

    override def apply(columns: Column[K, V] *: CT, record: Record): Out =
      val head = columns.head
      val validated = head.validate(record)
      val tailValidated = tailEnforcer(columns.tail, record)
      validated match
        // if current value is valid, add it to typed record from tail validation (or do nothing, if tail is invalid)
        case Valid((k, v)) =>
          tailValidated.map(tv => TypedRecord(k *: tv.keys, v *: tv.values, record.lineNum, record.rowNum))
        // if current value is invalid, add it to list of flaws from tail validation
        case Invalid(ff) =>
          val flaws = tailValidated match
            case Valid(_) => NonEmptyList.one(ff)
            case Invalid(ir) => ff :: ir.flaws
          Validated.invalid(InvalidRecord(record, flaws))

  /* Initial validation result for empty column tuple - empty typed record. */
  private def empty(record: Record) =
    Validated.valid[InvalidRecord, TypedRecord[EmptyTuple, EmptyTuple]](
      TypedRecord(Tuple(), Tuple(), record.lineNum, record.rowNum)
    )

/** Proof of column name uniqueness in schema.
  *
  * Witnesses that given candidate key type (singleton string) is not present on column list yet.
  *
  * @tparam K type of column name
  * @tparam T type of tuple of columns
  */
sealed trait NotPresent[K <: Key, T <: Tuple]

/** Givens for [[NotPresent]]. */
object NotPresent:

  // The type inequality snippet is base on com.chuusai.shapeless.=:!=
  /** Type inequalitiy. */
  trait =!=[A, B]

  /** Given for inequal types. */
  given neq[A, B]: =!=[A, B] with {}

  /** Given for equal types - due to duplication impossible to instantiate. */
  given neqContradiction1[A]: =!=[A, A] = sys.error("unexpected invocation")

  /** Given for equal types - due to duplication impossible to instantiate. */
  given neqContradiction2[A]: =!=[A, A] = sys.error("unexpected invocation")

  /** [[NotPresent]] witness for empty column list.
    *
    * @tparam K type of candidate column name
    * @return `NotPresent` for empty schema
    */
  given emptyNotPresent[K <: Key]: NotPresent[K, EmptyTuple] with {}

  /** Recursive [[NotPresent]] witness for tuple.
    *
    * @param neq witnesses that column name type of tuple head differs from the candidate name type
    * @param tailNP `NotPresent` witness for list tail
    * @tparam K type of candidate column name
    * @tparam KH type of tuple head column name
    * @tparam VH type of tuple head column value
    * @tparam T tail of tuple of columns
    * @return `NotPresent` for tuple
    */
  given recursiveNotPresent[K <: Key, KH <: Key, VH, T <: Tuple](using
    @unused neq: K =!= KH,
    @unused tailNP: NotPresent[K, T]
  ): NotPresent[K, Column[KH, VH] *: T] with {}
