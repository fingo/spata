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
import cats.effect.Sync
import fs2.{Pipe, Stream}
import shapeless.{::, =:!=, DepFn2, HList, HNil}
import shapeless.labelled.{field, FieldType}
import info.fingo.spata.Record
import info.fingo.spata.schema.error.TypeError
import info.fingo.spata.schema.validator.Validator
import info.fingo.spata.text.StringParser
import info.fingo.spata.util.{classLabel, Logger}

/** CSV schema definition and validation utility.
  *
  * Schema declares fields which are expected in CSV stream - their names and types.
  * Fields with optional values have to be defined as `Option`s:
  * {{{
  * val schema = CSVSchema()
  *   .add[String]("name")
  *   .add[Option[LocalDate]]("birthday")
  * }}}
  * Not all fields have to be declared by schema but only a subset of them.
  *
  * In case of header mapping through [[CSVConfig.mapHeader]],
  * the names provided in schema are the final ones, after mapping.
  *
  * Additionally, it is possible to specify per-field validators, posing additional requirements on CSV data:
  * {{{
  * val schema = CSVSchema()
  *   .add[String]("code", RegexValidator("[A-Z][A-Z0-9]+"))
  *   .add[BigDecimal]("price", MinValidator(0.01))
  * }}}
  * For more information on available, built-in validators
  * or how to create additional ones see [[validator.Validator Validator]].
  *
  * CSV schema is verified through its [[validate]] method. It may yield an [[InvalidRecord]],
  * containing validation error together with original [[Record]] data or a [[TypedRecord]],
  * containing selected, strongly typed data - in both cases wrapped in [[cats.data.Validated]].
  *
  * @param columns the list of typed columns with optional validators
  * @tparam L heterogeneous list encoding the schema
  */
class CSVSchema[L <: HList] private (columns: L) {

  /** Gets string representation of schema.
    *
    * @return short schema description
    */
  override def toString: String = "CSVSchema" + columns.mkString("(", ", ", ")")

  /** Adds field definition to schema.
    *
    * Field definition consists of field name and its type. Set of fields definitions constitutes schema definition.
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
    * {{{
    * val schema = CSVSchema()
    *   .add[Double]("latitude", RangeValidator(-90, 90))
    *   .add[Double]("longitude", RangeValidator(-180, 180))
    * }}}
    *
    * @param key unique field name - a singleton string
    * @param validators optional validators to check if field values do conform to additional rules
    * @param ev evidence that the key is unique - it is not present in the schema yet
    * @tparam V field value type
    * @return new schema definition with column (field definition) added to it
    */
  def add[V: StringParser: ClassTag](key: Key, validators: Validator[V]*)(
    implicit @unused ev: NotPresent[key.type, L]
  ): CSVSchema[Column[key.type, V] :: L] =
    new CSVSchema[Column[key.type, V] :: L](Column(key, validators) :: columns)

  /** Adds field definition to schema. Does not support attaching additional validators.
    *
    * @see [[add[V](key:info\.fingo\.spata\.schema\.Key,* add[V](key: Key, validators: Validator[V]*)]]
    * @param key unique field name - a singleton string
    * @param ev evidence that the key is unique - it is not present in the schema yet
    * @tparam V field value type
    * @return new schema definition with column (field definition) added to it
    */
  def add[V: StringParser: ClassTag](key: Key)(
    implicit @unused ev: NotPresent[key.type, L]
  ): CSVSchema[Column[key.type, V] :: L] =
    new CSVSchema[Column[key.type, V] :: L](Column(key) :: columns)

  /** Validates CSV stream against schema.
    *
    * For each input record the validation process:
    *  - parses all fields defined by schema to the declared type and, if successful,
    *  - runs provided validators with parsed value.
    * The process is successful and creates a [[TypedRecord]] if values of all fields defined in schema
    * are correctly parsed and positively validated. If any of these operations fails, an [[InvalidRecord]] is yielded.
    *
    * If there are many validators defined for single field, the validation stops at first invalid result.
    * Validation is nonetheless executed for all fields and collects errors from all of them.
    *
    * CSV values which are not declared in schema are omitted.
    * At the extremes, empty schema always proves valid, although yields empty typed records.
    *
    * @param enforcer implicit to recursively do the validation, provided by spata
    * @tparam F the effect type, with a type class providing support for suspended execution
    * (typically [[cats.effect.IO]]) and logging (provided internally by spata)
    * @return a pipe to validate [[Record]]s and turn them into [[ValidatedRecord]]s
    */
  def validate[F[_]: Sync: Logger](implicit enforcer: SchemaEnforcer[L]): Pipe[F, Record, enforcer.Out] =
    in => in.through(loggingPipe).map(r => validateRecord(r)(enforcer))

  /* Validate single record against schema. */
  private def validateRecord(r: Record)(implicit enforcer: SchemaEnforcer[L]): enforcer.Out = enforcer(columns, r)

  /* Add logging information to validation pipe. */
  private def loggingPipe[F[_]: Sync: Logger]: Pipe[F, Record, Record] = in => {
    if (Logger[F].isDebug)
      // Interleave is used to insert validation log entry after entries from CSVParser, >> inserts it at the beginning
      in.interleaveAll(Stream.eval_(Logger[F].debug(s"Validating CSV with $this")).covaryOutput[Record])
    else in
  }
}

/** [[CSVSchema]] companion with convenience method to create empty schema. */
object CSVSchema {

  /** Creates empty schema.
    *
    * Schema created this way should be extended using [[CSVSchema.add[V](key:info\.fingo\.spata\.schema\.Key)* add]].
    *
    * @return schema with no field definition
    */
  def apply(): CSVSchema[HNil] = new CSVSchema[HNil](HNil)
}

/** CSV column representing schema field definition.
  *
  * Columns are created by the schema [[CSVSchema.add[V](key:info\.fingo\.spata\.schema\.Key)* add]] method.
  *
  * @see [[CSVSchema]]
  * @param name the name of column - CSV field name
  * @param validators collection of optional validators for field value
  * @tparam K type of column name - the singleton string
  * @tparam V column type - CSV field value type
  */
class Column[K <: Key, V: StringParser: ClassTag] private (val name: K, validators: Seq[Validator[V]]) {

  /** Gets string representation of column.
    *
    * @return short column description
    */
  override def toString: String = {
    val vInfo = if (validators.isEmpty) "" else validators.map(classLabel).mkString(" +", "+", "")
    s"'$name' -> ${classTag[V]}$vInfo"
  }

  /* Validates field. Positively validated values are returned as FieldType to encode both, key and value types
   * and to be easily accommodated as shapeless records. */
  private[schema] def validate(record: Record): Validated[FieldFlaw, FieldType[K, V]] = {
    // get parsed value from CSV record and convert parsing error to FieldFlaw
    val typeValidated =
      Validated.fromEither(record.get(name)).leftMap(e => FieldFlaw(name, TypeError(e)))
    // call each validator, convert error to FieldFlaw and chain results, short-circuiting on error
    val fullyValidated = typeValidated.andThen { v =>
      validators
        .map(validator => validator(v).leftMap(FieldFlaw(name, _)))
        .foldLeft(typeValidated)((prev, curr) => prev.andThen(_ => curr))
    }
    // convert value to FieldType
    fullyValidated.map(field[K](_))
  }
}

/* Companion with methods for column creation. */
private[schema] object Column {
  def apply[A: StringParser: ClassTag](name: String): Column[name.type, A] = new Column(name, Seq.empty)
  def apply[A: StringParser: ClassTag](name: String, validators: Seq[Validator[A]]): Column[name.type, A] =
    new Column(name, validators)
}

/** Actual verifier of CSV data. Checks if CSV record is congruent with schema.
  *
  * The verification is achieved through recursive implicits for heterogeneous list of columns (as input type)
  * and heterogeneous list of typed record data (as output type).
  *
  * @tparam L the heterogeneous list (of columns) representing schema
  */
trait SchemaEnforcer[L <: HList] extends DepFn2[L, Record] {

  /** Output type of apply method - schema validation core result type. */
  type Out <: ValidatedRecord[HList]
}

/** Implicits for [[SchemaEnforcer]]. */
object SchemaEnforcer {

  /** Alias for SchemaEnforcer trait with `Out` type fixed.
    *
    * @tparam I input type
    * @tparam O output type
    */
  type Aux[I <: HList, O <: ValidatedRecord[HList]] = SchemaEnforcer[I] { type Out = O }

  /* Initial validation result for empty column list - empty typed record. */
  private def empty(record: Record) =
    Validated.valid[InvalidRecord, TypedRecord[HNil]](TypedRecord(HNil, record.lineNum, record.rowNum))

  /** Schema enforcer for empty column list */
  implicit val enforceHNil: Aux[HNil, ValidatedRecord[HNil]] = new SchemaEnforcer[HNil] {
    override type Out = ValidatedRecord[HNil]
    override def apply(columns: HNil, record: Record): Out = empty(record)
  }

  /** Recursive schema enforcer for [[shapeless.::]].
    *
    * @param tailEnforcer schema enforcer for column list tail
    * @tparam K type of field name of both, column list and typed record data head
    * @tparam V type of field value of both, column list and typed record data head
    * @tparam TC type of column list tail
    * @tparam TR type of typed record data tail
    * @return schema enforcer for `HCons`
    */
  implicit def enforceHCons[K <: Key, V, TC <: HList, TR <: HList](
    implicit tailEnforcer: Aux[TC, ValidatedRecord[TR]]
  ): Aux[Column[K, V] :: TC, ValidatedRecord[FieldType[K, V] :: TR]] =
    new SchemaEnforcer[Column[K, V] :: TC] {
      override type Out = ValidatedRecord[FieldType[K, V] :: TR]
      override def apply(columns: Column[K, V] :: TC, record: Record): Out = {
        val validated = columns.head.validate(record)
        val tailValidated = tailEnforcer(columns.tail, record)
        validated match {
          // if current value is valid, add it to typed record from tail validation (or do nothing, if tail is invalid)
          case Valid(vv) => tailValidated.map(tv => TypedRecord(vv :: tv.data, record.lineNum, record.rowNum))
          // if current value is invalid, add it to list of flaws from tail validation
          case Invalid(iv) =>
            val flaws = tailValidated match {
              case Valid(_) => NonEmptyList.one(iv)
              case Invalid(ivt) => iv :: ivt.flaws
            }
            Validated.invalid[InvalidRecord, TypedRecord[FieldType[K, V] :: TR]](InvalidRecord(record, flaws))
        }
      }
    }
}

/** Proof of column name uniqueness in schema.
  *
  * Witnesses that given candidate key type (singleton string) is not present on column list yet.
  *
  * @tparam K type of column name
  * @tparam L type of column list
  */
trait NotPresent[K <: Key, L <: HList]

/** Implicits for [[NotPresent]]. */
object NotPresent {

  /** [[NotPresent]] witness for empty column list.
    *
    * @tparam K type of candidate column name
    * @return `NotPresent` implicit for empty schema
    */
  implicit def notPresentHNil[K <: Key]: NotPresent[K, HNil] = new NotPresent[K, HNil] {}

  /** Recursive [[NotPresent]] witness for [[shapeless.::]].
    *
    * @param neq witnesses that column name type of list head differs from the candidate name type
    * @param tailNP `NotPresent` witness for list tail
    * @tparam K type of candidate column name
    * @tparam HK type of list head column name
    * @tparam HV type of list head column value
    * @tparam T tail of column list
    * @return `NotPresent` for HCons
    */
  implicit def notPresentHCons[K <: Key, HK <: Key, HV, T <: HList](
    implicit @unused neq: K =:!= HK,
    @unused tailNP: NotPresent[K, T]
  ): NotPresent[K, Column[HK, HV] :: T] =
    new NotPresent[K, Column[HK, HV] :: T] {}
}
