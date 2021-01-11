/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import scala.annotation.unused
import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}
import cats.effect.Sync
import fs2.Pipe
import shapeless.{::, =:!=, DepFn2, HList, HNil}
import shapeless.labelled.{field, FieldType}
import info.fingo.spata.Record
import info.fingo.spata.schema.error.TypeError
import info.fingo.spata.schema.validator.Validator
import info.fingo.spata.text.StringParser
import info.fingo.spata.util.Logger

/** CSV schema definition and validation.
  *
  * Schema defines fields (their names) which are expected in CSV stream along with their types.
  * Fields with optional values have to be defined as `Option`s:
  * {{{
  * val schema = CSVSchema()
  *   .add[String]("name")
  *   .add[Option[LocalDate]]("birthday")
  * }}}
  * Not all fields have to be declared in schema but only a subset of them.
  *
  * In case of header mapping through [[CSVConfig.mapHeader]],
  * the names provided in schema are the final ones, after mapping.
  *
  * Additionally, it is possible to declare per-field validators, posing additional requirements on CSV data:
  * {{{
  * val schema = CSVSchema()
  *   .add[String]("code", RegexValidator("[A-Z][A-Z0-9]+"))
  *   .add[BigDecimal]("price", MinValidator(0.01))
  * }}}
  * For more information on using existing or creating own validators see [[validator.Validator Validator]].
  *
  * CSV schema is verified through it [[validate]] method. It may yield an [[InvalidRecord]],
  * containing validation error together with original [[Record]] data or a [[TypedRecord]],
  * containing selected, strongly typed data - in both cases wrapped in [[cats.data.Validated]].
  *
  * @param columns the list of typed columns with optional validators
  * @tparam L heterogeneous list encoding schema
  */
class CSVSchema[L <: HList] private (columns: L) {

  /** Adds field definition to schema.
    *
    * Field definition consists of field name and its type. Set of fields definitions constitutes schema definition.
    * A collection of additional [[validator.Validator]]s may be added to a field.
    * When validating schema, they are checked after field type verification and are expressed in context of field type.
    *
    * To get value of proper type from a field, an implicit [[text.StringParser]] is required.
    * Parsers for basic types and formats are provided through [[text.StringParser$ StringParser]] object.
    * Additional ones may be provided by implementing `StringParser` trait.
    *
    * Optional values should be denoted by providing `Option[A]` as field type value.
    * Note that optional values require specific validators operating on `Option` instead of plain type.
    * Also note, that optional means in this case that individual values may be missing (empty) but
    * the declared field has to be present in the source data.
    *
    * This is a chaining method which allows extending schema created by previous call to `add`
    * or starting with empty one:
    * {{{
    * val schema = CSVSchema()
    *   .add[Double]("latitude", MinMaxValidator(-90, 90))
    *   .add[Double]("longitude", MinMaxValidator(-180, 180))
    * }}}
    *
    * @param key unique field name - a singleton string
    * @param validators optional validators to check if field values are conform to additional rules, apart from type
    * @param ev evidence that provided key is unique - is not presented in schema yet
    * @tparam V field value type
    * @return new schema definition with column (field definition) added to it
    */
  def add[V: StringParser](key: Key, validators: Validator[V]*)(
    implicit @unused ev: NotPresent[key.type, L]
  ): CSVSchema[Column[key.type, V] :: L] =
    new CSVSchema[Column[key.type, V] :: L](Column.apply[V](key, validators) :: columns)

  /** Adds field definition to schema. Does not allow attaching additional validators.
    *
    * @see [[add[V](key:info\.fingo\.spata\.schema\.Key,* add[V](key: Key, validators: Validator[V]*)]]
    * @param key unique field name - a singleton string
    * @param ev evidence that provided key is unique - is not presented in schema yet
    * @tparam V field value type
    * @return new schema definition with column (field definition) added to it
    */
  def add[V: StringParser](key: Key)(
    implicit @unused ev: NotPresent[key.type, L]
  ): CSVSchema[Column[key.type, V] :: L] =
    new CSVSchema[Column[key.type, V] :: L](Column.apply[V](key) :: columns)

  /** Validate CSV stream against schema.
    *
    * For each input record the validation process:
    *  - parses the field to the type required by schema and, if successful
    *  - runs provided validators against parsed value.
    * The process is successful and creates a [[TypedRecord]] if values of all fields defined in schema
    * are correctly parsed and positively validated. If any of these operations fails, an [[InvalidRecord]] is yielded.
    *
    * If there are many validators defined for single field, the validation stops at first invalid result.
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
    // TODO: this creates the log entry before the log entry from parser and have to be fixed
    in => Logger[F].infoS(s"Validating CSV with $columns") >> in.map(r => validateRecord(r)(enforcer))

  /* Validate single record against schema. */
  private def validateRecord(r: Record)(implicit enforcer: SchemaEnforcer[L]): enforcer.Out = enforcer(columns, r)
}

/** [[CSVSchema]] companion with convenience method to create empty schema. */
object CSVSchema {

  /** Creates empty schema.
    *
    * Schema created this way is intended to be extended using
    * [[CSVSchema.add[V](key:info\.fingo\.spata\.schema\.Key)* add]]
    *
    * @return schema with no field definitions
    */
  def apply(): CSVSchema[HNil] = new CSVSchema[HNil](HNil)
}

/** CSV column representing schema field definition.
  *
  * Columns are created by schema [[CSVSchema.add[V](key:info\.fingo\.spata\.schema\.Key)* add]] method.
  *
  * @see [[CSVSchema]]
  * @param name the name of column - CSV field name
  * @param validators collection of optional validators for field value
  * @tparam K type of column name - singleton string
  * @tparam V field value type
  */
class Column[K <: Key, V: StringParser] private (val name: K, validators: Seq[Validator[V]]) {

  /* Validates field. Positively validated values are returned as FieldType to encode both, key and value types
   * and to be easily accommodated as shapeless records. */
  private[schema] def validate(record: Record): Validated[FieldFlaw, FieldType[K, V]] = {
    // get parsed value from CSV record and convert parsing error to FieldFlaw
    val typeValidated =
      Validated.fromEither(record.get(name)).leftMap(e => FieldFlaw(name, TypeError(e)))
    // call each validator, convert error to FieldFlaw and chain results
    val fullyValidated = typeValidated.andThen { v =>
      validators.map { validator =>
        Validated.fromOption(validator(v), v).map(FieldFlaw(name, _)).swap
      }.foldLeft(typeValidated)((prev, curr) => prev.andThen(_ => curr))
    }
    // convert value to FieldType
    fullyValidated.map(field[K](_))
  }
}

/* Companion with methods for column creation */
private[schema] object Column {
  def apply[A: StringParser](name: String): Column[name.type, A] = new Column(name, Nil)
  def apply[A: StringParser](name: String, validators: Seq[Validator[A]]): Column[name.type, A] =
    new Column(name, validators)
}

/** Actual verifier of CSV data. Checks if CSV record is congruent with schema.
  *
  * The verification is achieved through recursive implicits for heterogeneous list of columns.
  *
  * @tparam L the heterogeneous list (of columns) representing schema
  */
trait SchemaEnforcer[L <: HList] extends DepFn2[L, Record] {

  /** Output type of apply method. */
  type Out <: ValidatedRecord[HList]
}

/** Implicits for [[SchemaEnforcer]]. */
object SchemaEnforcer {

  /** Alias for SchemaEnforcer trait with Out type fixed.
    *
    * @tparam I input type
    * @tparam O output type
    */
  type Aux[I <: HList, O <: ValidatedRecord[HList]] = SchemaEnforcer[I] { type Out = O }

  /* Initial validation result for empty column list. */
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
    * @tparam K type of field name of column list and typed record data head
    * @tparam V type of field value of column list and typed record data head
    * @tparam T type of column list tail
    * @tparam TTR type of typed record data tail
    * @return schema enforcer for `HCons`
    */
  implicit def enforceHCons[K <: Key, V, T <: HList, TTR <: HList](
    implicit tailEnforcer: Aux[T, ValidatedRecord[TTR]]
  ): Aux[Column[K, V] :: T, ValidatedRecord[FieldType[K, V] :: TTR]] =
    new SchemaEnforcer[Column[K, V] :: T] {
      override type Out = ValidatedRecord[FieldType[K, V] :: TTR]
      override def apply(columns: Column[K, V] :: T, record: Record): Out = {
        val column = columns.head
        val validated = column.validate(record)
        val tailValidated = tailEnforcer(columns.tail, record)
        validated match {
          // if current value is valid, add it to typed record from tail validation (or do nothing, if tail is invalid)
          case Valid(vv) => tailValidated.map(tv => TypedRecord(vv :: tv.data, record.lineNum, record.rowNum))
          // if current value is invalid, add it to list of flaws fro tail validatiom
          case Invalid(iv) =>
            val flaws = tailValidated match {
              case Valid(_) => Nil
              case Invalid(ivt) => ivt.flaws
            }
            Validated.invalid[InvalidRecord, TypedRecord[FieldType[K, V] :: TTR]](InvalidRecord(record, iv :: flaws))
        }
      }
    }
}

/** Proof of column name uniqueness in schema.
  *
  * Witnesses that given key type (singleton string) is not present yet on column list.
  *
  * @tparam K type of column name
  * @tparam L type of column list
  */
trait NotPresent[K <: Key, L <: HList]

/** Implicits for [[NotPresent]]. */
object NotPresent {

  /** [[NotPresent]] witness for empty column list.
    *
    * @tparam K type of column name
    * @return `NotPresent` implicit for empty sxchema
    */
  implicit def notPresentHNil[K <: Key]: NotPresent[K, HNil] = new NotPresent[K, HNil] {}

  /** Recursive [[NotPresent]] witness for [[shapeless.::]].
    *
    * @param neq witness that column name type of list head differs from type being examined
    * @param tailNP `NotPresent` witness for list tail
    * @tparam K type of column name being examined
    * @tparam V type of value of column from list head
    * @tparam H type of name of column from list head
    * @tparam T tail of column list
    * @return `NotPresent` for HCons
    */
  implicit def notPresentHCons[K <: Key, V, H <: Key, T <: HList](
    implicit @unused neq: K =:!= H,
    @unused tailNP: NotPresent[K, T]
  ): NotPresent[K, Column[H, V] :: T] =
    new NotPresent[K, Column[H, V] :: T] {}
}
