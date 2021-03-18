/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import java.util.NoSuchElementException

import shapeless.{HList, LabelledGeneric}
import info.fingo.spata.Record.ToProduct
import info.fingo.spata.converter.{RecordFromHList, RecordToHList}
import info.fingo.spata.error.{ContentError, DataError, HeaderError, ParsingErrorCode, StructureException}
import info.fingo.spata.text.{FormattedStringParser, ParseResult, StringParser}

import scala.collection.immutable.VectorBuilder

/** CSV record representation.
  * A record is basically a map from string to string.
  * Values are indexed by header row if present or by tuple-style header: `"_1"`, `"_2"` etc.
  *
  * `lineNum` is the last line in source file which content is part of this record
  * - in other words it is the number of lines consumed so far to load this record.
  * It starts with `1`, including header line - first data record has typically line number `2`.
  * There may be many lines per record when some fields contain line breaks.
  * New line is interpreted independently from CSV record separator, as the standard platform `EOL` character sequence.
  *
  * `rowNum` is record counter. It start with `1` for data, with header row having number `0`.
  * It differs from `lineNum` for sources with header or fields containing line breaks.
  *
  * @param row core record data
  * @param lineNum last line number in source file this record is built from
  * @param rowNum row number in source file this record comes from
  * @param header indexing header (field names)
  */
class Record private (private val row: IndexedSeq[String], val lineNum: Int, val rowNum: Int)(val header: Header) {
  self =>

  /** Creates record from provided values.
    *
    * @param fields values of record's fields
    * @param header field's names
    */
  // FIXME: improve line and row numbers handling in user-created records + Scaladoc
  def this(fields: String*)(header: Header) = this(fields.toIndexedSeq, 0, 0)(header)

  /** Safely gets typed record value.
    *
    * Parsers for basic types are provided through [[text.StringParser$ StringParser]] object.
    *
    * To parse optional values provide `Option[_]` as type parameter.
    * Parsing empty value to simple type will result in an error.
    *
    * If wrong header key is provided this function will return `Left[error.HeaderError,_]`.
    * If parsing fails `Left[error.DataError,_]` will be returned.
    *
    * @see [[text.StringParser StringParser]] for information on providing custom parsers.
    * @note This function assumes "standard" string formatting, without any locale support,
    * e.g. point as decimal separator or ISO date and time formats.
    * Use [[get[A]:* get]] if more control over source format is required.
    * @tparam A type to parse the field to
    * @param key the key of retrieved field
    * @return either parsed value or an error
    */
  def get[A: StringParser](key: String): Decoded[A] = retrieve(key)

  /** Safely gets typed record value.
    *
    * The combination of `get`, [[Field]] constructor and `apply` method allows value retrieval in following form:
    * {{{ val date: Decoded[LocalDate] = record.get[LocalDate]("key", DateTimeFormatter.ofPattern("dd.MM.yy")) }}}
    * (type of formatter is inferred based on target type).
    *
    * Parsers for basic types are provided through [[text.StringParser$ StringParser]] object.
    * Additional ones may be provided as implicits.
    *
    * To parse optional values provide `Option[_]` as type parameter.
    * Parsing empty value to simple type will result in an error.
    *
    * @see [[text.StringParser StringParser]] for information on providing custom parsers.
    * @tparam A type to parse the field to
    * @return intermediary to retrieve value according to custom format
    */
  def get[A]: Field[A] = new Field[A]

  /** Converts this record to [[scala.Product]], e.g. case class.
    *
    * The combination of `to`, [[Record.ToProduct]] constructor and `apply` method
    * allows conversion in following form:
    * {{{
    * // Assume following CSV source
    * // ----------------
    * // name,born
    * // Nicolaus Copernicus,1473-02-19,1543-05-24
    * // Johannes Hevelius,1611-01-28,
    * // ----------------
    * // and a Record created based on it
    * val record: Record = ???
    * case class Person(name: String, born: LocalDate, died: Option[LocalDate])
    * val person: Decoded[Person] = record.to[Person]() // this line may cause IntelliJ to mistakenly show an error
    * }}}
    * Please note, that the conversion is name-based (case class field names have to match CSV header)
    * and is case sensitive.
    * The order of fields doesn't matter.
    * Case class may be narrower and effectively retrieve only a subset of record's fields.
    *
    * It is possible to use a tuple instead of case class.
    * In such case the header must match the tuple field naming convention: `_1`, `_2` etc.
    *
    * Current implementation supports only shallow conversion -
    * each product field has to be retrieved from single CSV field through [[text.StringParser StringParser]].
    *
    * Because conversion to product requires parsing of all fields through [[text.StringParser StringParser]],
    * there is no way to provide custom formatter, like while using [[get[A]:* get]] method.
    * If other then the default formatting has to be handled, a custom implicit `stringParser` has to be provided:
    * {{{
    * // Assume following CSV source
    * // ----------------
    * // name,born,died
    * // Nicolaus Copernicus,19.02.1473,24.05.1543
    * // Johannes Hevelius,28.01.1611,
    * // ----------------
    * // and a Record created based on it
    * val record: Record = ???
    * case class Person(name: String, born: LocalDate, died: Option[LocalDate])
    * implicit val ldsp: StringParser[LocalDate] = (str: String) =>
    *     LocalDate.parse(str.strip, DateTimeFormatter.ofPattern("dd.MM.yyyy"))
    * val person: Decoded[Person] = record.to[Person]() // this line may cause IntelliJ to mistakenly show an error
    * }}}
    *
    * @tparam P the Product type to converter this record to
    * @return intermediary to infer representation type and return proper type
    */
  def to[P <: Product]: ToProduct[P] = new ToProduct[P](this)

  /** Gets field value.
    *
    * @param key the key of retrieved field
    * @return field value in its original, string format if correct key is provided or None otherwise.
    */
  def apply(key: String): Option[String] =
    header(key).flatMap(apply)

  /** Gets field value.
    *
    * @param idx the index of retrieved field, starting from `0`.
    * @return field value in original, string format or None if index is out of bounds.
    */
  def apply(idx: Int): Option[String] = row.unapply(idx)

  /** Gets number of fields in record. */
  def size: Int = row.size

  /** Gets text representation of record, with fields separated by comma. */
  override def toString: String = row.mkString(",")

  /* Retrieve field value using string parser with provided format */
  private def retrieve[A, B](key: String, fmt: B)(implicit parser: FormattedStringParser[A, B]): Decoded[A] =
    decode(key)(v => StringParser.parse(v, fmt))

  /* Retrieve field value using string parser */
  private def retrieve[A](key: String)(implicit parser: StringParser[A]): Decoded[A] =
    decode(key)(v => StringParser.parse(v))

  /* Decode field value using provided string parsing function. Wraps error into proper CSVException subclass. */
  private def decode[A](key: String)(parse: String => ParseResult[A]): Decoded[A] = {
    val value = apply(key)
    value match {
      case Some(str) =>
        parse(str) match {
          case Right(value) => Right(value)
          case Left(error) => Left(new DataError(error.content, lineNum, rowNum, key, error))
        }
      case None => Left(new HeaderError(lineNum, rowNum, key, new NoSuchElementException()))
    }
  }

  /** Intermediary to delegate parsing to in order to infer type of formatter used by parser.
    * Provides exception-free parsing method.
    *
    * @tparam A target type for parsing
    */
  class Field[A] {

    /** Safely parses string to desired type based on provided format.
      *
      * If wrong header key is provided this function will return `Left[error.HeaderError,_]`.
      * If parsing fails `Left[error.DataError,_]` will be returned.
      *
      * @param key the key of retrieved field
      * @param fmt formatter specific for particular result type, e.g. `DateTimeFormatter` for dates and times
      * @param parser the parser for specific target type
      * @tparam B type of formatter
      * @return either parsed value or an error
      */
    def apply[B](key: String, fmt: B)(implicit parser: FormattedStringParser[A, B]): Decoded[A] = retrieve(key, fmt)
  }

  /** Access to unsafe (exception throwing) methods */
  object unsafe {

    /** Gets typed record value.
      *
      * Parsers for basic types are provided through [[text.StringParser$ StringParser]] object.
      *
      * To parse optional values provide `Option[_]` as type parameter.
      * Parsing empty value to simple type will throw an exception.
      *
      * @see [[text.StringParser StringParser]] for information on providing custom parsers.
      * @note This function assumes "standard" string formatting, without any locale support,
      * e.g. point as decimal separator or ISO date and time formats.
      * Use [[get[A]:* get]] if more control over source format is required.
      * @tparam A type to parse the field to
      * @param key the key of retrieved field
      * @return parsed value
      * @throws error.ContentError if field cannot be parsed to requested type or incorrect `key` is provided
      */
    @throws[ContentError]("if field cannot be parsed to requested type or wrong key is provided")
    def get[A: StringParser](key: String): A = rethrow(retrieve(key))

    /** Gets typed record value. Supports custom string formats through [[text.StringParser.Pattern Pattern]].
      *
      * The combination of `get`, [[Field]] constructor and `apply` method
      * allows value retrieval in following form:
      * {{{ val date: LocalDate = record.unsafe.get[LocalDate]("key", DateTimeFormatter.ofPattern("dd.MM.yy")) }}}
      * (type of formatter is inferred based on target type).
      *
      * Parsers for basic types (as required by [[Field]])
      * are provided through [[text.StringParser$ StringParser]] object.
      * Additional ones may be provided as implicits.
      *
      * To parse optional values provide `Option[_]` as type parameter.
      * Parsing empty value to simple type will throw an exception.
      *
      * @see [[text.StringParser StringParser]] for information on providing custom parsers.
      * @tparam A type to parse the field to
      * @return intermediary to retrieve value according to custom format
      */
    def get[A]: Field[A] = new Field[A]

    /** Gets field value.
      *
      * @param key the key of retrieved field
      * @return field value in original, string format
      * @throws NoSuchElementException when incorrect `key` is provided
      */
    @throws[NoSuchElementException]("when incorrect key is provided")
    def apply(key: String): String = self(key).get

    /** Gets field value.
      *
      * @param idx the index of retrieved field, starting from `0`
      * @return field value in original, string format
      * @throws IndexOutOfBoundsException when incorrect index is provided
      */
    @throws[IndexOutOfBoundsException]("when incorrect index is provided")
    def apply(idx: Int): String = row(idx)

    /* Throws exception if Either is Left. */
    private def rethrow[A](result: Decoded[A]) = result match {
      case Right(value) => value
      case Left(error) => throw error
    }

    /** Intermediary to delegate parsing to in order to infer type of formatter used by parser.
      *
      * @tparam A target type for parsing
      */
    class Field[A] {

      /** Parses field to desired type based on provided format.
        *
        * @param key the key of retrieved field
        * @param fmt formatter specific for particular result type, e.g. `DateTimeFormatter` for dates and times
        * @param parser the parser for specific target type
        * @tparam B type of concrete formatter
        * @return parsed value
        * @throws error.ContentError if field cannot be parsed to requested type or incorrect `key` is provided
        */
      @throws[ContentError]("if field cannot be parsed to requested type or incorrect key is provided")
      def apply[B](key: String, fmt: B)(implicit parser: FormattedStringParser[A, B]): A = rethrow(retrieve(key, fmt))
    }
  }
}

/** Record helper object. Used to create and converter records. */
object Record {

  /* Creates `Record`. See Record documentation for more information about parameters. */
  private[spata] def apply(row: IndexedSeq[String], lineNum: Int, rowNum: Int)(
    header: Header
  ): Either[StructureException, Record] =
    if (row.size == header.size)
      Right(new Record(row, lineNum, rowNum)(header))
    else
      Left(new StructureException(ParsingErrorCode.WrongNumberOfFields, lineNum, rowNum))

  def from[P <: Product]: FromProduct[P] = new FromProduct[P]()

  /** Intermediary to delegate conversion to in order to infer [[shapeless.HList]] representation type.
    *
    * When converting a record to [[scala.Product]] (e.g. case class) one may use:
    * {{{ val tp = new Record.ToProduct[C](record)() }}}
    *
    * @see [[Record.to]] for real world usage scenario.
    * @param record the record to convert
    * @tparam P the target type for conversion
    */
  class ToProduct[P <: Product](record: Record) {

    /** Converts record to [[scala.Product]], e.g. case class.
      *
      * @param gen the generic for specified target type and [[shapeless.HList]] representation
      * @param rToHL intermediary converter from record to [[shapeless.HList]] of [[shapeless.labelled.FieldType]]s
      * @tparam R [[shapeless.HList]] representation type
      * @return either converted product or an error
      */
    final def apply[R <: HList]()(implicit gen: LabelledGeneric.Aux[P, R], rToHL: RecordToHList[R]): Decoded[P] =
      rToHL(record).map(gen.from)
  }

  class FromProduct[P <: Product] {

    final def apply[R <: HList](
      product: P
    )(implicit gen: LabelledGeneric.Aux[P, R], rFromHL: RecordFromHList[R]): Record =
      rFromHL(gen.to(product)).result()
  }
}

class RecordBuilder {
  val vb = new VectorBuilder[(String, String)]()

  def add[A](key: String, value: A): this.type = {
    // TODO: add formatting
    vb.addOne((key, value.toString))
    this
  }

  def result(): Record = {
    val v = vb.result().reverse
    val header = new Header(v.map(_._1): _*)
    new Record(v.map(_._2): _*)(header)
  }
}
