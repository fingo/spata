package info.fingo.spata

import java.time.format.DateTimeParseException
import java.util.NoSuchElementException

import info.fingo.spata.CSVRecord.ToProduct
import info.fingo.spata.converter.RecordToHList
import info.fingo.spata.parser.ParsingErrorCode
import info.fingo.spata.text.{DataParseException, FormattedStringParser, StringParser}
import shapeless.{HList, LabelledGeneric}

import scala.util.control.NonFatal

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
  * @param header indexing header (keys)
  */
class CSVRecord private (private val row: IndexedSeq[String], val lineNum: Int, val rowNum: Int)(
  implicit header: Map[String, Int]
) {

  /** Gets typed record value.
    *
    * Parsers for basic types are provided through [[text.StringParser StringParser]] object.
    *
    * To parse optional values provide `Option[_]` as type parameter.
    * Parsing empty value to simple type will throw an exception.
    *
    * @see [[text.StringParser StringParser]] for information on providing custom parsers.
    *
    * @note This function assumes "standard" string formatting, without any locale support,
    * e.g. point as decimal separator or ISO date and time formats.
    * Use [[get[A]:* get]] if more control over source format is required.
    *
    * @tparam A type to parse the field to
    * @param key the key of retrieved field
    * @return parsed value
    * @throws NoSuchElementException when incorrect `key` is provided
    * @throws CSVDataException if field cannot be parsed to requested type
    */
  @throws[NoSuchElementException]("when incorrect key is provided")
  @throws[CSVDataException]("if field cannot be parsed to requested type")
  def get[A: StringParser](key: String): A = {
    val value = row(header(key))
    val parser = implicitly[StringParser[A]]
    wrapParseExc(key) { parser.parse(value) }
  }

  /** Gets typed record value. Supports custom string formats through [[text.StringParser.Pattern Pattern]].
    *
    * The combination of `get`, [[Field]] constructor and `apply` method
    * allows value retrieval in following form:
    * {{{ val date: LocalDate = record.get[LocalDate]("key", DateTimeFormatter.ofPattern("dd.MM.yy")) }}}
    *
    * Parsers for basic types (as required by [[Field]]) are provided through [[text.StringParser StringParser]] object.
    * Additional ones may be provided as implicits.
    *
    * To parse optional values provide `Option[_]` as type parameter.
    * Parsing empty value to simple type will throw an exception.
    *
    * @see [[text.StringParser StringParser]] for information on providing custom parsers.
    *
    * @tparam A type to parse the field to
    * @return intermediary to retrieve value according to custom format
    */
  def get[A]: Field[A] = new Field[A]

  /** Safely gets typed record value.
    *
    * Parsers for basic types are provided through [[text.StringParser StringParser]] object.
    *
    * To parse optional values provide `Option[_]` as type parameter.
    * Parsing empty value to simple type will result in an error.
    *
    * If wrong header is provided this function will return `Left[NoSuchElementException,_]`.
    * If parsing fails `Left[CSVDataException,_]` will be returned.
    *
    * @see [[text.StringParser StringParser]] for information on providing custom parsers.
    *
    * @note This function assumes "standard" string formatting, without any locale support,
    * e.g. point as decimal separator or ISO date and time formats.
    * Use [[seek[A]:* seek]] if more control over source format is required.
    *
    * @tparam A type to parse the field to
    * @param key the key of retrieved field
    * @return either parsed value or an exception
    */
  def seek[A: StringParser](key: String): Maybe[A] = maybe(get[A](key))

  /** Safely gets typed record value.
    *
    * The combination of `get`, [[SafeField]] constructor and `apply` method allows value retrieval in following form:
    * {{{ val date: Maybe[LocalDate] = record.get[LocalDate]("key", DateTimeFormatter.ofPattern("dd.MM.yy")) }}}
    *
    * Parsers for basic types are provided through [[text.StringParser StringParser]] object.
    * Additional ones may be provided as implicits.
    *
    * To parse optional values provide `Option[_]` as type parameter.
    * Parsing empty value to simple type will result in an error.
    *
    * @see [[text.StringParser StringParser]] for information on providing custom parsers.
    *
    * @tparam A type to parse the field to
    * @return intermediary to retrieve value according to custom format
    */
  def seek[A]: SafeField[A] = new SafeField[A]

  /** Gets field value.
    *
    * @param key the key of retrieved field
    * @return field value in original, string format
    * @throws NoSuchElementException when incorrect `key` is provided
    */
  @throws[NoSuchElementException]("when incorrect key is provided")
  def apply(key: String): String = {
    val pos = header(key)
    row(pos)
  }

  /** Converts this record to [[scala.Product]], e.g. case class.
    *
    * The combination of `to`, [[CSVRecord.ToProduct]] constructor and `apply` method
    * allows conversion in following form:
    * {{{
    * // assumming following CSV source
    * // ----------------
    * // name,born
    * // Nicolaus Copernicus,1473-02-19,1543-05-24
    * // Johannes Hevelius,1611-01-28,
    * // ----------------
    * // and a CSVRecord created based on this
    * val record: CSVRecord = ???
    * case class Person(name: String, born: LocalDate, died: Option[LocalDate])
    * val person: Maybe[Person] = record.to[Person]() // this line may cause IntelliJ to mistakenly show an error
    * }}}
    * Please note, that the conversion is name-base (case class field names have to match CSV header)
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
    * there is no way to provide custom formatter, like while using [[get[A]:* get]] or [[seek[A]:* seek]] methods.
    * If other then the default formatting has to be handled, a custom implicit `stringParser` has to be provided:
    * {{{
    * // assumming following CSV source
    * // ----------------
    * // name,born,died
    * // Nicolaus Copernicus,19.02.1473,24.05.1543
    * // Johannes Hevelius,28.01.1611,
    * // ----------------
    * // and a CSVRecord created based on this
    * val record: CSVRecord = ???
    * case class Person(name: String, born: LocalDate, died: Option[LocalDate])
    * implicit val ldsp: StringParser[LocalDate] = (str: String) =>
    *     LocalDate.parse(str.strip, DateTimeFormatter.ofPattern("dd.MM.yyyy"))
    * val person: Maybe[Person] = record.to[Person]() // this line may cause IntelliJ to mistakenly show an error
    * }}}
    *
    * @tparam P the Product type to converter this record to
    * @return intermediary to infer representation type and return proper type
    */
  def to[P <: Product]: ToProduct[P] = new ToProduct[P](this)

  /** Gets field value
    *
    * @param idx the index of retrieved field, starting from `0`
    * @return field value in original, string format
    * @throws IndexOutOfBoundsException when incorrect `index` is provided
    */
  @throws[IndexOutOfBoundsException]("when incorrect index is provided")
  def apply(idx: Int): String = row(idx)

  /** Gets number of fields in record. */
  def size: Int = row.size

  /** Gets text representation of record, with fields separated by comma. */
  override def toString: String = row.mkString(",")

  /* Converts any parsing exception to CSVDataException to provide error position */
  private def wrapParseExc[A](field: String)(code: => A): A =
    try code
    catch {
      case NonFatal(ex) => throw new CSVDataException(exceptionMsg(ex, field), "wrongType", lineNum, rowNum, field, ex)
    }

  private def exceptionMsg(ex: Throwable, field: String) = {
    val typeInfo = ex match {
      case e: DataParseException if e.dataType.isDefined => e.dataType.get
      case _: NumberFormatException => "number"
      case _: DateTimeParseException => "date/time"
      case NonFatal(_) => "requested type"
    }
    s"Cannot parse field $field at record $rowNum to $typeInfo"
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
      * @throws NoSuchElementException when incorrect `key` is provided
      * @throws CSVDataException if field cannot be parsed to requested type
      */
    @throws[NoSuchElementException]("when incorrect key is provided")
    @throws[CSVDataException]("if field cannot be parsed to requested type")
    def apply[B](key: String, fmt: B)(implicit parser: FormattedStringParser[A, B]): A = {
      val value = row(header(key))
      wrapParseExc(key) { parser.parse(value, fmt) }
    }
  }

  /** Intermediary to delegate parsing to in order to infer type of formatter used by parser.
    * Provide exception-free parsing method.
    *
    * @tparam A target type for parsing
    */
  class SafeField[A] {

    /** Safely parses string to desired type based on provided format.
      *
      * If wrong header is provided this function will return `Left[NoSuchElementException,_]`.
      * If parsing fails `Left[CSVDataException,_]` will be returned.
      *
      * @param key the key of retrieved field
      * @param fmt formatter specific for particular result type, e.g. `DateTimeFormatter` for dates and times
      * @param parser the parser for specific target type
      * @tparam B type of formatter
      * @return either parsed value or an exception
      */
    def apply[B](key: String, fmt: B)(implicit parser: FormattedStringParser[A, B]): Maybe[A] = maybe {
      val value = row(header(key))
      wrapParseExc(key) { parser.parse(value, fmt) }
    }
  }
}

/** CSVRecord helper object. Used to create and converter records. */
object CSVRecord {

  /* Creates `CSVRecord`. See CSVRecord documentation for more information about parameters. */
  private[spata] def apply(row: IndexedSeq[String], lineNum: Int, rowNum: Int)(
    implicit header: Map[String, Int]
  ): Either[CSVStructureException, CSVRecord] =
    if (row.size == header.size)
      Right(new CSVRecord(row, lineNum, rowNum)(header))
    else {
      val err = ParsingErrorCode.WrongNumberOfFields
      Left(new CSVStructureException(err.message, err.code, lineNum, rowNum))
    }

  /** Intermediary to delegate conversion to in order to infer [[shapeless.HList]] representation type.
    *
    * When converting a record to [[scala.Product]] (e.g. case class) one may use:
    * {{{ val tp = new CSVRecord.ToProduct[C](record)() }}}
    *
    * @see [[CSVRecord.to]] for real world usage scenario.
    * @param record the record to converter
    * @tparam P the target type for conversion
    */
  class ToProduct[P <: Product](record: CSVRecord) {

    /** Converts record to [[scala.Product]], e.g. case class.
      *
      * @param gen the generic for specified target type and [[shapeless.HList]] representation
      * @param rToHL intermediary converter from record to [[shapeless.HList]] of [[shapeless.labelled.FieldType]]s
      * @tparam R [[shapeless.HList]] representation type
      * @return either converted product or an exception
      */
    final def apply[R <: HList]()(implicit gen: LabelledGeneric.Aux[P, R], rToHL: RecordToHList[R]): Maybe[P] =
      rToHL(record).map(gen.from)
  }
}
