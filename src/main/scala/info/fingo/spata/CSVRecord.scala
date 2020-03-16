package info.fingo.spata

import info.fingo.spata.parser.ParsingErrorCode
import info.fingo.spata.text.{DataParseException, StringParser}

/** CSV record representation.
  * A record is basically an indexed collection of strings.
  * They are indexed by header row if present or by header made from numbers, starting from `"0"`.
  *
  * `lineNum` is the last line in source file which content is part of this record
  * - in other words it is the number of consumed lines.
  * It start with `1`, including header line - first data record has typically line number `2`.
  * There may be many lines per record when some fields contain line breaks.
  * New line is interpreted independently from CSV record separator, as the standard platform `EOL` character sequence.
  *
  * `rowNum` is record counter. It start with `1` for data, with header row having number `0`.
  * It differs from `lineNum` for sources with header or fields containing line breaks.
  *
  * @param row core record data
  * @param lineNum line number in source file this record comes from
  * @param rowNum row number is source file this record comes from
  * @param header indexing header
  */
class CSVRecord private (private val row: IndexedSeq[String], val lineNum: Int, val rowNum: Int)(
  implicit header: Map[String, Int]
) {
  import StringParser._

  /** Get typed record value.
    *
    * Parsers for basic types are provided through [[text.StringParser StringParser]] object.
    *
    * To parse optional values provide `Option[T]` as type parameter.
    * Simple types will result in an exception for empty values.
    *
    * @see [[text.StringParser StringParser]] for information on providing custom parsers.
    *
    * @note This function assumes "standard" string formatting, without any locale support,
    * e.g. point as decimal separator or ISO date and time formats.
    * Use [[get[A]:* get]] if more control over source format is required
    *
    * @tparam A type to parse the field to
    * @param key the key of retrieved field
    * @return parsed value
    * @throws NoSuchElementException when incorrect `key` is provided
    * @throws text#DataParseException if field cannot be parsed to requested type
    */
  @throws[NoSuchElementException]("when incorrect key is provided")
  @throws[DataParseException]("if field cannot be parsed to requested type")
  def get[A: StringParser](key: String): A = {
    val pos = header(key)
    parse[A](row(pos))
  }

  /** Get typed record value. Supports custom string formats through [[text.StringParser.Formatter Formatter]].
    *
    * The combination of `get`, [[text.StringParser.Formatter Formatter]] constructor and `apply` method
    * allow value retrieval in following form:
    * {{{ val date: LocalDate = record.get[LocalDate]("key", DateTimeFormatter.ofPattern("dd.MM.yy")) }}}
    *
    * Parsers for basic types (as required by [[text.StringParser.Formatter Formatter]])
    * are provided through [[text.StringParser StringParser]] object.
    *
    * To parse optional values provide `Option[T]` as type parameter.
    * Simple types will result in an exception for empty values.
    *
    * @see [[text.StringParser StringParser]] for information on providing custom parsers.
    *
    * @tparam A type to parse the field to
    * @return formatter to retrieve value according to custom format
    * @throws NoSuchElementException when incorrect `key` is provided
    * @throws text#DataParseException if field cannot be parsed to requested type
    */
  @throws[NoSuchElementException]("when incorrect key is provided")
  @throws[DataParseException]("if field cannot be parsed to requested type")
  def get[A]: Formatter[A] = {
    val get = (key: String) => row(header(key))
    new Formatter[A](get)
  }

  /** Safely get typed record value.
    *
    * Parsers for basic types are provided through [[text.StringParser StringParser]] object.
    *
    * To parse optional values provide `Option[T]` as type parameter.
    * Simple types will result in an error for empty values.
    *
    * If wrong header is provided this function will return `Left[NoSuchElementException,A]`.
    * If parsing fails `Left[DataParseException,A]` will be returned.
    *
    * @see [[text.StringParser StringParser]] for information on providing custom parsers.
    *
    * @note This function assumes "standard" string formatting, without any locale support,
    * e.g. point as decimal separator or ISO date and time formats.
    * Use [[seek[A]:* seek]] if more control over source format is required
    *
    * @tparam A type to parse the field to
    * @param key the key of retrieved field
    * @return either parsed value or an exception
    */
  def seek[A: StringParser](key: String): Maybe[A] = maybe(get[A](key))

  /** Safely get typed record value.
    *
    * The combination of `get`, [[text.StringParser.SafeFormatter SafeFormatter]] constructor and `apply` method
    * allow value retrieval in following form:
    * {{{ val date: Maybe[LocalDate] = record.get[LocalDate]("key", DateTimeFormatter.ofPattern("dd.MM.yy")) }}}
    *
    * Parsers for basic types are provided through [[text.StringParser StringParser]] object.
    *
    * To parse optional values provide `Option[T]` as type parameter.
    * Simple types will result in an error for empty values.
    *
    * If wrong header is provided this function will return `Left[NoSuchElementException,A]`.
    * If parsing fails `Left[DataParseException,A]` will be returned.
    *
    * @see [[text.StringParser StringParser]] for information on providing custom parsers.
    *
    * @tparam A type to parse the field to
    * @return formatter to retrieve value according to custom format
    */
  def seek[A]: SafeFormatter[A] = {
    val get = (key: String) => row(header(key))
    new SafeFormatter[A](get)
  }

  /** Get field value
    *
    * @param key the key of retrieved field
    * @return field value in original, string form
    * @throws NoSuchElementException when incorrect `key` is provided
    */
  @throws[NoSuchElementException]("when incorrect key is provided")
  def apply(key: String): String = {
    val pos = header(key)
    row(pos)
  }

  /** Get field value
    *
    * @param idx the index of retrieved field, starting from `0`
    * @return field value in original, string form
    * @throws IndexOutOfBoundsException when incorrect `index` is provided
    */
  @throws[IndexOutOfBoundsException]("when incorrect index is provided")
  def apply(idx: Int): String = row(idx)

  /** Get number of fields in record */
  def size: Int = row.size

  /** Get text representation of record, with field separated by comma */
  override def toString: String = row.mkString(",")
}

/* CSVRecord helper object. Used to create records. */
private[spata] object CSVRecord {

  /* Create `CSVRecord`. see CSVRecord documentation for more information about parameters. */
  def apply(row: IndexedSeq[String], lineNum: Int, rowNum: Int)(
    implicit header: Map[String, Int]
  ): Either[CSVException, CSVRecord] =
    if (row.size == header.size)
      Right(new CSVRecord(row, lineNum, rowNum)(header))
    else {
      val err = ParsingErrorCode.WrongNumberOfFields
      Left(new CSVException(err.message, err.code, lineNum, rowNum))
    }
}
