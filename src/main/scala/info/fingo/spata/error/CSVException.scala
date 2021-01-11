/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.error

import ParsingErrorCode.ErrorCode
import info.fingo.spata.text.StringParser

/** Base exception reported by CSV handling methods.
  * May be thrown, raised through [[fs2.Stream#raiseError]] or returned as `Left`, depending on context.
  *
  * `Exception` subclasses denote errors which make CSV parsing completely impossible,
  * while `Error`s are typically local and mean that at least partial results should be available.
  *
  * For possible error codes see concrete classes implementations.
  *
  * `line` is the line in source file at which error has been detected.
  * It starts with `1`, including header line - first data record has typically line number `2`.
  * There may be many lines per record when some fields contain line breaks.
  * New line is interpreted independently from CSV record separator, as the standard platform `EOL` character sequence.
  *
  * `row` is record number at which error has been detected.
  * It starts with `1` for data, with header row having number `0`.
  * It differs from `line` for sources with header or fields containing line breaks.
  *
  * `col` is the position (character) at given line at which the error has been detected.
  * `0` means "before first character".
  *
  * `field` is the name (key) of field at which error has been detected.
  * It may be a name from header or a number (starting with `0`) if no header is present.
  *
  * @param message error message
  * @param messageCode error code
  * @param line source line at which error occurred, starting from 1
  * @param row row (record) at which error occurred, starting from 1 (`0` for header)
  * @param col column (character) at which error occurred
  * @param field field name at which error occurred
  * @param cause the root exception, if available
  */
sealed abstract class CSVException private[spata] (
  message: String,
  val messageCode: String,
  val line: Int,
  val row: Int,
  val col: Option[Int],
  val field: Option[String],
  cause: Option[Throwable]
) extends Exception(message, cause.orNull)

/** Exception reported for CSV format errors.
  *
  * Possible `messageCode`s are:
  *   - `unclosedQuotation` for not enclosed quotation,
  *   - `unescapedQuotation` for not escaped quotation,
  *   - `unmatchedQuotation` for unmatched quotation (probably premature end of file),
  *   - `fieldTooLong` for values longer than provided maximum (may be caused by unmatched quotation),
  *   - `missingHeader` when header isn't found (may be empty content),
  *   - `duplicatedHeader` when two or more header names are duplicated (not unique),
  *   - `wrongNumberOfFields` when number of values doesn't match header or previous records size.
  *
  * @see [[CSVException]] for description of fields providing error location.
  *
  * @param errorCode parsing error code
  * @param line source line at which error occurred, starting from 1
  * @param row row (record) at which error occurred, starting from 1 (`0` for header)
  * @param col column (character) at which error occurred
  * @param field field name at which error occurred
  */
class StructureException private[spata] (
  errorCode: ErrorCode,
  line: Int,
  row: Int,
  col: Option[Int] = None,
  field: Option[String] = None
) extends CSVException(
    StructureException.message(errorCode, line, row, col, field),
    errorCode.code,
    line,
    row,
    col,
    field,
    None
  )

private object StructureException {
  def message(errorCode: ErrorCode, line: Int, row: Int, col: Option[Int], field: Option[String]): String = {
    val colInfo = col.map(c => s" and column $c").getOrElse("")
    val fieldInfo = field.map(f => s" (field $f)").getOrElse("")
    s"Error occurred at row $row (line $line)$colInfo$fieldInfo while parsing CSV source. ${errorCode.message}"
  }
}

/** Error for content-related issues, typically reported on record level.
  *
  * @see [[CSVException]] for description of fields providing error location.
  *
  * @param message error message
  * @param messageCode error code
  * @param line source line at which error occurred, starting from 1
  * @param row row (record) at which error occurred, starting from 1 (`0` for header)
  * @param field field name (header key) at which error occurred
  * @param cause the root exception
  */
sealed abstract class ContentError private[spata] (
  message: String,
  messageCode: String,
  line: Int,
  row: Int,
  field: String,
  cause: Throwable
) extends CSVException(
    message,
    messageCode,
    line,
    row,
    None,
    Some(field),
    Some(cause)
  )

/** Error reported while accessing field with incorrect header value (key).
  *
  * Possible `messageCode`s are:
  *   - `wrongKey` for error caused by accessing field with incorrect key.
  *
  * @see [[CSVException]] for description of fields providing error location.
  *
  * @param line source line at which error occurred, starting from 1
  * @param row row (record) at which error occurred, starting from 1 (`0` for header)
  * @param field field name (header key) at which error occurred
  * @param cause the root exception
  */
class HeaderError private[spata] (
  line: Int,
  row: Int,
  field: String,
  cause: Throwable
) extends ContentError(
    HeaderError.message(line, row, field),
    HeaderError.messageCode,
    line,
    row,
    field,
    cause
  )

private object HeaderError {
  val messageCode = "wrongKey"

  def message(line: Int, row: Int, field: String): String =
    s"Error occurred at row $row (line $line) while trying to access CSV field by '$field'."
}

/** Error reported for CSV data problems, caused by string parsing.
  *
  * Possible `messageCode`s are:
  *   - `wrongType` for error caused by parsing field to specific type.
  *
  * @see [[CSVException]] for description of fields providing error location.
  *
  * @param line source line at which error occurred, starting from 1
  * @param row row (record) at which error occurred, starting from 1 (`0` for header)
  * @param field field name at which error occurred
  * @param cause the root exception
  */
class DataError private[spata] (
  value: String,
  line: Int,
  row: Int,
  field: String,
  cause: Throwable
) extends ContentError(
    DataError.message(value, line, row, field, cause),
    DataError.messageCode,
    line,
    row,
    field,
    cause
  )

private object DataError {
  val messageCode = "wrongType"
  val maxValueLength = 20
  val valueCutSuffix = "..."

  def message(value: String, line: Int, row: Int, field: String, cause: Throwable): String = {
    val v =
      if (value.length > maxValueLength)
        value.substring(0, maxValueLength - valueCutSuffix.length) + valueCutSuffix
      else value
    val typeInfo = StringParser.parseErrorTypeInfo(cause).getOrElse("requested type")
    s"Error occurred at row $row (line $line) while parsing CSV field '$field' with value [$v] to $typeInfo."
  }
}
