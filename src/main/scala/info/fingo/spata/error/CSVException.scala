/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.error

import java.util.NoSuchElementException
import ParsingErrorCode.ErrorCode
import info.fingo.spata.Position
import info.fingo.spata.text.StringParser

/** Base exception reported by CSV handling methods.
  * May be thrown, raised through [[fs2.Stream#raiseError]] or returned as `Left`, depending on context.
  *
  * `Exception` subclasses denote errors which make CSV parsing or rendering completely impossible,
  * while `Error`s are typically local and mean that at least partial results should be available.
  *
  * For possible error codes see concrete classes implementations.
  *
  * Position is the record and line number at which error has been detected.
  * See [[Position]] for row and line description.
  *
  * `col` is the position (character) at given line at which the error has been detected.
  * `0` means "before first character".
  *
  * `field` is the name (key) of field at which error has been detected.
  * It may be a name from header or a number (starting with `0`) if no header is present.
  *
  * @param message error message
  * @param messageCode error code
  * @param position source row (record) and line at which error occurred
  * @param col column (character) at which error occurred
  * @param field field name at which error occurred
  * @param cause the root exception, if available
  */
sealed abstract class CSVException private[spata] (
  message: String,
  val messageCode: String,
  val position: Option[Position],
  val col: Option[Int],
  val field: FieldInfo,
  cause: Option[Throwable]
) extends Exception(message, cause.orNull)

private[spata] object CSVException {
  def positionInfo(position: Option[Position]): String =
    position.map(p => s" at row ${p.row} (line ${p.line})").getOrElse("")
}

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
  * @param position source row (record) and line at which error occurred
  * @param col column (character) at which error occurred
  * @param field field name at which error occurred
  */
final class StructureException private[spata] (
  errorCode: ErrorCode,
  position: Option[Position],
  col: Option[Int] = None,
  field: FieldInfo = FieldInfo.none
) extends CSVException(
    StructureException.message(errorCode, position, col, field),
    errorCode.code,
    position,
    col,
    field,
    None
  )

private object StructureException {
  def message(errorCode: ErrorCode, position: Option[Position], col: Option[Int], field: FieldInfo): String = {
    val colInfo = col.map(c => s" and column $c").getOrElse("")
    val fieldInfo = if (field.isDefined) s" (field $field)" else ""
    val positionInfo = CSVException.positionInfo(position)
    s"Error occurred$positionInfo$colInfo$fieldInfo while parsing CSV source. ${errorCode.message}"
  }
}

/** Error for content-related issues, typically reported on record level.
  *
  * @see [[CSVException]] for description of fields providing error location.
  *
  * @param message error message
  * @param messageCode error code
  * @param position source row (record) and line at which error occurred
  * @param field field name (header key) at which error occurred
  * @param cause the root exception
  */
sealed abstract class ContentError private[spata] (
  message: String,
  messageCode: String,
  position: Option[Position],
  field: FieldInfo,
  cause: Throwable
) extends CSVException(
    message,
    messageCode,
    position,
    None,
    field,
    Some(cause)
  )

/** Error reported while accessing field with incorrect header value (key).
  *
  * Possible `messageCode`s are:
  *   - `wrongKey` for error caused by accessing field with incorrect key.
  *
  * @see [[CSVException]] for description of fields providing error location.
  *
  * @param position source row (record) and line at which error occurred
  * @param field field information (header key) at which error occurred
  */
final class HeaderError private[spata] (
  position: Option[Position],
  field: FieldInfo
) extends ContentError(
    HeaderError.message(position, field),
    HeaderError.messageCode,
    position,
    field,
    new NoSuchElementException()
  )

private object HeaderError {
  val messageCode = "wrongKey"

  def message(position: Option[Position], field: FieldInfo): String = {
    val positionInfo = CSVException.positionInfo(position)
    s"Error occurred$positionInfo while trying to access CSV field by key '${field.nameInfo}'."
  }
}

/** Error reported while accessing field with incorrect index.
  *
  * Possible `messageCode`s are:
  *   - `wrongIndex` for error caused by accessing field with index out of bounds.
  *
  * @see [[CSVException]] for description of fields providing error location.
  *
  * @param position source row (record) and line at which error occurred
  * @param field field information (index) at which error occurred
  */
final class IndexError private[spata] (
  position: Option[Position],
  field: FieldInfo
) extends ContentError(
    HeaderError.message(position, field),
    HeaderError.messageCode,
    position,
    field,
    new NoSuchElementException()
  )

private object IndexError {
  val messageCode = "wrongIndex"

  def message(position: Option[Position], field: FieldInfo): String = {
    val positionInfo = CSVException.positionInfo(position)
    s"Error occurred$positionInfo while trying to access CSV field by index ${field.indexInfo}."
  }
}

/** Error reported for CSV data problems, caused by string parsing.
  *
  * Possible `messageCode`s are:
  *   - `wrongType` for error caused by parsing field to specific type.
  *
  * @see [[CSVException]] for description of fields providing error location.
  *
  * @param position source row (record) and line at which error occurred
  * @param field field name at which error occurred
  * @param cause the root exception
  */
final class DataError private[spata] (
  value: String,
  position: Option[Position],
  field: FieldInfo,
  cause: Throwable
) extends ContentError(
    DataError.message(value, position, field, cause),
    DataError.messageCode,
    position,
    field,
    cause
  )

private object DataError {
  val messageCode = "wrongType"
  val maxValueLength = 20
  val valueCutSuffix = "..."

  def message(value: String, position: Option[Position], field: FieldInfo, cause: Throwable): String = {
    val v =
      if (value.length > maxValueLength)
        value.substring(0, maxValueLength - valueCutSuffix.length) + valueCutSuffix
      else value
    val typeInfo = StringParser.parseErrorTypeInfo(cause).getOrElse("requested type")
    val positionInfo = CSVException.positionInfo(position)
    s"Error occurred$positionInfo while parsing CSV field ($field) having value [$v] to $typeInfo."
  }
}
