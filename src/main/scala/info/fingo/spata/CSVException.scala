package info.fingo.spata

/** Exception reported by CSV parser.  May be thrown or raised through [[fs2.Stream#raiseError]].
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
abstract class CSVException private[spata] (
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
  *   - `wrongNumberOfFields` when number of values doesn't match header or previous records size.
  *
  * @see [[CSVException]] for description of fields providing error location.
  *
  * @param message error message
  * @param messageCode error code
  * @param line source line at which error occurred, starting from 1
  * @param row row (record) at which error occurred, starting from 1 (`0` for header)
  * @param col column (character) at which error occurred
  * @param field field name at which error occurred
  */
class CSVStructureException private[spata] (
  message: String,
  messageCode: String,
  line: Int,
  row: Int,
  col: Option[Int] = None,
  field: Option[String] = None
) extends CSVException(message, messageCode, line, row, col, field, None)

/** Exception reported for CSV data (content) errors, caused by string parsing.
  *
  * Possible `messageCode`s are:
  *   - `wrongType` for error caused by parsing field to specific type.
  *
  * @see [[CSVException]] for description of fields providing error location.
  *
  * @param message error message
  * @param messageCode error code
  * @param line source line at which error occurred, starting from 1
  * @param row row (record) at which error occurred, starting from 1 (`0` for header)
  * @param field field name at which error occurred
  * @param cause the root exception
  */
class CSVDataException private[spata] (
  message: String,
  messageCode: String,
  line: Int,
  row: Int,
  field: String,
  cause: Throwable
) extends CSVException(message, messageCode, line, row, None, Some(field), Some(cause))
