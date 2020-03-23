package info.fingo.spata.text

/** Exception for string parsing errors.
  *
  * @constructor Create new `DataParseException`
  * @param content the content which has been parsed
  * @param dataType the target data type name
  * @param cause the root exception, if available
  */
class DataParseException(val content: String, val dataType: String, cause: Option[Throwable] = None)
  extends Exception(DataParseException.message(content, dataType), cause.orNull)

/* DataParseException companion object with helper methods */
private object DataParseException {
  val maxInfoLength = 60
  val infoCutSuffix = "..."
  private def message(content: String, dataType: String): String =
    if (content.length > maxInfoLength + 3)
      s"""Cannot parse string starting with "${content.substring(0, maxInfoLength) + infoCutSuffix} as $dataType"""
    else
      s"""Cannot parse "$content" as $dataType"""
}
