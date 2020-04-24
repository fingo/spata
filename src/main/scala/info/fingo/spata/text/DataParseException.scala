/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.text

/** Exception for string parsing errors.
  *
  * @constructor Creates new `DataParseException`
  * @param content the content which has been parsed
  * @param dataType the target data type description
  * @param cause the root exception, if available
  */
class DataParseException(val content: String, val dataType: Option[String] = None, cause: Option[Throwable] = None)
  extends Exception(DataParseException.message(content, dataType), cause.orNull)

/* DataParseException companion object with helper methods. */
private object DataParseException {
  val maxInfoLength = 60
  val infoCutSuffix = "..."
  private def message(content: String, dataType: Option[String]): String =
    if (content.length > maxInfoLength + 3)
      s"""Cannot parse string starting with "${content.substring(0, maxInfoLength) + infoCutSuffix} as requested ${dataType
        .getOrElse("type")}"""
    else
      s"""Cannot parse "$content" as requested ${dataType.getOrElse("type")}"""
}
