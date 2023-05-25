/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.text

/** Exception for string parsing errors.
  *
  * @constructor Creates new `ParseError`
  * @param content the content which has been parsed
  * @param dataType the target data type description
  * @param cause the root exception, if available
  */
final class ParseError(val content: String, val dataType: Option[String] = None, cause: Option[Throwable] = None)
  extends Exception(ParseError.message(content, dataType), cause.orNull)

/* ParseError companion object with helper methods. */
private object ParseError:
  val maxInfoLength = 60
  val infoCutSuffix = "..."
  private def message(content: String, dataType: Option[String]): String =
    val typeInfo = dataType.getOrElse("type")
    if content.length > maxInfoLength + 3
    then
      val init = content.substring(0, maxInfoLength) + infoCutSuffix
      s"Cannot parse string starting with [$init] to requested $typeInfo"
    else s"Cannot parse [$content] to requested $typeInfo"
