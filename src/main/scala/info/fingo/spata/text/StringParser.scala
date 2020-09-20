/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.text

import java.text.{DecimalFormat, NumberFormat, ParseException, ParsePosition}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import scala.util.control.NonFatal

/** Parser from `String` to desired type.
  *
  * This parser defines behavior to be implemented by concrete, implicit parsers for various types,
  * used by [[StringParser.parse[A](str:String)* parse]] function from [[StringParser$ StringParser]] object.
  *
  * Parsing fails on an empty input if a simple type is provided.
  * To accept empty input `Option[_]` should be provided as type parameter.
  *
  * @tparam A target type for parsing
  */
trait StringParser[A] {

  /** Parses string to desired type.
    *
    * If parsing fails this function should report error by throwing exception.
    * This is further handled by [[StringParser#parse[A](* parse]] to allow for exception-free, functional code.
    *
    * @note This function assumes "standard" string formatting,
    * e.g. point as decimal separator or ISO date and time formats, without any locale support.
    * Use [[FormattedStringParser]] if more control over source format is required.
    *
    * @param str the input string
    * @return parsed value
    * @throws RuntimeException if text cannot be parsed to requested type
    */
  @throws[RuntimeException]("if text cannot be parsed to requested type")
  def apply(str: String): A
}

/** Parser from `String` to desired type with support for different formats.
  *
  * This parser defines behavior to be implemented by concrete, implicit parsers for various types,
  * used by [[StringParser.parse[A](str:String)* parse]] function from [[StringParser$ StringParser]] object.
  *
  * Parsing fails on an empty input if a simple type is provided.
  * To accept empty input `Option[_]` should be provided as type parameter.
  *
  * @tparam A target type for parsing
  * @tparam B type of formatter
  */
trait FormattedStringParser[A, B] extends StringParser[A] {

  /** Parses string to desired type based on provided format.
    *
    * If parsing fails this function should throw [[DataParseException]], possibly wrapping root exception.
    * This is further handled by [[StringParser#parse[A]:* parse]] to allow for exception-free, functional code.
    *
    * @param str the input string
    * @param fmt formatter, specific for particular result type, e.g. `DateTimeFormatter` for dates and times
    * @return parsed value
    * @throws RuntimeException if text cannot be parsed to requested type
    */
  @throws[RuntimeException]("if text cannot be parsed to requested type")
  def apply(str: String, fmt: B): A
}

/** Parsing methods from `String` to various simple types.
  *
  * Contains parsers for common types, like numbers, dates and times.
  *
  * Additional parsers may be provided by implementing [[StringParser]] or [[FormattedStringParser]] traits
  * and putting implicit values in scope.
  * `StringParser` may be implemented if there are no different formatting options for given type, e.g. for integers
  * (although one may argue if this is a good example). For all cases when different formatting options exist,
  * `FormattedStringParser` should be implemented.
  */
object StringParser {

  /** Intermediary to delegate parsing to in order to infer type of formatter used by parser.
    *
    * When adding parsing function to an entity, instead of providing
    * {{{def parse[A, B](input: A, format: B)(implicit parser: FormattedStringParser[A, B]): ValueOrError[A]}}}
    * which requires then to use it as
    * {{{entity.parse[Double, NumberFormat]("123,45", numberFormat)}}}
    * one can provide function to get this `Pattern`
    * {{{def parse[A]: Pattern[A]}}}
    * and use it with nicer syntax
    * {{{entity.parse[Double]("123,45", numberFormat)}}}
    *
    * @tparam A target type for parsing
    */
  class Pattern[A]() {

    /** Safely parses string to desired type based on provided format.
      *
      * @param str the input string to parse
      * @param fmt formatter specific for particular result type, e.g. `DateTimeFormatter` for dates and times
      * @param parser the parser for specific target type
      * @tparam B type of formatter
      * @return either parsed value or an exception
      */
    def apply[B](str: String, fmt: B)(implicit parser: FormattedStringParser[A, B]): ValueOrError[A] =
      wrapExc(str) { parser(str, fmt) }
  }

  /** Safely parses string to desired type.
    *
    * @example {{{
    * import info.fingo.spata.text.StringParser._
    * val x = parse[Double]("123.45").getOrElse(Double.NaN)
    * val y = parse[Option[Double]]("123.45").map(_.getOrElse(0.0)).getOrElse(Double.NaN)
    * }}}
    *
    * @param str the input string
    * @param parser the parser for specific target type
    * @tparam A target type for parsing
    * @return either parsed value or an [[DataParseException]]
    */
  def parse[A](str: String)(implicit parser: StringParser[A]): ValueOrError[A] =
    wrapExc(str) { parser(str) }

  /** Parses string to desired type based on provided format.
    *
    * Delegates actual parsing to [[Pattern#apply]] method.
    *
    * @see [[parse[A]:* parse]] for sample usage.
    * @tparam A target type for parsing
    * @return intermediary to retrieve value according to custom format
    */
  def parse[A]: Pattern[A] = new Pattern[A]

  /** Parser for optional values.
    * Allows conversion of any simple parser to return `Option[A]` instead of `A`, avoiding error for empty string.
    *
    * @param parser the parser for underlying simple type
    * @tparam A the simple type wrapped by [[scala.Option]]
    * @return parser which accepts empty input
    */
  implicit def optionParser[A](implicit parser: StringParser[A]): StringParser[Option[A]] =
    (str: String) => if (str == null || str.isBlank) None else Some(parser(str))

  /** Parser for optional values with support for different formats.
    * Allows conversion of any simple parser to return `Option[A]` instead of `A`, avoiding error for empty string.
    *
    * @param parser the parser for underlying simple type
    * @tparam A the simple type wrapped by [[scala.Option]]
    * @tparam B type of formatter
    * @return parser which support formatted input and accepts empty one
    */
  implicit def fmtOptionParser[A, B](
    implicit parser: FormattedStringParser[A, B]
  ): FormattedStringParser[Option[A], B] =
    new FormattedStringParser[Option[A], B] {
      override def apply(str: String): Option[A] = if (str == null || str.isBlank) None else Some(parser(str))
      override def apply(str: String, fmt: B): Option[A] =
        if (str == null || str.isBlank) None else Some(parser(str, fmt))
    }

  /** No-op parser for strings. */
  implicit val stringParser: StringParser[String] = (str: String) => str

  /** Parser for integer values. */
  implicit val intParser: StringParser[Int] = (str: String) => str.strip.toInt

  /** Parser for long values with support for formats. */
  implicit val longParser: FormattedStringParser[Long, NumberFormat] =
    new FormattedStringParser[Long, NumberFormat] {
      override def apply(str: String): Long = str.strip.toLong
      override def apply(str: String, fmt: NumberFormat): Long = parseNumber(str, fmt).longValue()
    }

  /** Parser for double values with support for formats. */
  implicit val doubleParser: FormattedStringParser[Double, DecimalFormat] =
    new FormattedStringParser[Double, DecimalFormat] {
      override def apply(str: String): Double = str.strip.toDouble
      override def apply(str: String, fmt: DecimalFormat): Double = parseNumber(str, fmt).doubleValue()
    }

  /** Parser for decimal values with support for formats. */
  implicit val bigDecimalParser: FormattedStringParser[BigDecimal, DecimalFormat] =
    new FormattedStringParser[BigDecimal, DecimalFormat] {
      override def apply(str: String): BigDecimal = BigDecimal(str.strip)
      override def apply(str: String, fmt: DecimalFormat): BigDecimal = {
        fmt.setParseBigDecimal(true)
        BigDecimal(parseNumber(str, fmt).asInstanceOf[java.math.BigDecimal])
      }
    }

  /** Parser for date values with support for formats. */
  implicit val localDateParser: FormattedStringParser[LocalDate, DateTimeFormatter] =
    new FormattedStringParser[LocalDate, DateTimeFormatter] {
      override def apply(str: String): LocalDate = LocalDate.parse(str.strip)
      override def apply(str: String, fmt: DateTimeFormatter): LocalDate = LocalDate.parse(str.strip, fmt)
    }

  /** Parser for time values with support for formats. */
  implicit val localTimeParser: FormattedStringParser[LocalTime, DateTimeFormatter] =
    new FormattedStringParser[LocalTime, DateTimeFormatter] {
      override def apply(str: String): LocalTime = LocalTime.parse(str.strip)
      override def apply(str: String, fmt: DateTimeFormatter): LocalTime = LocalTime.parse(str.strip, fmt)
    }

  /** Parser for date with time values with support for formats. */
  implicit val localDateTimeParser: FormattedStringParser[LocalDateTime, DateTimeFormatter] =
    new FormattedStringParser[LocalDateTime, DateTimeFormatter] {
      override def apply(str: String): LocalDateTime = LocalDateTime.parse(str.strip)
      override def apply(str: String, fmt: DateTimeFormatter): LocalDateTime = LocalDateTime.parse(str.strip, fmt)
    }

  /** Parser for boolean values with support for formats. */
  implicit val booleanParser: FormattedStringParser[Boolean, BooleanFormatter] =
    new FormattedStringParser[Boolean, BooleanFormatter] {
      override def apply(str: String): Boolean = BooleanFormatter.default.parse(str)
      override def apply(str: String, fmt: BooleanFormatter): Boolean = fmt.parse(str)
    }

  /* Parses whole string to number (since NumberFormat accepts partial input). */
  private def parseNumber(str: String, fmt: NumberFormat): Number = {
    val pos = new ParsePosition(0)
    val s = str.strip
    val num = fmt.parse(s, pos)
    if (pos.getIndex != s.length)
      throw new DataParseException(
        str,
        Some("number"),
        Some(new ParseException(s"Cannot parse $str as number", pos.getIndex))
      )
    num
  }

  /* Wraps any parsing exception in DataParseException. */
  private def wrapExc[A](content: String)(code: => A): ValueOrError[A] =
    try Right(code)
    catch {
      case ex: DataParseException => Left(ex)
      case NonFatal(ex) => Left(new DataParseException(content, parseErrorTypeInfo(ex), Some(ex)))
    }

  /* Gets type of parsed value based on type of exception thrown while parsing. */
  private[spata] def parseErrorTypeInfo(ex: Throwable): Option[String] = ex match {
    case e: DataParseException => e.dataType
    case _: NumberFormatException => Some("number")
    case _: DateTimeParseException => Some("date/time")
    case _ => None
  }
}
