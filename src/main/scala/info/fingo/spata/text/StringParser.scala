package info.fingo.spata.text

import java.text.{DecimalFormat, NumberFormat, ParseException, ParsePosition}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.Locale

import info.fingo.spata.{maybe, Maybe}

/** Parser from `String` to desired type.
  *
  * It defines behavior to be implemented by concrete, implicit parsers for various types,
  * used by `parse` function from `StringParser` object.
  *
  * Parsing fails on an empty input if a simple type is provided.
  * To accept empty input `Option[T]` should be provided as type parameter.
  *
  * @tparam A target type for parsing
  */
trait StringParser[A] {

  /** Parse string to desired type.
    *
    * If parsing fails this function should throw [[DataParseException]], possibly wrapping root exception.
    * This is further handled by [[StringParser#parseSafe[A](* parseSafe]] to allow for exception-free, functional code.
    *
    * @note This function assumes "standard" string formatting,
    * e.g. point as decimal separator or ISO date and time formats, without any locale support.
    * Use [[FormattedStringParser]] if more control over source format is required
    *
    * @param str the input string
    * @return parsed value
    * @throws DataParseException if field cannot be parsed to requested type
    */
  @throws[DataParseException]("if field cannot be parsed to requested type")
  def parse(str: String): A
}

/** Parser from `String` to desired type with support for different formats.
  *
  * It defines behavior to be implemented by concrete, implicit parsers for various types,
  * used by `parse` function from `StringParser` object.
  *
  * Parsing fails on an empty input if a simple type is provided.
  * To accept empty input `Option[T]` should be provided as type parameter.
  *
  * @tparam A target type for parsing
  * @tparam B type of formatter
  */
trait FormattedStringParser[A, B] extends StringParser[A] {

  /** Parse string to desired type based on provided format.
    *
    * If parsing fails this function should throw [[DataParseException]], possibly wrapping root exception.
    * This is further handled by [[StringParser#parseSafe[A]:* parseSafe]] to allow for exception-free, functional code.
    *
    * @param str the input string
    * @param fmt formatter, specific for particular result type, e.g. `DateTimeFormatter` for dates and times
    * @return parsed value
    * @throws DataParseException if field cannot be parsed to requested type
    */
  @throws[DataParseException]("if field cannot be parsed to requested type")
  def parse(str: String, fmt: B): A
}

/** Parsing methods from `String` to various simple types.
  *
  * Contains parsers for common types, like numbers, dates and times.
  *
  * Additional parsers may be provided by implementing [[StringParser]] or [[FormattedStringParser]] traits
  * and putting implicit values in scope.
  * `StringParser` may be implemented if there are no different formatting options for given type, e.g. for integers
  * (although one may argue in this case). For all cases when different formatting options exist,
  * `FormattedStringParser` should be implemented.
  */
object StringParser {

  /** Intermediary to delegate parsing to in order to infer type of formatter used by parser.
    *
    * When adding parsing function to an entity, instead of providing
    * {{{def parse[A, B](input: A, format: B)(implicit parser: FormattedStringParser[A, B]): A}}}
    * which requires then to use it as
    * {{{entity.parse[Double, NumberFormat]("123,45", numberFormat)}}}
    * one can provide function to get this `Pattern`
    * {{{def parse[A]: Pattern[A]}}}
    * and use it with nicer syntax
    * {{{entity.parse[Double]("123,45", numberFormat)}}}
    *
    * When one needs the formatter to retrieve the string to be parsed, e.g. get value from a map based on key,
    * it is possible to do it through the `get` parameter function, still keeping above shorter syntax of parsing call:
    * {{{
    * import info.fingo.spata.text.StringParser._
    * val map = Map("v1" -> "123,45", "v2" -> "543,21")
    * def retrieve[A]: Pattern[A] = new Pattern[A](s => map(s))
    * val locale = new java.util.Locale("pl", "PL")
    * val fmt = java.text.NumberFormat.getInstance(locale).asInstanceOf[java.text.DecimalFormat]
    * val result1 = retrieve[Double]("v1", fmt)
    * val result2 = retrieve[Double]("v2", fmt)
    * }}}
    *
    * @param get the function to retrieve string to be parsed - identity by default
    * @tparam A target type for parsing
    */
  class Pattern[A](get: String => String = identity) {

    /** Parse string to desired type based on provided format.
      *
      * @param str the input string to parse
      * @param fmt concrete formatter, specific for particular result type, e.g. `DateTimeFormatter` for dates and times
      * @param parser the parser for specific target type
      * @tparam B type of concrete formatter
      * @return parsed value
      * @throws DataParseException if field cannot be parsed to requested type
      */
    @throws[DataParseException]("if field cannot be parsed to requested type")
    def apply[B](str: String, fmt: B)(implicit parser: FormattedStringParser[A, B]): A = {
      val s = get(str)
      parser.parse(s, fmt)
    }
  }

  /** Intermediary to delegate parsing to in order to infer type of formatter used by parser.
    * Provide exception-free parsing method
    *
    * @see [[Pattern]] for more information and example usage.
    * @param get the function to retrieve string to be parsed - identity by default
    * @tparam A target type for parsing
    */
  class SafePattern[A](get: String => String = identity) {

    /** Safely parse string to desired type based on provided format.
      *
      * @param str the input string to parse
      * @param fmt formatter specific for particular result type, e.g. `DateTimeFormatter` for dates and times
      * @param parser the parser for specific target type
      * @tparam B type of formatter
      * @return either parsed value or an exception
      */
    def apply[B](str: String, fmt: B)(implicit parser: FormattedStringParser[A, B]): Maybe[A] = maybe {
      val s = get(str)
      parser.parse(s, fmt)
    }
  }

  /** Parse string to desired type.
    *
    * @example {{{
    * import info.fingo.spata.text.StringParser._
    * val result = parse[Double]("123.45")
    * }}}
    *
    * @param str the input string
    * @param parser the parser for specific target type
    * @tparam A target type for parsing
    * @return parsed value
    * @throws DataParseException if field cannot be parsed to requested type
    */
  @throws[DataParseException]("if field cannot be parsed to requested type")
  def parse[A](str: String)(implicit parser: StringParser[A]): A = parser.parse(str)

  /** Parse string to desired type based on provided format.
    *
    * Delegate actual parsing to [[Pattern#apply]] method
    *
    * @example {{{
    * import info.fingo.spata.text.StringParser._
    * val locale = new java.util.Locale("pl", "PL")
    * val fmt = java.text.NumberFormat.getInstance(locale).asInstanceOf[java.text.DecimalFormat]
    * val result1 = parse[Double]("123,45", fmt)
    * }}}
    *
    * @tparam A target type for parsing
    * @return intermediary to retrieve value according to custom format
    * @throws DataParseException if field cannot be parsed to requested type
    */
  @throws[DataParseException]("if field cannot be parsed to requested type")
  def parse[A]: Pattern[A] = new Pattern[A]

  /** Safely parse string to desired type.
    *
    * @see [[parse[A](* parse]] for sample usage
    *
    * @param str the input string
    * @param parser the parser for specific target type
    * @tparam A target type for parsing
    * @return either parsed value or an [[DataParseException]]
    */
  def parseSafe[A](str: String)(implicit parser: StringParser[A]): Maybe[A] =
    maybe(parser.parse(str))

  /** Parse string to desired type based on provided format.
    *
    * Delegate actual parsing to [[SafePattern#apply]] method
    *
    * @see [[parse[A]:* parse]] for sample usage
    *
    * @tparam A target type for parsing
    * @return intermediary to retrieve value according to custom format
    */
  def parseSafe[A]: SafePattern[A] = new SafePattern[A]

  implicit def optionParser[A](implicit parser: StringParser[A]): StringParser[Option[A]] =
    (str: String) => if (str == null || str.isBlank) None else Some(parser.parse(str))

  implicit def fmtOptionParser[A, B](
    implicit parser: FormattedStringParser[A, B]
  ): FormattedStringParser[Option[A], B] =
    new FormattedStringParser[Option[A], B] {
      override def parse(str: String): Option[A] = if (str == null || str.isBlank) None else Some(parser.parse(str))
      override def parse(str: String, fmt: B): Option[A] =
        if (str == null || str.isBlank) None else Some(parser.parse(str, fmt))
    }

  implicit val stringParser: StringParser[String] = (str: String) => str

  implicit val intParser: StringParser[Int] = (str: String) => wrapException(str, "Int") { str.strip.toInt }

  implicit val longParser: FormattedStringParser[Long, NumberFormat] =
    new FormattedStringParser[Long, NumberFormat] {
      override def parse(str: String): Long = wrapException(str, "Long") { str.strip.toLong }
      override def parse(str: String, fmt: NumberFormat): Long = wrapException(str, "Long") {
        parseNumber(str, fmt).longValue()
      }
    }

  implicit val doubleParser: FormattedStringParser[Double, DecimalFormat] =
    new FormattedStringParser[Double, DecimalFormat] {
      override def parse(str: String): Double = wrapException(str, "Double") { str.strip.toDouble }
      override def parse(str: String, fmt: DecimalFormat): Double = wrapException(str, "Double") {
        parseNumber(str, fmt).doubleValue()
      }
    }

  implicit val bigDecimalParser: FormattedStringParser[BigDecimal, DecimalFormat] =
    new FormattedStringParser[BigDecimal, DecimalFormat] {
      override def parse(str: String): BigDecimal = wrapException(str, "BigDecimal") { BigDecimal(str.strip) }
      override def parse(str: String, fmt: DecimalFormat): BigDecimal = wrapException(str, "BigDecimal") {
        fmt.setParseBigDecimal(true)
        BigDecimal(parseNumber(str, fmt).asInstanceOf[java.math.BigDecimal])
      }
    }

  implicit val localDateParser: FormattedStringParser[LocalDate, DateTimeFormatter] =
    new FormattedStringParser[LocalDate, DateTimeFormatter] {
      override def parse(str: String): LocalDate = wrapException(str, "LocalDate") { LocalDate.parse(str.strip) }
      override def parse(str: String, fmt: DateTimeFormatter): LocalDate = wrapException(str, "LocalDate") {
        LocalDate.parse(str.strip, fmt)
      }
    }

  implicit val localTimeParser: FormattedStringParser[LocalTime, DateTimeFormatter] =
    new FormattedStringParser[LocalTime, DateTimeFormatter] {
      override def parse(str: String): LocalTime = wrapException(str, "LocalTime") { LocalTime.parse(str.strip) }
      override def parse(str: String, fmt: DateTimeFormatter): LocalTime = wrapException(str, "LocalTime") {
        LocalTime.parse(str.strip, fmt)
      }
    }

  implicit val localDateTimeParser: FormattedStringParser[LocalDateTime, DateTimeFormatter] =
    new FormattedStringParser[LocalDateTime, DateTimeFormatter] {
      override def parse(str: String): LocalDateTime = wrapException(str, "LocalDateTime") {
        LocalDateTime.parse(str.strip)
      }
      override def parse(str: String, fmt: DateTimeFormatter): LocalDateTime = wrapException(str, "LocalDateTime") {
        LocalDateTime.parse(str.strip, fmt)
      }
    }

  implicit val booleanParser: FormattedStringParser[Boolean, BooleanFormatter] =
    new FormattedStringParser[Boolean, BooleanFormatter] {
      override def parse(str: String): Boolean = BooleanFormatter.default.parse(str)
      override def parse(str: String, fmt: BooleanFormatter): Boolean = fmt.parse(str)
    }

  private def parseNumber(str: String, fmt: NumberFormat): Number = {
    val pos = new ParsePosition(0)
    val s = str.strip
    val num = fmt.parse(s, pos)
    if (pos.getIndex != s.length) throw new ParseException(s"Cannot parse $str as number", pos.getIndex)
    num
  }

  private def wrapException[A](content: String, dataType: String)(code: => A): A =
    try code
    catch {
      case ex: Exception => throw new DataParseException(content, dataType, Some(ex))
    }
}

class BooleanFormatter(tt: String, ft: String, locale: Locale) {
  val trueTerm: String = tt.toLowerCase(locale)
  val falseTerm: String = ft.toLowerCase(locale)

  def this(trueTerm: String, falseTerm: String) = this(trueTerm, falseTerm, Locale.getDefault())

  def format(value: Boolean): String = if (value) trueTerm else falseTerm
  def parse(string: String): Boolean = string.strip().toLowerCase(locale) match {
    case `trueTerm` => true
    case `falseTerm` => false
    case _ => throw new DataParseException(string, "Boolean")
  }
}

object BooleanFormatter {
  def apply(tt: String, ft: String, locale: Locale): BooleanFormatter = new BooleanFormatter(tt, ft, locale)
  def apply(tt: String, ft: String): BooleanFormatter = new BooleanFormatter(tt, ft)
  val default: BooleanFormatter = apply(true.toString, false.toString)
}

class DataParseException(val content: String, val dataType: String, cause: Option[Throwable] = None)
  extends Exception(DataParseException.message(content, dataType), cause.orNull)

object DataParseException {
  val maxInfoLength = 60
  val infoCutSuffix = "..."
  private def message(content: String, dataType: String): String =
    if (content.length > maxInfoLength + 3)
      s"""Cannot parse string starting with "${content.substring(0, maxInfoLength) + infoCutSuffix} as $dataType"""
    else
      s"""Cannot parse "$content" as $dataType"""
}
