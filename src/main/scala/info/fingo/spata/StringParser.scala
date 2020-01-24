package info.fingo.spata

import java.util.Locale
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter
import java.text.{DecimalFormat, NumberFormat, ParseException, ParsePosition}

trait StringParser[A] {
  def parse(str: String): A
}

trait FormattedStringParser[A, B] extends StringParser[A] {
  def parse(str: String, fmt: B): A
}

object StringParser {
  type Maybe[A] = Either[DataParseException, A]

  class Formatter[A](get: String => String = identity) {
    def apply[B](str: String, fmt: B)(implicit parser: FormattedStringParser[A, B]): A = {
      val s = get(str)
      parser.parse(s, fmt)
    }
  }
  class SafeFormatter[A](get: String => String = identity) {
    def apply[B](str: String, fmt: B)(implicit parser: FormattedStringParser[A, B]): Maybe[A] = {
      val s = get(str)
      maybe(parser.parse(s, fmt))
    }
  }

  def parse[A](str: String)(implicit parser: StringParser[A]): A = parser.parse(str)
  def parse[A]: Formatter[A] = new Formatter[A]

  def attempt[A](str: String)(implicit parser: StringParser[A]): Either[DataParseException, A] =
    maybe(parser.parse(str))
  def attempt[A]: SafeFormatter[A] = new SafeFormatter[A]

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

  def maybe[A](code: => A): Maybe[A] =
    try {
      Right(code)
    } catch {
      case ex: DataParseException => Left(ex)
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
