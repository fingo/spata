package info.fingo.spata

import java.util.Locale
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter
import java.text.{DecimalFormat, NumberFormat, ParseException, ParsePosition}

trait StringParser[A] {
  def parse(str: String): A
}

trait FormattedStringParser[A] extends StringParser[A] {
  type FmtType
  def parse(str: String, fmt: FmtType): A
}

object StringParser {
  type Aux[A, B] = FormattedStringParser[A] { type FmtType = B }

  class Formatter[A](get: String => String = identity) {
    def apply[B](str: String, fmt: B)(implicit parser: Aux[A, B]): A = {
      val s = get(str)
      parser.parse(s, fmt)
    }
  }

  def parse[A](str: String)(implicit parser: StringParser[A]): A = parser.parse(str)
  def parse[A]: Formatter[A] = new Formatter[A]

  implicit def optionParser[A](implicit parser: StringParser[A]): StringParser[Option[A]] =
    (str: String) => if (str == null || str.isBlank) None else Some(parser.parse(str))

  implicit def fmtOptionParser[A](
    implicit parser: FormattedStringParser[A]
  ): FormattedStringParser[Option[A]] { type FmtType = parser.FmtType } =
    new FormattedStringParser[Option[A]] {
      override type FmtType = parser.FmtType
      override def parse(str: String): Option[A] =
        if (str == null || str.isBlank) None else Some(parser.parse(str))
      override def parse(str: String, fmt: FmtType): Option[A] =
        if (str == null || str.isBlank) None else Some(parser.parse(str, fmt))
    }

  implicit val stringParser: StringParser[String] = (str: String) => str

  implicit val intParser: StringParser[Int] = (str: String) => wrapException(str, "Int") { str.strip.toInt }

  implicit val longParser: FormattedStringParser[Long] { type FmtType = NumberFormat } =
    new FormattedStringParser[Long] {
      override type FmtType = NumberFormat
      override def parse(str: String): Long = wrapException(str, "Long") { str.strip.toLong }
      override def parse(str: String, fmt: FmtType): Long = wrapException(str, "Long") {
        parseNumber(str, fmt).longValue()
      }
    }

  implicit val doubleParser: FormattedStringParser[Double] { type FmtType = DecimalFormat } =
    new FormattedStringParser[Double] {
      override type FmtType = DecimalFormat
      override def parse(str: String): Double = wrapException(str, "Double") { str.strip.toDouble }
      override def parse(str: String, fmt: FmtType): Double = wrapException(str, "Double") {
        parseNumber(str, fmt).doubleValue()
      }
    }

  implicit val bigDecimalParser: FormattedStringParser[BigDecimal] { type FmtType = DecimalFormat } =
    new FormattedStringParser[BigDecimal] {
      override type FmtType = DecimalFormat
      override def parse(str: String): BigDecimal = wrapException(str, "BigDecimal") { BigDecimal(str.strip) }
      override def parse(str: String, fmt: FmtType): BigDecimal = wrapException(str, "BigDecimal") {
        fmt.setParseBigDecimal(true)
        BigDecimal(parseNumber(str, fmt).asInstanceOf[java.math.BigDecimal])
      }
    }

  implicit val localDateParser: FormattedStringParser[LocalDate] { type FmtType = DateTimeFormatter } =
    new FormattedStringParser[LocalDate] {
      override type FmtType = DateTimeFormatter
      override def parse(str: String): LocalDate = wrapException(str, "LocalDate") { LocalDate.parse(str.strip) }
      override def parse(str: String, fmt: FmtType): LocalDate = wrapException(str, "LocalDate") {
        LocalDate.parse(str.strip, fmt)
      }
    }

  implicit val localTimeParser: FormattedStringParser[LocalTime] { type FmtType = DateTimeFormatter } =
    new FormattedStringParser[LocalTime] {
      override type FmtType = DateTimeFormatter
      override def parse(str: String): LocalTime = wrapException(str, "LocalTime") { LocalTime.parse(str.strip) }
      override def parse(str: String, fmt: FmtType): LocalTime = wrapException(str, "LocalTime") {
        LocalTime.parse(str.strip, fmt)
      }
    }

  implicit val localDateTimeParser: FormattedStringParser[LocalDateTime] { type FmtType = DateTimeFormatter } =
    new FormattedStringParser[LocalDateTime] {
      override type FmtType = DateTimeFormatter
      override def parse(str: String): LocalDateTime = wrapException(str, "LocalDateTime") {
        LocalDateTime.parse(str.strip)
      }
      override def parse(str: String, fmt: FmtType): LocalDateTime = wrapException(str, "LocalDateTime") {
        LocalDateTime.parse(str.strip, fmt)
      }
    }

  implicit val booleanParser: FormattedStringParser[Boolean] { type FmtType = BooleanFormatter } =
    new FormattedStringParser[Boolean] {
      override type FmtType = BooleanFormatter
      override def parse(str: String): Boolean = BooleanFormatter.default.parse(str)
      override def parse(str: String, fmt: FmtType): Boolean = fmt.parse(str)
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
