package info.fingo.spata

import java.util.Locale
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter
import java.text.{DecimalFormat, NumberFormat, ParseException, ParsePosition}

trait SimpleStringParser[A] {
  def parse(str: String): A
}

trait StringParser[A] extends SimpleStringParser[A] {
  type FmtType
  def parse(str: String, fmt: FmtType): A
}

object StringParser {
  type Aux[A, B] = StringParser[A] { type FmtType = B }

  def parse[A](str: String)(implicit parser: SimpleStringParser[A]): Option[A] =
    if (str == null || str.isBlank) None else Some(parser.parse(str.trim))
  def parse[A, B](str: String, fmt: B)(implicit parser: Aux[A, B]): Option[A] =
    if (str == null || str.isBlank) None else Some(parser.parse(str.trim, fmt))

  implicit val stringParser: SimpleStringParser[String] = (str: String) => str

  implicit val intParser: SimpleStringParser[Int] = (str: String) => wrapException(str, "Int") { str.toInt }

  implicit val longParser: StringParser[Long] { type FmtType = NumberFormat } =
    new StringParser[Long] {
      override type FmtType = NumberFormat
      override def parse(str: String): Long = wrapException(str, "Long") { str.toLong }
      override def parse(str: String, fmt: FmtType): Long = wrapException(str, "Long") {
        parseNumber(str, fmt).longValue()
      }
    }

  implicit val doubleParser: StringParser[Double] { type FmtType = DecimalFormat } =
    new StringParser[Double] {
      override type FmtType = DecimalFormat
      override def parse(str: String): Double = wrapException(str, "Double") { str.toDouble }
      override def parse(str: String, fmt: FmtType): Double = wrapException(str, "Double") {
        parseNumber(str, fmt).doubleValue()
      }
    }

  implicit val bigDecimalParser: StringParser[BigDecimal] { type FmtType = DecimalFormat } =
    new StringParser[BigDecimal] {
      override type FmtType = DecimalFormat
      override def parse(str: String): BigDecimal = wrapException(str, "BigDecimal") { BigDecimal(str) }
      override def parse(str: String, fmt: FmtType): BigDecimal = wrapException(str, "BigDecimal") {
        fmt.setParseBigDecimal(true)
        BigDecimal(parseNumber(str, fmt).asInstanceOf[java.math.BigDecimal])
      }
    }

  implicit val localDateParser: StringParser[LocalDate] { type FmtType = DateTimeFormatter } =
    new StringParser[LocalDate] {
      override type FmtType = DateTimeFormatter
      override def parse(str: String): LocalDate = wrapException(str, "LocalDate") { LocalDate.parse(str) }
      override def parse(str: String, fmt: FmtType): LocalDate = wrapException(str, "LocalDate") {
        LocalDate.parse(str, fmt)
      }
    }

  implicit val localTimeParser: StringParser[LocalTime] { type FmtType = DateTimeFormatter } =
    new StringParser[LocalTime] {
      override type FmtType = DateTimeFormatter
      override def parse(str: String): LocalTime = wrapException(str, "LocalTime") { LocalTime.parse(str) }
      override def parse(str: String, fmt: FmtType): LocalTime = wrapException(str, "LocalTime") {
        LocalTime.parse(str, fmt)
      }
    }

  implicit val localDateTimeParser: StringParser[LocalDateTime] { type FmtType = DateTimeFormatter } =
    new StringParser[LocalDateTime] {
      override type FmtType = DateTimeFormatter
      override def parse(str: String): LocalDateTime = wrapException(str, "LocalDateTime") { LocalDateTime.parse(str) }
      override def parse(str: String, fmt: FmtType): LocalDateTime = wrapException(str, "LocalDateTime") {
        LocalDateTime.parse(str, fmt)
      }
    }

  implicit val booleanParser: StringParser[Boolean] { type FmtType = BooleanFormatter } =
    new StringParser[Boolean] {
      override type FmtType = BooleanFormatter
      override def parse(str: String): Boolean = BooleanFormatter.default.parse(str)
      override def parse(str: String, fmt: FmtType): Boolean = fmt.parse(str)
    }

  private def parseNumber(str: String, fmt: NumberFormat): Number = {
    val pos = new ParsePosition(0)
    val num = fmt.parse(str, pos)
    if (pos.getIndex != str.length) throw new ParseException(s"Cannot parse $str as number", pos.getIndex)
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
