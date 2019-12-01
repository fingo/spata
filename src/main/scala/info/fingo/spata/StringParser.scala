package info.fingo.spata

import java.text.{DecimalFormat, NumberFormat, ParseException, ParsePosition}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter
import java.util.Locale

trait SimpleStringParser[A] {
  def parse(str: String): A
}

trait StringParser[A] extends SimpleStringParser[A] {
  type FmtType
  def parse(str: String, fmt: FmtType): A
}

object StringParser {
  type Aux[A,B] = StringParser[A] {type FmtType = B}

  def parse[A](str: String)(implicit parser: SimpleStringParser[A]): Option[A] =
    if(str == null || str.isBlank) None else Some(parser.parse(str.trim))
  def parse[A,B](str: String, fmt: B)(implicit parser: Aux[A,B]): Option[A] =
    if(str == null || str.isBlank) None else Some(parser.parse(str.trim, fmt))

  implicit val stringParser: SimpleStringParser[String] = (str: String) => str

  implicit val intParser: SimpleStringParser[Int] = (str: String) => str.toInt

  implicit val longParser: StringParser[Long] {type FmtType = NumberFormat} =
    new StringParser[Long] {
      override type FmtType = NumberFormat
      override def parse(str: String): Long = str.toLong
      override def parse(str: String, fmt: FmtType): Long = parseNumber(str, fmt).longValue()
    }

  implicit val doubleParser: StringParser[Double] {type FmtType = DecimalFormat} =
    new StringParser[Double] {
      override type FmtType = DecimalFormat
      override def parse(str: String): Double = str.toDouble
      override def parse(str: String, fmt: FmtType): Double = parseNumber(str, fmt).doubleValue()
    }

  implicit val bigDecimalParser: StringParser[BigDecimal] {type FmtType = DecimalFormat} =
    new StringParser[BigDecimal] {
      override type FmtType = DecimalFormat
      override def parse(str: String): BigDecimal = BigDecimal(str)
      override def parse(str: String, fmt: FmtType): BigDecimal = {
        fmt.setParseBigDecimal(true)
        BigDecimal(parseNumber(str, fmt).asInstanceOf[java.math.BigDecimal])
      }
  }

  implicit val localDateParser: StringParser[LocalDate] {type FmtType = DateTimeFormatter} =
    new StringParser[LocalDate] {
      override type FmtType = DateTimeFormatter
      override def parse(str: String): LocalDate = LocalDate.parse(str)
      override def parse(str: String, fmt: FmtType): LocalDate = LocalDate.parse(str, fmt)
    }

  implicit val localTimeParser: StringParser[LocalTime] {type FmtType = DateTimeFormatter} =
    new StringParser[LocalTime] {
      override type FmtType = DateTimeFormatter
      override def parse(str: String): LocalTime = LocalTime.parse(str)
      override def parse(str: String, fmt: FmtType): LocalTime = LocalTime.parse(str, fmt)
    }

  implicit val localDateTimeParser: StringParser[LocalDateTime] {type FmtType = DateTimeFormatter} =
    new StringParser[LocalDateTime] {
      override type FmtType = DateTimeFormatter
      override def parse(str: String): LocalDateTime = LocalDateTime.parse(str)
      override def parse(str: String, fmt: FmtType): LocalDateTime = LocalDateTime.parse(str, fmt)
    }

  implicit val booleanParser: StringParser[Boolean] {type FmtType = BooleanFormatter} =
    new StringParser[Boolean] {
      override type FmtType = BooleanFormatter
      override def parse(str: String): Boolean = BooleanFormatter.default.parse(str)
      override def parse(str: String, fmt: FmtType): Boolean = fmt.parse(str)
    }

  private def parseNumber(str: String, fmt: NumberFormat): Number = {
    val pos = new ParsePosition(0)
    val num = fmt.parse(str, pos)
    if(pos.getIndex != str.length) throw new ParseException(s"Cannot parse $str as number", pos.getIndex)
    num
  }
}

class BooleanFormatter(tt: String, ft: String, locale: Locale) {

  val trueTerm: String = tt.toLowerCase(locale)
  val falseTerm: String = ft.toLowerCase(locale)

  def this(trueTerm: String, falseTerm: String) = this(trueTerm, falseTerm, Locale.getDefault())

  def format(value: Boolean): String = if(value) trueTerm else falseTerm
  def parse(string: String): Boolean = string.strip().toLowerCase(locale) match {
    case `trueTerm` => true
    case `falseTerm` => false
    case _ => throw new ParseException(s"Cannot parse $string as boolean", 0)
  }
}

object BooleanFormatter {
  def apply(tt: String, ft: String, locale: Locale): BooleanFormatter = new BooleanFormatter(tt, ft, locale)
  def apply(tt: String, ft: String): BooleanFormatter = new BooleanFormatter(tt, ft)
  val default: BooleanFormatter = apply(true.toString, false.toString)
}
