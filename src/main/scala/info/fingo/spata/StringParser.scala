package info.fingo.spata

import java.text.{DecimalFormat, NumberFormat, ParseException}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter
import java.util.Locale

import scala.util.Try

trait SimpleStringParser[A] {
  def parse(str: String): Option[A]
}

trait StringParser[A] extends SimpleStringParser[A] {
  type FmtType
  def parse(str: String, fmt: FmtType): Option [A]
}

object StringParser {
  type Aux[A,B] = StringParser[A] {type FmtType = B}

  def parse[A](str: String)(implicit parser: SimpleStringParser[A]): Option[A] = parser.parse(str)
  def parse[A,B](str: String, fmt: B)(implicit parser: Aux[A,B]): Option[A] = parser.parse(str, fmt)

  implicit val stringParser: SimpleStringParser[String] = (str: String) => if(str.isEmpty) None else Some(str)

  implicit val intParser: SimpleStringParser[Int] = (str: String) => Try(str.toInt).toOption

  implicit val longParser: StringParser[Long] {type FmtType = NumberFormat} =
    new StringParser[Long] {
      override type FmtType = NumberFormat
      override def parse(str: String): Option[Long] = Try(str.toLong).toOption
      override def parse(str: String, fmt: FmtType): Option[Long] = Try(fmt.parse(str).longValue()).toOption
    }

  implicit val doubleParser: StringParser[Double] {type FmtType = DecimalFormat} =
    new StringParser[Double] {
      override type FmtType = DecimalFormat
      override def parse(str: String): Option[Double] = Try(str.toDouble).toOption
      override def parse(str: String, fmt: FmtType): Option[Double] = Try(fmt.parse(str).doubleValue()).toOption
    }

  implicit val bigDecimalParser: StringParser[BigDecimal] {type FmtType = DecimalFormat} =
    new StringParser[BigDecimal] {
      override type FmtType = DecimalFormat
      override def parse(str: String): Option[BigDecimal] = Try(BigDecimal(str)).toOption
      override def parse(str: String, fmt: FmtType): Option[BigDecimal] = {
        fmt.setParseBigDecimal(true)
        Try(BigDecimal(fmt.parse(str).asInstanceOf[java.math.BigDecimal])).toOption
      }
  }

  implicit val localDateParser: StringParser[LocalDate] {type FmtType = DateTimeFormatter} =
    new StringParser[LocalDate] {
      override type FmtType = DateTimeFormatter
      override def parse(str: String): Option[LocalDate] = Try(LocalDate.parse(str)).toOption
      override def parse(str: String, fmt: FmtType): Option[LocalDate] = Try(LocalDate.parse(str, fmt)).toOption
    }

  implicit val localTimeParser: StringParser[LocalTime] {type FmtType = DateTimeFormatter} =
    new StringParser[LocalTime] {
      override type FmtType = DateTimeFormatter
      override def parse(str: String): Option[LocalTime] = Try(LocalTime.parse(str)).toOption
      override def parse(str: String, fmt: FmtType): Option[LocalTime] = Try(LocalTime.parse(str, fmt)).toOption
    }

  implicit val localDateTimeParser: StringParser[LocalDateTime] {type FmtType = DateTimeFormatter} =
    new StringParser[LocalDateTime] {
      override type FmtType = DateTimeFormatter
      override def parse(str: String): Option[LocalDateTime] = Try(LocalDateTime.parse(str)).toOption
      override def parse(str: String, fmt: FmtType): Option[LocalDateTime] = Try(LocalDateTime.parse(str, fmt)).toOption
    }

  implicit val booleanParser: StringParser[Boolean] {type FmtType = BooleanFormatter} =
    new StringParser[Boolean] {
      override type FmtType = BooleanFormatter
      override def parse(str: String): Option[Boolean] = Try(BooleanFormatter.default.parse(str)).toOption
      override def parse(str: String, fmt: FmtType): Option[Boolean] = Try(fmt.parse(str)).toOption
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
