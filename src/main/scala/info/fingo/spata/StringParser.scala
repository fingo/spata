package info.fingo.spata

import java.text.DecimalFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.util.Try

trait StringParser[A] {
  type FmtType
  def parse(str: String): Option[A]
  def parse(str: String, fmt: FmtType): Option [A]
}

object StringParser {
  type Aux[A,B] = StringParser[A] {type FmtType = B}

  def parse[A](str: String)(implicit parser: StringParser[A]): Option[A] = parser.parse(str)
  def parse[A,B](str: String, fmt: B)(implicit parser: Aux[A,B]): Option[A] = parser.parse(str, fmt)

  implicit val stringParser: StringParser[String] = new StringParser[String] {
    override def parse(str: String): Option[String] = if(str.isEmpty) None else Some(str)
    override def parse(str: String, fmt: FmtType): Option[String] = parse(str)
  }

  implicit val bigDecimalParser: StringParser[BigDecimal] {type FmtType = DecimalFormat} =
    new StringParser[BigDecimal] {
      override type FmtType = DecimalFormat
      override def parse(str: String): Option[BigDecimal] = Try(BigDecimal(str)).toOption
      def parse(str: String, fmt: FmtType): Option[BigDecimal] = {
        fmt.setParseBigDecimal(true)
        Try(BigDecimal(fmt.parse(str).asInstanceOf[java.math.BigDecimal])).toOption
      }
  }

  implicit val localDateParser: StringParser[LocalDate] {type FmtType = DateTimeFormatter} =
    new StringParser[LocalDate] {
      type FmtType = DateTimeFormatter
      override def parse(str: String): Option[LocalDate] = Try(LocalDate.parse(str)).toOption
      def parse(str: String, fmt: FmtType): Option[LocalDate] = Try(LocalDate.parse(str, fmt)).toOption
    }
}
