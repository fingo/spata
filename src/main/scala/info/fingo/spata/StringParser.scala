package info.fingo.spata

import java.time.LocalDate

import scala.util.Try

trait StringParser[A] {
  def parse(str: String): Option[A]
}

object StringParser {
  def parse[A](str: String)(implicit parser: StringParser[A]): Option[A] = parser.parse(str)

  implicit val stringParser: StringParser[String] = (str: String) => if(str.isEmpty) None else Some(str)
  implicit val bigDecimalParser: StringParser[BigDecimal] = (str: String) => Try(BigDecimal(str)).toOption
  implicit val localDateParser: StringParser[LocalDate] = (str: String) => Try(LocalDate.parse(str)).toOption
}
