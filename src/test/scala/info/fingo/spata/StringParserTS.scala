package info.fingo.spata

import java.text.{DecimalFormat, NumberFormat}
import java.time.LocalDate
import java.time.format.{DateTimeFormatter, FormatStyle}
import java.util.Locale

import org.scalatest.FunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class StringParserTS extends FunSuite with TableDrivenPropertyChecks {

  import StringParser._
  val locale = new Locale("pl", "PL")

  test("StringParser should parse correctly strings") {
    forAll(strings) { (_: String, str: String, string: Option[String]) =>
      assert(parse[String](str) == string)
    }
  }

  test("StringParser should parse correctly big decimals") {
    forAll(bigDecimals) { (_: String, str: String, decimal: Option[BigDecimal], fmt: Option[DecimalFormat]) =>
      val result = fmt match {
        case Some(f) => parse(str,f)
        case _ => parse[BigDecimal](str)
      }
      assert(result == decimal)
    }
  }

  test("StringParser should parse correctly local dates") {
    forAll(localDates) { (_: String, str: String, date: Option[LocalDate], fmt: Option[DateTimeFormatter]) =>
      val result = fmt match {
        case Some(f) => parse(str,f)
        case _ => parse[LocalDate](str)
      }
      assert(result == date)
    }
  }

  val strings = Table(
    ("testCase","str","string"),
    ("basic", "lorem ipsum", Some("lorem ipsum")),
    ("empty", "", None)
  )

  val bigDecimals = Table(
    ("testCase","str","decimal","format"),
    ("basic", "123.45", Some(BigDecimal(123.45)), None),
    ("locale", "123,45", Some(BigDecimal(123.45)), Some(NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat])),
    ("empty", "", None, None)
  )

  val localDates = Table(
    ("testCase","str","date","format"),
    ("basic", "2020-02-02", Some(LocalDate.of(2020,2,2)), None),
    ("locale", "02.02.2020", Some(LocalDate.of(2020,2,2)), Some(DateTimeFormatter.ofLocalizedDate(FormatStyle.SHORT).withLocale(locale))),
    ("formatted", "02/02/2020", Some(LocalDate.of(2020,2,2)), Some(DateTimeFormatter.ofPattern("dd/MM/yyyy"))),
    ("empty", "", None, None)
  )
}
