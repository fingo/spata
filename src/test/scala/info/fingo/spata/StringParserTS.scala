package info.fingo.spata

import java.time.LocalDate

import org.scalatest.FunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class StringParserTS extends FunSuite with TableDrivenPropertyChecks {

  import StringParser._

  test("StringParser should parse correctly strings") {
    forAll(strings) { (_: String, str: String, string: Option[String]) =>
      assert(parse[String](str) == string)
    }
  }

  test("StringParser should parse correctly big decimals") {
    forAll(bigDecimals) { (_: String, str: String, decimal: Option[BigDecimal]) =>
      assert(parse[BigDecimal](str) == decimal)
    }
  }

  test("StringParser should parse correctly local dates") {
    forAll(localDates) { (_: String, str: String, date: Option[LocalDate]) =>
      assert(parse[LocalDate](str) == date)
    }
  }

  val strings = Table(
    ("testCase","str","string"),
    ("basic", "lorem ipsum", Some("lorem ipsum")),
    ("empty", "", None)
  )

  val bigDecimals = Table(
    ("testCase","str","decimal"),
    ("basic", "123.45", Some(BigDecimal(123.45))),
    ("empty", "", None)
  )

  val localDates = Table(
    ("testCase","str","date"),
    ("basic", "2020-02-02", Some(LocalDate.of(2020,2,2))),
    ("empty", "", None)
  )
}
