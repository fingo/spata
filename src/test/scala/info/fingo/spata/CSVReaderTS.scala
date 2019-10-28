package info.fingo.spata

import info.fingo.spata.CSVReader.{CSVCallback, CSVErrHandler, IOErrHandler}
import org.scalatest.FunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.io.{BufferedSource, Source}
import java.io.IOException

class CSVReaderTS extends FunSuite with TableDrivenPropertyChecks {

  val separators = Table("separator",',',';','\t')
  val maxFieldSize = Some(100)

  test("Reader should process basic csv data") {
    forAll(basicCases) { (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
      forAll(separators) { separator =>
        val source = generateBasicCSV(testCase,separator)
        val reader = new CSVReader(separator, maxFieldSize)
        var count = 0
        val cb: CSVCallback = row => {
          count += 1
          row.rowNum match {
            case 1 =>
              assert(row.getString("NAME") == firstName)
              assert(row.getString("VALUE") == firstValue)
              true
            case 3 =>
              assert(row.getString("NAME") == lastName)
              assert(row.getString("VALUE") == lastValue)
              true
            case _ => true
          }
        }
        reader.process(source, cb)
        assert(count == 3)
      }
    }
  }

  test("Reader should clearly report errors in source data while handling errors") {
    forAll(errorCases) { (testCase: String, errorCode: String, line: Option[Int], col: Option[Int], row: Option[Int], field: Option[String]) =>
      forAll(separators) { separator =>
        val source = generateErroneousCSV(testCase, separator)
        val reader = new CSVReader(separator, maxFieldSize)
        val ehCSV: CSVErrHandler = ex => assertCSVException(ex, errorCode, line, col, row, field)
        val ehIO: IOErrHandler = ex => assertIOException(ex, errorCode)
        reader.process(source, _ => true, ehCSV, ehIO)
      }
    }
  }

  test("Reader should throw exception on errors when there is no error handler provided") {
    forAll(errorCases) { (testCase: String, errorCode: String, line: Option[Int], col: Option[Int], row: Option[Int], field: Option[String]) =>
      forAll(separators) { separator =>
        val source = generateErroneousCSV(testCase, separator)
        val reader = new CSVReader(separator, maxFieldSize)
        try {
          reader.process(source, _ => true)
        }
        catch {
          case ex: CSVException => assertCSVException(ex, errorCode, line, col, row, field)
          case ex: IOException => assertIOException(ex, errorCode)
          case _: Throwable => fail()
        }
      }
    }
  }

  test("Reader should consume only required part of stream") {
    val cb: CSVCallback = row => if(row.row(0).startsWith("2")) false else true
    forAll(basicCases) { (testCase: String, _: String, _: String, _: String, _: String) =>
      forAll(separators) { separator =>
        val source = generateBasicCSV(testCase, separator)
        val reader = new CSVReader(separator, maxFieldSize)
        reader.process(source, cb)
        assert(source.hasNext)
      }
    }
  }

  test("Reader should allow getting the records as list") {
    forAll(basicCases) { (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
      forAll(separators) { separator =>
        val source = generateBasicCSV(testCase, separator)
        val reader = new CSVReader(separator, maxFieldSize)
        val list = reader.load(source)
        assert(list.size == 3)
        val first = list.head
        assert(first.getString("NAME") == firstName)
        assert(first.getString("VALUE") == firstValue)
        val last = list.last
        assert(last.getString("NAME") == lastName)
        assert(last.getString("VALUE") == lastValue)
      }
    }
  }

  test("Reader should allow getting limited number of records as list") {
    forAll(basicCases) { (testCase: String, firstName: String, firstValue: String, _: String, _: String) =>
      forAll(separators) { separator =>
        val source = generateBasicCSV(testCase, separator)
        val reader = new CSVReader(separator, maxFieldSize)
        val list = reader.load(source, 2)
        assert(list.size == 2)
        val first = list.head
        assert(first.getString("NAME") == firstName)
        assert(first.getString("VALUE") == firstValue)
        assert(source.hasNext)
      }
    }
  }

  private def assertCSVException(ex: CSVException, errorCode: String, line: Option[Int], col: Option[Int], row: Option[Int], field: Option[String]): Unit = {
    assert(ex.messageCode == errorCode)
    assert(ex.line == line)
    assert(ex.row == row)
    assert(ex.col == col)
    assert(ex.field == field)
    ()
  }

  private def assertIOException(ex: Throwable, errorCode: String): Unit = {
    assert(ex.getMessage == errorCode)
    ()
  }

  val basicCases = Table(
    ("testCase","firstName","firstValue","lastName","lastValue"),
    ("basic","Fanky Koval","100.00","Han Solo","999.99"),
    ("basic quoted","Koval, Fanky","100.00","Solo, Han","999.99"),
    ("mixed","Fanky Koval","100.00","Solo, Han","999.99"),
    ("spaces"," Fanky Koval "," ","Han Solo"," 999.99 "),
    ("empty values","","","",""),
    ("double quotes","\"Fanky\" Koval","\"100.00\"","Solo, \"Han\"","999\".\"99"),
    ("line breaks","Fanky\nKoval","100.00","\nHan Solo","999.99\n"),
    ("empty lines","Fanky Koval","100.00","Han Solo","999.99")
  )

  def generateBasicCSV(testCase: String, separator: Char): Source = {
    val s = separator
    val csv = testCase match {
      case "basic" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1${s}Fanky Koval${s}01.01.2001${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "basic quoted" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |"1"$s"Koval, Fanky"$s"01.01.2001"$s"100.00"
            |"2"$s"Solo, Eva"$s"31.12.2012"$s"123.45"
            |"3"$s"Solo, Han"$s"09.09.1999"$s"999.99"""".stripMargin
      case "mixed" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |"1"${s}Fanky Koval${s}01.01.2001$s"100.00"
            |"2"$s"Solo, Eva"$s"31.12.2012"$s"123.45"
            |"3"$s"Solo, Han"${s}09.09.1999${s}999.99""".stripMargin
      case "spaces" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |"1"$s" Fanky Koval "${s}01.01.2001$s" "
            |"2"$s Eva Solo ${s}31.12.2012${s}123.45
            |"3"$s  Han Solo  ${s}09.09.1999$s" 999.99 """".stripMargin
      case "empty values" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |"1"$s${s}01.01.2001$s
            |"2"${s}Eva Solo${s}31.12.2012${s}123.45
            |"3"$s""${s}09.09.1999$s""""".stripMargin
      case "double quotes" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |"1"$s"^Fanky^ Koval"$s"01.01.2001"$s"^100.00^"
            |"2"$s"Solo, Eva"$s"31.12.2012"$s"123.45"
            |"3"$s"Solo, ^Han^"$s"09.09.1999"$s"999^.^99"""".stripMargin.replace("^","\"\"")
      case "line breaks" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1$s"Fanky\nKoval"${s}01.01.2001${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3$s"\nHan Solo"${s}09.09.1999$s"999.99\n"""".stripMargin
      case "empty lines" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |
           |1${s}Fanky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99
           |""".stripMargin
      case _ => throw new RuntimeException("Unknown test case")
    }
    Source.fromString(csv)
  }

  val errorCases = Table(
    ("testCase","errorCode","lineNum","colNum","rowNum","field"),
    ("missing value","fieldsHeaderImbalance",Some(2),None,Some(1),None),
    ("missing value with empty lines","fieldsHeaderImbalance",Some(3),None,Some(1),None),
    ("too many values","fieldsHeaderImbalance",Some(2),None,Some(1),None),
    ("unclosed quotation","unclosedQuotation",Some(2),Some(8),Some(1),Some("NAME")),
    ("unclosed quotation with empty lines","unclosedQuotation",Some(3),Some(8),Some(1),Some("NAME")),
    ("unescaped quotation","unescapedQuotation",Some(2),Some(9),Some(1),Some("NAME")),
    ("unmatched quotation","unmatchedQuotation",Some(2),Some(3),Some(1),Some("NAME")),
    ("unmatched quotation with trailing spaces","unmatchedQuotation",Some(2),Some(5),Some(1),Some("NAME")),
    ("unmatched quotation with escaped one","unmatchedQuotation",Some(2),Some(3),Some(1),Some("NAME")),
    ("field too long","fieldTooLong",Some(2),Some(103),Some(1),Some("NAME")),
    ("field too long through unmatched quotation","fieldTooLong",Some(3),Some(11),Some(1),Some("NAME")),
    ("malformed header", "unclosedQuotation", Some(1), Some(10), Some(0), None),
    ("no content", "missingHeader", Some(1), None, Some(0), None),
    ("io exception", "message", None, None, None, None)
  )

  def generateErroneousCSV(testCase: String, separator: Char): Source = {
    val s = separator
    val csv = testCase match {
      case "missing value" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1${s}Fanky Koval${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "missing value with empty lines" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |
           |1${s}Fanky Koval${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "too many values" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1${s}Fanky Koval${s}XYZ${s}01.01.2001${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unclosed quotation" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1${s}Fanky" Koval${s}01.01.2001${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unclosed quotation with empty lines" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |
           |1${s}Fanky" Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unescaped quotation" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1$s"Fanky" Koval"${s}01.01.2001${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unmatched quotation" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1$s"Fanky Koval${s}01.01.2001${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unmatched quotation with trailing spaces" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1$s  "Fanky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unmatched quotation with escaped one" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1$s"Fanky Koval""${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "field too long" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1${s}Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "field too long through unmatched quotation" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1$s"Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "malformed header" =>
        s"""ID${s}NAME${s}D"ATE${s}VALUE
           |1${s}Fanky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "no content" => ""
      case "io exception" => "io.exception"
      case _ => throw new RuntimeException("Unknown test case")
    }
    if(csv == "io.exception") new ExceptionSource else Source.fromString(csv)
  }

  private class ExceptionSource extends BufferedSource(() => throw new IOException("message"))
}
