package info.fingo.spata

import java.io.IOException
import scala.io.{BufferedSource, Source}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import cats.effect.IO
import fs2.Stream
import info.fingo.spata.CSVReader.{CSVCallback, CSVErrHandler, IOErrHandler}

class CSVReaderTS extends AnyFunSuite with TableDrivenPropertyChecks {
  type ErrorHandler = Throwable => Stream[IO, Unit]

  private val separators = Table("separator", ',', ';', '\t')
  private val maxFieldSize = Some(100)

  test("Reader should process basic csv data") {
    forAll(basicCases) {
      (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
        forAll(separators) { separator =>
          val reader = new CSVReader(separator, maxFieldSize)
          val stream = Stream
            .bracket(IO { generateBasicCSV(testCase, separator) })(source => IO { source.close() })
            .flatMap(reader.parse)
          val list = stream.compile.toList.unsafeRunSync()
          assert(list.size == 3)
          assertListFirst(list, firstName, firstValue)
          assertListLast(list, lastName, lastValue)
        }
    }
  }

  test("Reader should clearly report errors in source data while handling errors") {
    forAll(errorCases) {
      (
        testCase: String,
        errorCode: String,
        line: Option[Int],
        col: Option[Int],
        row: Option[Int],
        field: Option[String]
      ) =>
        forAll(separators) { separator =>
          val reader = new CSVReader(separator, maxFieldSize)
          val stream = Stream
            .bracket(IO { generateErroneousCSV(testCase, separator) })(source => IO { source.close() })
            .flatMap(reader.parse)
          val eh: ErrorHandler = ex => assertError(ex, errorCode, line, col, row, field)
          stream.handleErrorWith(eh).compile.drain.unsafeRunSync()
        }
    }
  }

  test("Reader should throw exception on errors when there is no error handler for stream provided") {
    forAll(errorCases) {
      (
        testCase: String,
        errorCode: String,
        line: Option[Int],
        col: Option[Int],
        row: Option[Int],
        field: Option[String]
      ) =>
        forAll(separators) { separator =>
          val reader = new CSVReader(separator, maxFieldSize)
          val stream = Stream
            .bracket(IO { generateErroneousCSV(testCase, separator) })(source => IO { source.close() })
            .flatMap(reader.parse)
          val ex = intercept[Exception] {
            stream.compile.drain.unsafeRunSync()
          }
          assertError(ex, errorCode, line, col, row, field)
        }
    }
  }

  test("Reader should allow loading records as list") {
    forAll(basicCases) {
      (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
        forAll(separators) { separator =>
          val reader = new CSVReader(separator, maxFieldSize)
          val source = generateBasicCSV(testCase, separator)
          val list = reader.load(source)
          assert(list.size == 3)
          assertListFirst(list, firstName, firstValue)
          assertListLast(list, lastName, lastValue)
        }
    }
  }

  test("Reader should allow loading limited number of records as list") {
    forAll(basicCases) { (testCase: String, firstName: String, firstValue: String, _: String, _: String) =>
      forAll(separators) { separator =>
        val reader = new CSVReader(separator, maxFieldSize)
        val source = generateBasicCSV(testCase, separator)
        val list = reader.load(source, 2)
        assert(list.size == 2)
        assertListFirst(list, firstName, firstValue)
        assert(source.hasNext)
      }
    }
  }

  test("Reader should provide access to csv data through callback function") {
    forAll(basicCases) {
      (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
        forAll(separators) { separator =>
          val reader = new CSVReader(separator, maxFieldSize)
          val source = generateBasicCSV(testCase, separator)
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

  test("Reader should support dedicated error handlers while processing callbacks") {
    forAll(errorCases) {
      (
        testCase: String,
        errorCode: String,
        line: Option[Int],
        col: Option[Int],
        row: Option[Int],
        field: Option[String]
      ) =>
        forAll(separators) { separator =>
          val reader = new CSVReader(separator, maxFieldSize)
          val source = generateErroneousCSV(testCase, separator)
          val ehCSV: CSVErrHandler = ex => assertCSVException(ex, errorCode, line, col, row, field)
          val ehIO: IOErrHandler = ex => assertIOException(ex, errorCode)
          reader.process(source, _ => true, ehCSV, ehIO)
        }
    }
  }

  test("Reader should throw exception on errors when there is no error handler for callback provided") {
    forAll(errorCases) {
      (
        testCase: String,
        errorCode: String,
        line: Option[Int],
        col: Option[Int],
        row: Option[Int],
        field: Option[String]
      ) =>
        forAll(separators) { separator =>
          val reader = new CSVReader(separator, maxFieldSize)
          val source = generateErroneousCSV(testCase, separator)
          val ex = intercept[Exception] {
            reader.process(source, _ => true)
          }
          assertError(ex, errorCode, line, col, row, field)
        }
    }
  }

  test("Reader should consume only required part of stream depending on callback return value") {
    val cb: CSVCallback = row => if (row.row(0).startsWith("2")) false else true
    forAll(basicCases) { (testCase: String, _: String, _: String, _: String, _: String) =>
      forAll(separators) { separator =>
        val reader = new CSVReader(separator, maxFieldSize)
        val source = generateBasicCSV(testCase, separator)
        reader.process(source, cb)
        assert(source.hasNext)
      }
    }
  }

  private def assertListFirst(list: List[CSVRecord], firstName: String, firstValue: String): Unit = {
    val first = list.head
    assert(first.getString("NAME") == firstName)
    assert(first.getString("VALUE") == firstValue)
    ()
  }

  private def assertListLast(list: List[CSVRecord], lastName: String, lastValue: String): Unit = {
    val last = list.last
    assert(last.getString("NAME") == lastName)
    assert(last.getString("VALUE") == lastValue)
    ()
  }

  private def assertError(
    ex: Throwable,
    errorCode: String,
    line: Option[Int],
    col: Option[Int],
    row: Option[Int],
    field: Option[String]
  ): Stream[IO, Unit] = {
    ex match {
      case ex: CSVException => assertCSVException(ex, errorCode, line, col, row, field)
      case ex: IOException => assertIOException(ex, errorCode)
      case _ => fail()
    }
    Stream.emit[IO, Unit](())
  }

  private def assertCSVException(
    ex: CSVException,
    errorCode: String,
    line: Option[Int],
    col: Option[Int],
    row: Option[Int],
    field: Option[String]
  ): Unit = {
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

  private lazy val basicCases = Table(
    ("testCase", "firstName", "firstValue", "lastName", "lastValue"),
    ("basic", "Fanky Koval", "100.00", "Han Solo", "999.99"),
    ("basic quoted", "Koval, Fanky", "100.00", "Solo, Han", "999.99"),
    ("mixed", "Fanky Koval", "100.00", "Solo, Han", "999.99"),
    ("spaces", " Fanky Koval ", " ", "Han Solo", " 999.99 "),
    ("empty values", "", "", "", ""),
    ("double quotes", "\"Fanky\" Koval", "\"100.00\"", "Solo, \"Han\"", "999\".\"99"),
    ("line breaks", "Fanky\nKoval", "100.00", "\nHan Solo", "999.99\n"),
    ("empty lines", "Fanky Koval", "100.00", "Han Solo", "999.99")
  )

  private def generateBasicCSV(testCase: String, separator: Char): Source = {
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
           |"3"$s"Solo, ^Han^"$s"09.09.1999"$s"999^.^99"""".stripMargin.replace("^", "\"\"")
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

  private lazy val errorCases = Table(
    ("testCase", "errorCode", "lineNum", "colNum", "rowNum", "field"),
    ("missing value", "fieldsHeaderImbalance", Some(2), None, Some(1), None),
    ("missing value with empty lines", "fieldsHeaderImbalance", Some(3), None, Some(1), None),
    ("too many values", "fieldsHeaderImbalance", Some(2), None, Some(1), None),
    ("unclosed quotation", "unclosedQuotation", Some(2), Some(8), Some(1), Some("NAME")),
    ("unclosed quotation with empty lines", "unclosedQuotation", Some(3), Some(8), Some(1), Some("NAME")),
    ("unescaped quotation", "unescapedQuotation", Some(2), Some(9), Some(1), Some("NAME")),
    ("unmatched quotation", "unmatchedQuotation", Some(2), Some(3), Some(1), Some("NAME")),
    ("unmatched quotation with trailing spaces", "unmatchedQuotation", Some(2), Some(5), Some(1), Some("NAME")),
    ("unmatched quotation with escaped one", "unmatchedQuotation", Some(2), Some(3), Some(1), Some("NAME")),
    ("field too long", "fieldTooLong", Some(2), Some(103), Some(1), Some("NAME")),
    ("field too long through unmatched quotation", "fieldTooLong", Some(3), Some(11), Some(1), Some("NAME")),
    ("malformed header", "unclosedQuotation", Some(1), Some(10), Some(0), None),
    ("no content", "missingHeader", Some(1), None, Some(0), None),
    ("io exception", "message", None, None, None, None)
  )

  private def generateErroneousCSV(testCase: String, separator: Char): Source = {
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
    if (csv == "io.exception") new ExceptionSource else Source.fromString(csv)
  }

  private class ExceptionSource extends BufferedSource(() => throw new IOException("message"))
}
