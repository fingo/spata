package info.fingo.spata

import java.io.IOException
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.io.{BufferedSource, Source}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import cats.effect.IO
import fs2.Stream
import info.fingo.spata.CSVReader.CSVCallback
import info.fingo.spata.text.StringParser

class CSVReaderTS extends AnyFunSuite with TableDrivenPropertyChecks {
  type ErrorHandler = Throwable => Stream[IO, Unit]

  private val separators = Table("separator", ',', ';', '\t')
  private val maxFieldSize = 100

  test("Reader should process basic csv data") {
    forAll(basicCases) {
      (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
        forAll(separators) { separator =>
          val content = basicContent(testCase, separator)
          val header = basicHeader(separator)
          val csv = s"$header\n$content"
          val reader = CSVReader.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get
          val stream = Stream
            .bracket(IO { Source.fromString(csv) })(source => IO { source.close() })
            .through(reader.pipe)
          val list = stream.compile.toList.unsafeRunSync()
          assert(list.size == 3)
          val head = list.head
          val last = list.last
          assertElement(head, firstName, firstValue)
          assertElement(last, lastName, lastValue)
          assert(head.size == last.size)
          assert(head.lineNum == 1 + csv.takeWhile(_ != '.').count(_ == '\n')) // line breaks are placed before first dot
          assert(head.rowNum == 1)
          assert(last.lineNum == 1 + csv.stripTrailing().count(_ == '\n'))
          assert(last.rowNum == list.size)

          case class Data(NAME: String, DATE: LocalDate)
          implicit val ldsp: StringParser[LocalDate] =
            (str: String) => LocalDate.parse(str.strip, DateTimeFormatter.ofPattern("dd.MM.yyyy"))
          val hmd = head.to[Data]()
          assert(hmd.isRight)
          assert(hmd.forall(_.NAME == firstName))
          val lmd = last.to[Data]()
          assert(lmd.isRight)
          assert(lmd.forall(_.NAME == lastName))
        }
    }
  }

  test("Reader should process basic csv data without header") {
    forAll(basicCases) {
      (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
        forAll(separators) { separator =>
          val csv = basicContent(testCase, separator)
          val reader = CSVReader.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).noHeader().get
          val stream = Stream
            .bracket(IO { Source.fromString(csv) })(source => IO { source.close() })
            .through(reader.pipe)
          val list = stream.compile.toList.unsafeRunSync()
          assert(list.size == 3)
          val head = list.head
          val last = list.last
          assert(head("_2") == firstName)
          assert(head("_4") == firstValue)
          assert(last("_2") == lastName)
          assert(last("_4") == lastValue)
          assert(head.size == last.size)
          assert(head.lineNum == 1 + csv.takeWhile(_ != '.').count(_ == '\n')) // line breaks are placed before first dot
          assert(head.rowNum == 1)
          assert(last.lineNum == 1 + csv.stripTrailing().count(_ == '\n'))
          assert(last.rowNum == list.size)

          type Data = (Int, String)
          val hmd = head.to[Data]()
          assert(hmd.isRight)
          assert(hmd.forall(_._2 == firstName))
          val lmd = last.to[Data]()
          assert(lmd.isRight)
          assert(lmd.forall(_._2 == lastName))
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
          val reader = CSVReader.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get
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
          val reader = CSVReader.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get
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
          val reader = CSVReader.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get
          val source = Source.fromString(basicCSV(testCase, separator))
          val list = reader.load(source)
          assert(list.size == 3)
          assertListFirst(list, firstName, firstValue)
          assertListLast(list, lastName, lastValue)

          case class Data(ID: Int, NAME: String)
          val data = list.map(_.to[Data]()).collect { case Right(v) => v }
          assert(data.length == 3)
          assert(data.head.NAME == firstName)
        }
    }
  }

  test("Reader should allow loading limited number of records as list") {
    forAll(basicCases) { (testCase: String, firstName: String, firstValue: String, _: String, _: String) =>
      forAll(separators) { separator =>
        val reader = CSVReader.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get
        val source = Source.fromString(basicCSV(testCase, separator))
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
          val reader = CSVReader.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get
          val source = Source.fromString(basicCSV(testCase, separator))
          var count = 0
          val cb: CSVCallback = row => {
            count += 1
            row.rowNum match {
              case 1 =>
                assert(row("NAME") == firstName)
                assert(row("VALUE") == firstValue)
                true
              case 3 =>
                assert(row("NAME") == lastName)
                assert(row("VALUE") == lastValue)
                true
              case _ => true
            }
          }
          reader.process(source)(cb)
          assert(count == 3)
        }
    }
  }

  test("Reader should throw exception on errors") {
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
          val reader = CSVReader.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get
          val source = generateErroneousCSV(testCase, separator)
          val ex = intercept[Exception] {
            reader.process(source)(_ => true)
          }
          assertError(ex, errorCode, line, col, row, field)
        }
    }
  }

  test("Reader should consume only required part of stream depending on callback return value") {
    val cb: CSVCallback = row => if (row(0).startsWith("2")) false else true
    forAll(basicCases) { (testCase: String, _: String, _: String, _: String, _: String) =>
      forAll(separators) { separator =>
        val reader = CSVReader.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get
        val source = Source.fromString(basicCSV(testCase, separator))
        reader.process(source)(cb)
        assert(source.hasNext)
      }
    }
  }

  private def assertListFirst(list: List[CSVRecord], firstName: String, firstValue: String): Unit = {
    val first = list.head
    assert(first.rowNum == 1)
    assertElement(first, firstName, firstValue)
  }

  private def assertListLast(list: List[CSVRecord], lastName: String, lastValue: String): Unit = {
    val last = list.last
    assert(last.rowNum == list.size)
    assertElement(last, lastName, lastValue)
  }

  private def assertElement(elem: CSVRecord, name: String, value: String): Unit = {
    assert(elem("NAME") == name)
    assert(elem("VALUE") == value)
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

  private def basicCSV(testCase: String, separator: Char): String = {
    val header = basicHeader(separator)
    val content = basicContent(testCase, separator)
    s"$header\n$content"
  }

  private def basicHeader(separator: Char): String = {
    val s = separator
    s"""ID${s}NAME${s}DATE${s}VALUE"""
  }

  private def basicContent(testCase: String, separator: Char): String = {
    val s = separator
    testCase match {
      case "basic" =>
        s"""1${s}Fanky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "basic quoted" =>
        s""""1"$s"Koval, Fanky"$s"01.01.2001"$s"100.00"
           |"2"$s"Solo, Eva"$s"31.12.2012"$s"123.45"
           |"3"$s"Solo, Han"$s"09.09.1999"$s"999.99"""".stripMargin
      case "mixed" =>
        s""""1"${s}Fanky Koval${s}01.01.2001$s"100.00"
           |"2"$s"Solo, Eva"$s"31.12.2012"$s"123.45"
           |"3"$s"Solo, Han"${s}09.09.1999${s}999.99""".stripMargin
      case "spaces" =>
        s""""1"$s" Fanky Koval "${s}01.01.2001$s" "
           |"2"$s Eva Solo ${s}31.12.2012${s}123.45
           |"3"$s  Han Solo  ${s}09.09.1999$s" 999.99 """".stripMargin
      case "empty values" =>
        s""""1"$s${s}01.01.2001$s
           |"2"${s}Eva Solo${s}31.12.2012${s}123.45
           |"3"$s""${s}09.09.1999$s""""".stripMargin
      case "double quotes" =>
        s""""1"$s"^Fanky^ Koval"$s"01.01.2001"$s"^100.00^"
           |"2"$s"Solo, Eva"$s"31.12.2012"$s"123.45"
           |"3"$s"Solo, ^Han^"$s"09.09.1999"$s"999^.^99"""".stripMargin.replace("^", "\"\"")
      case "line breaks" =>
        s"""1$s"Fanky\nKoval"${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3$s"\nHan Solo"${s}09.09.1999$s"999.99\n"""".stripMargin
      case "empty lines" =>
        s"""
           |1${s}Fanky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99
           |""".stripMargin
      case _ => throw new RuntimeException("Unknown test case")
    }
  }

  private lazy val errorCases = Table(
    ("testCase", "errorCode", "lineNum", "colNum", "rowNum", "field"),
    ("missing value", "wrongNumberOfFields", Some(2), None, Some(1), None),
    ("missing value with empty lines", "wrongNumberOfFields", Some(3), None, Some(1), None),
    ("too many values", "wrongNumberOfFields", Some(2), None, Some(1), None),
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
