/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import java.io.IOException
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.LongAdder

import scala.concurrent.ExecutionContext
import scala.io.{BufferedSource, Source}
import cats.effect.{ContextShift, IO}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import info.fingo.spata.CSVParser.CSVCallback
import info.fingo.spata.error.StructureException
import info.fingo.spata.io.reader
import info.fingo.spata.text.StringParser

class CSVParserTS extends AnyFunSuite with TableDrivenPropertyChecks {
  type StreamErrorHandler = Throwable => Stream[IO, Unit]
  type IOErrorHandler = Throwable => IO[Unit]

  private val separators = Table("separator", ',', ';', '\t')
  private val maxFieldSize = 100
  // use smaller chunk size in tests than the default one to avoid having all data in single chunk
  private val chunkSize = 16

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  test("parser should process basic csv data") {
    forAll(basicCases) {
      (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
        forAll(separators) { separator =>
          val content = basicContent(testCase, separator)
          val header = basicHeader(separator)
          val csv = s"$header\n$content"
          val parser = CSVParser.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get[IO]()
          val stream = csvStream(csv).through(parser.parse)
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

  test("parser should process basic csv data without header") {
    forAll(basicCases) {
      (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
        forAll(separators) { separator =>
          val csv = basicContent(testCase, separator)
          val parser = CSVParser.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).noHeader().get[IO]()
          val stream = csvStream(csv).through(parser.parse)
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

  test("parser should process empty csv data without header record") {
    forAll(emptyCases) { (_: String, content: String) =>
      val parser = CSVParser.config.noHeader().get[IO]()
      val stream = csvStream(content).through(parser.parse)
      val list = stream.compile.toList.unsafeRunSync()
      assert(list.isEmpty)
    }
  }

  test("parser should clearly report errors in source data while handling them") {
    forAll(errorCases) {
      (
        testCase: String,
        errorCode: String,
        line: Int,
        row: Int,
        col: Option[Int],
        field: Option[String]
      ) =>
        forAll(separators) { separator =>
          val parser = CSVParser.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get[IO]()
          val input = reader[IO](chunkSize).read(generateErroneousCSV(testCase, separator))
          val stream = input.through(parser.parse)
          val eh: StreamErrorHandler =
            ex => assertError(ex, errorCode, line, col, row, field)(Stream.emit(()))
          stream.handleErrorWith(eh).compile.drain.unsafeRunSync()
        }
    }
  }

  test("parser errors should manifest themselves as exceptions when there is no error handler provided") {
    forAll(errorCases) {
      (
        testCase: String,
        errorCode: String,
        line: Int,
        row: Int,
        col: Option[Int],
        field: Option[String]
      ) =>
        forAll(separators) { separator =>
          val parser = CSVParser.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get[IO]()
          val input = reader[IO](chunkSize).read(generateErroneousCSV(testCase, separator))
          val stream = input.through(parser.parse)
          val ex = intercept[Exception] {
            stream.compile.drain.unsafeRunSync()
          }
          assertError(ex, errorCode, line, col, row, field)(())
        }
    }
  }

  test("parser should allow fetching records as list") {
    forAll(basicCases) {
      (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
        forAll(separators) { separator =>
          val parser = CSVParser.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get[IO]()
          val input = csvStream(basicCSV(testCase, separator))
          val list = parser.get(input).unsafeRunSync()
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

  test("parser should allow fetching limited number of records as list") {
    forAll(basicCases) { (testCase: String, firstName: String, firstValue: String, _: String, _: String) =>
      forAll(separators) { separator =>
        val parser = CSVParser.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get[IO]()
        val source = Source.fromString(basicCSV(testCase, separator))
        val input = reader[IO](chunkSize).read(source)
        val list = parser.get(input, 2).unsafeRunSync()
        assert(list.size == 2)
        assertListFirst(list, firstName, firstValue)
        assert(source.hasNext)
      }
    }
  }

  test("parser should provide access to csv data through callback function") {
    forAll(basicCases) {
      (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
        forAll(separators) { separator =>
          val parser = CSVParser.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get[IO]()
          val input = csvStream(basicCSV(testCase, separator))
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
          parser.process(input)(cb).unsafeRunSync()
          assert(count == 3)
        }
    }
  }

  test("parser errors may be handled by IO error handler") {
    forAll(errorCases) {
      (
        testCase: String,
        errorCode: String,
        line: Int,
        row: Int,
        col: Option[Int],
        field: Option[String]
      ) =>
        forAll(separators) { separator =>
          val parser = CSVParser.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get[IO]()
          val source = generateErroneousCSV(testCase, separator)
          val input = reader[IO](chunkSize).read(source)
          val eh: IOErrorHandler = ex => assertError(ex, errorCode, line, col, row, field)(IO.unit)
          parser.process(input)(_ => true).handleErrorWith(eh).unsafeRunSync()
        }
    }
  }

  test("parser should consume only required part of stream depending on callback return value") {
    val cb: CSVCallback = row => if (row(0).startsWith("2")) false else true
    forAll(basicCases) { (testCase: String, _: String, _: String, _: String, _: String) =>
      forAll(separators) { separator =>
        val parser = CSVParser.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get[IO]()
        val source = Source.fromString(basicCSV(testCase, separator))
        val input = reader[IO](chunkSize).read(source)
        parser.process(input)(cb).unsafeRunSync()
        assert(source.hasNext)
      }
    }
  }

  test("parser should provide access to csv data through async callback function") {
    forAll(basicCases) {
      (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
        forAll(separators) { separator =>
          val parser = CSVParser.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get[IO]()
          val input = csvStream(basicCSV(testCase, separator))
          val count = new LongAdder()
          val cb: CSVCallback = row => {
            count.increment()
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
          parser.async.process(input, 2)(cb).unsafeRunSync()
          assert(count.intValue() == 3)
        }
    }
  }

  test("parser errors may be examined by callback passed to final impure method") {
    forAll(errorCases) {
      (
        testCase: String,
        errorCode: String,
        line: Int,
        row: Int,
        col: Option[Int],
        field: Option[String]
      ) =>
        forAll(separators) { separator =>
          val parser = CSVParser.config.fieldDelimiter(separator).fieldSizeLimit(maxFieldSize).get[IO]()
          val source = generateErroneousCSV(testCase, separator)
          val input = reader[IO](chunkSize).read(source)
          val cdl = new CountDownLatch(1)
          var result: Either[Throwable, Unit] = Right(())
          val rcb: Either[Throwable, Unit] => Unit = r => {
            result = r
            cdl.countDown()
          }
          parser.async.process(input)(_ => true).unsafeRunAsync(rcb)
          cdl.await(100, TimeUnit.MILLISECONDS)
          assert(cdl.getCount == 0)
          result match {
            case Right(_) =>
              fail()
            case Left(ex) =>
              assertError(ex, errorCode, line, col, row, field)(())
          }
        }
    }
  }

  private def csvStream(csvString: String) = reader[IO](chunkSize).read(Source.fromString(csvString))

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

  private def assertError[A](
    ex: Throwable,
    errorCode: String,
    line: Int,
    col: Option[Int],
    row: Int,
    field: Option[String]
  )(result: A): A = {
    ex match {
      case ex: StructureException => assertCSVException(ex, errorCode, line, col, row, field)
      case ex: IOException => assertIOException(ex, errorCode)
      case _ => fail()
    }
    result
  }

  private def assertCSVException(
    ex: StructureException,
    errorCode: String,
    line: Int,
    col: Option[Int],
    row: Int,
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
    ("basic", "Funky Koval", "100.00", "Han Solo", "999.99"),
    ("basic quoted", "Koval, Funky", "100.00", "Solo, Han", "999.99"),
    ("mixed", "Funky Koval", "100.00", "Solo, Han", "999.99"),
    ("spaces", " Funky Koval ", " ", "Han Solo", " 999.99 "),
    ("empty values", "", "", "", ""),
    ("double quotes", "\"Funky\" Koval", "\"100.00\"", "Solo, \"Han\"", "999\".\"99"),
    ("line breaks", "Funky\nKoval", "100.00", "\nHan Solo", "999.99\n"),
    ("empty lines", "Funky Koval", "100.00", "Han Solo", "999.99")
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
        s"""1${s}Funky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "basic quoted" =>
        s""""1"$s"Koval, Funky"$s"01.01.2001"$s"100.00"
           |"2"$s"Solo, Eva"$s"31.12.2012"$s"123.45"
           |"3"$s"Solo, Han"$s"09.09.1999"$s"999.99"""".stripMargin
      case "mixed" =>
        s""""1"${s}Funky Koval${s}01.01.2001$s"100.00"
           |"2"$s"Solo, Eva"$s"31.12.2012"$s"123.45"
           |"3"$s"Solo, Han"${s}09.09.1999${s}999.99""".stripMargin
      case "spaces" =>
        s""""1"$s" Funky Koval "${s}01.01.2001$s" "
           | 2$s Eva Solo ${s}31.12.2012${s}123.45
           |"3"$s  Han Solo  ${s}09.09.1999$s" 999.99 """".stripMargin
      case "empty values" =>
        s""""1"$s${s}01.01.2001$s
           |"2"${s}Eva Solo${s}31.12.2012${s}123.45
           |"3"$s""${s}09.09.1999$s""""".stripMargin
      case "double quotes" =>
        s""""1"$s"^Funky^ Koval"$s"01.01.2001"$s"^100.00^"
           |"2"$s"Solo, Eva"$s"31.12.2012"$s"123.45"
           |"3"$s"Solo, ^Han^"$s"09.09.1999"$s"999^.^99"""".stripMargin.replace("^", "\"\"")
      case "line breaks" =>
        s"""1$s"Funky\nKoval"${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3$s"\nHan Solo"${s}09.09.1999$s"999.99\n"""".stripMargin
      case "empty lines" =>
        s"""
           |1${s}Funky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99
           |""".stripMargin
      case _ => throw new RuntimeException("Unknown test case")
    }
  }

  private lazy val errorCases = Table(
    ("testCase", "errorCode", "lineNum", "colNum", "rowNum", "field"),
    ("missing value", "wrongNumberOfFields", 2, 1, None, None),
    ("missing value with empty lines", "wrongNumberOfFields", 3, 1, None, None),
    ("too many values", "wrongNumberOfFields", 2, 1, None, None),
    ("unclosed quotation", "unclosedQuotation", 2, 1, Some(8), Some("NAME")),
    ("unclosed quotation with empty lines", "unclosedQuotation", 3, 1, Some(8), Some("NAME")),
    ("unescaped quotation", "unescapedQuotation", 2, 1, Some(9), Some("NAME")),
    ("unmatched quotation", "unmatchedQuotation", 2, 1, Some(3), Some("NAME")),
    ("unmatched quotation with trailing spaces", "unmatchedQuotation", 2, 1, Some(5), Some("NAME")),
    ("unmatched quotation with escaped one", "unmatchedQuotation", 2, 1, Some(3), Some("NAME")),
    ("field too long", "fieldTooLong", 2, 1, Some(103), Some("NAME")),
    ("field too long through unmatched quotation", "fieldTooLong", 3, 1, Some(11), Some("NAME")),
    ("malformed header", "unclosedQuotation", 1, 0, Some(10), None),
    ("no content", "missingHeader", 1, 0, None, None),
    ("io exception", "message", 0, 0, None, None)
  )

  private def generateErroneousCSV(testCase: String, separator: Char): Source = {
    val s = separator
    val csv = testCase match {
      case "missing value" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1${s}Funky Koval${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "missing value with empty lines" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |
           |1${s}Funky Koval${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "too many values" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1${s}Funky Koval${s}XYZ${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unclosed quotation" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1${s}Funky" Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unclosed quotation with empty lines" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |
           |1${s}Funky" Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unescaped quotation" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1$s"Funky" Koval"${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unmatched quotation" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1$s"Funky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unmatched quotation with trailing spaces" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1$s  "Funky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unmatched quotation with escaped one" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1$s"Funky Koval""${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "field too long" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1${s}Funky Koval Funky Koval Funky Koval Funky Koval Funky Koval Funky Koval Funky Koval Funky Koval Funky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "field too long through unmatched quotation" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1$s"Funky Koval Funky Koval Funky Koval Funky Koval Funky Koval Funky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "malformed header" =>
        s"""ID${s}NAME${s}D"ATE${s}VALUE
           |1${s}Funky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "no content" => ""
      case "io exception" => "io.exception"
      case _ => throw new RuntimeException("Unknown test case")
    }
    if (csv == "io.exception") new ExceptionSource else Source.fromString(csv)
  }

  private class ExceptionSource extends BufferedSource(() => throw new IOException("message"))

  private lazy val emptyCases = Table(
    ("testCase", "content"),
    ("empty", ""),
    ("space", " "),
    ("spaces", "   "),
    ("new line", "\n"),
    ("new lines", "\n\n\n"),
    ("new lines and spaces", " \n \n \n"),
    ("mixed new lines and spaces", "  \n \n\n   \n ")
  )
}
