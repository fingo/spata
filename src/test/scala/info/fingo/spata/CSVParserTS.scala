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
import scala.collection.mutable
import cats.effect.{ContextShift, IO}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import info.fingo.spata.CSVParser.Callback
import info.fingo.spata.error.StructureException
import info.fingo.spata.io.Reader
import info.fingo.spata.text.StringParser
import org.scalatest.Assertion

class CSVParserTS extends AnyFunSuite with TableDrivenPropertyChecks {
  type StreamErrorHandler = Throwable => Stream[IO, Unit]
  type IOErrorHandler = Throwable => IO[Unit]

  private val maxFieldSize = 100
  // use smaller chunk size in tests than the default one to avoid having all data in single chunk
  private val chunkSize = 16

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  // wrap required test parameters into class to easier pass it around
  private class BasicTestInfo(testData: (String, String, String, String, String), val separator: Char) {
    val (testCase, firstName, firstValue, lastName, lastValue) = testData
  }

  // wrap basic test structure into a method to reduce code duplication
  private def basicTestWrapper(hasHeader: Boolean)(testCode: (BasicTestInfo, CSVParser[IO]) => Assertion): Assertion =
    forAll(trimmingWs) { (trim: Boolean, ws: String) =>
      forAll(basicCases(ws)) {
        (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
          forAll(separators) { separator =>
            val testInfo = new BasicTestInfo((testCase, firstName, firstValue, lastName, lastValue), separator)
            val basicConfig = CSVConfig().fieldDelimiter(separator).fieldSizeLimit(maxFieldSize)
            val config = if (hasHeader) basicConfig else basicConfig.noHeader
            val parser = if (trim) config.stripSpaces.parser[IO] else config.parser[IO]
            testCode(testInfo, parser)
          }
      }
    }

  test("parser should process basic csv data") {
    basicTestWrapper(hasHeader = true) { (testInfo, parser) =>
      val content = basicContent(testInfo.testCase, testInfo.separator)
      val header = basicHeader(testInfo.separator)
      val csv = s"$header\n$content"
      val stream = csvStream(csv).through(parser.parse)
      val list = stream.compile.toList.unsafeRunSync()
      assert(list.size == 3)
      val head = list.head
      val last = list.last
      assertElement(head, testInfo.firstName, testInfo.firstValue)
      assertElement(last, testInfo.lastName, testInfo.lastValue)
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
      assert(hmd.forall(_.NAME == testInfo.firstName))
      val lmd = last.to[Data]()
      assert(lmd.isRight)
      assert(lmd.forall(_.NAME == testInfo.lastName))
    }
  }

  test("parser should process basic csv data without header") {
    basicTestWrapper(hasHeader = false) { (testInfo, parser) =>
      val csv = basicContent(testInfo.testCase, testInfo.separator)
      val stream = csvStream(csv).through(parser.parse)
      val list = stream.compile.toList.unsafeRunSync()
      assert(list.size == 3)
      val head = list.head
      val last = list.last
      assert(head("_2").contains(testInfo.firstName))
      assert(head("_4").contains(testInfo.firstValue))
      assert(last("_2").contains(testInfo.lastName))
      assert(last("_4").contains(testInfo.lastValue))
      assert(head.size == last.size)
      assert(head.lineNum == 1 + csv.takeWhile(_ != '.').count(_ == '\n')) // line breaks are placed before first dot
      assert(head.rowNum == 1)
      assert(last.lineNum == 1 + csv.stripTrailing().count(_ == '\n'))
      assert(last.rowNum == list.size)

      type Data = (Int, String)
      val hmd = head.to[Data]()
      assert(hmd.isRight)
      assert(hmd.forall { case (_, name) => name == testInfo.firstName })
      val lmd = last.to[Data]()
      assert(lmd.isRight)
      assert(lmd.forall { case (_, name) => name == testInfo.lastName })
    }
  }

  test("parser should process empty csv data without header record") {
    forAll(emptyContentWs) { (trim: Boolean, ws: String) =>
      forAll(emptyCases(ws)) { (_: String, content: String) =>
        val config = CSVParser.config.noHeader
        val parser = if (trim) config.stripSpaces.parser[IO] else config.parser[IO]
        val stream = csvStream(content).through(parser.parse)
        val list = stream.compile.toList.unsafeRunSync()
        assert(list.isEmpty)
      }
    }
  }

  test("parser should clearly report errors in source data while handling them") {
    forAll(trimmings) { trim =>
      forAll(errorCases ++ (if (trim) trimErrorCases else noTrimErrorCases)) {
        (
          testCase: String,
          errorCode: String,
          line: Int,
          row: Int,
          col: Option[Int],
          field: Option[String]
        ) =>
          forAll(separators) { separator =>
            val config = CSVConfig().fieldDelimiter(separator).fieldSizeLimit(maxFieldSize)
            val parser = if (trim) config.stripSpaces.parser[IO] else config.parser[IO]
            val input = Reader[IO](chunkSize).read(generateErroneousCSV(testCase, separator))
            val stream = input.through(parser.parse)
            val eh: StreamErrorHandler =
              ex => assertError(ex, errorCode, line, col, row, field)(Stream.emit(()))
            stream.handleErrorWith(eh).compile.drain.unsafeRunSync()
          }
      }
    }
  }

  test("parser errors should manifest themselves as exceptions when there is no error handler provided") {
    forAll(trimmings) { trim =>
      forAll(errorCases ++ (if (trim) trimErrorCases else noTrimErrorCases)) {
        (
          testCase: String,
          errorCode: String,
          line: Int,
          row: Int,
          col: Option[Int],
          field: Option[String]
        ) =>
          forAll(separators) { separator =>
            val config = CSVConfig().fieldDelimiter(separator).fieldSizeLimit(maxFieldSize)
            val parser = if (trim) config.stripSpaces.parser[IO] else config.parser[IO]
            val input = Reader[IO](chunkSize).read(generateErroneousCSV(testCase, separator))
            val stream = input.through(parser.parse)
            val ex = intercept[Exception] {
              stream.compile.drain.unsafeRunSync()
            }
            assertError(ex, errorCode, line, col, row, field)(())
          }
      }
    }
  }

  test("parser should allow fetching records as list") {
    basicTestWrapper(hasHeader = true) { (testInfo, parser) =>
      val input = csvStream(basicCSV(testInfo.testCase, testInfo.separator))
      val list = parser.get(input).unsafeRunSync()
      assert(list.size == 3)
      assertListFirst(list, testInfo.firstName, testInfo.firstValue)
      assertListLast(list, testInfo.lastName, testInfo.lastValue)

      case class Data(ID: Int, NAME: String)
      val data = list.map(_.to[Data]()).collect { case Right(v) => v }
      assert(data.length == 3)
      assert(data.head.NAME == testInfo.firstName)
    }
  }

  test("parser should allow fetching limited number of records as list") {
    basicTestWrapper(hasHeader = true) { (testInfo, parser) =>
      val source = Source.fromString(basicCSV(testInfo.testCase, testInfo.separator))
      val input = Reader[IO](chunkSize).read(source)
      val list = parser.get(input, 2).unsafeRunSync()
      assert(list.size == 2)
      assertListFirst(list, testInfo.firstName, testInfo.firstValue)
      assert(source.hasNext)
    }
  }

  test("parser should provide access to csv data through callback function") {
    basicTestWrapper(hasHeader = true) { (testInfo, parser) =>
      val input = csvStream(basicCSV(testInfo.testCase, testInfo.separator))
      val rowNums = mutable.Stack[Int]()
      val cb: Callback = row => {
        rowNums.push(row.rowNum)
        row.rowNum match {
          case 1 =>
            assert(row("NAME").contains(testInfo.firstName))
            assert(row("VALUE").contains(testInfo.firstValue))
            true
          case 3 =>
            assert(row("NAME").contains(testInfo.lastName))
            assert(row("VALUE").contains(testInfo.lastValue))
            true
          case _ => true
        }
      }
      parser.process(input)(cb).unsafeRunSync()
      assert(rowNums == mutable.Stack[Int](3, 2, 1))
    }
  }

  test("parser errors may be handled by IO error handler") {
    forAll(trimmings) { trim =>
      forAll(errorCases ++ (if (trim) trimErrorCases else noTrimErrorCases)) {
        (
          testCase: String,
          errorCode: String,
          line: Int,
          row: Int,
          col: Option[Int],
          field: Option[String]
        ) =>
          forAll(separators) { separator =>
            val config = CSVConfig().fieldDelimiter(separator).fieldSizeLimit(maxFieldSize)
            val parser = if (trim) config.stripSpaces.parser[IO] else config.parser[IO]
            val source = generateErroneousCSV(testCase, separator)
            val input = Reader[IO](chunkSize).read(source)
            val eh: IOErrorHandler = ex => assertError(ex, errorCode, line, col, row, field)(IO.unit)
            parser.process(input)(_ => true).handleErrorWith(eh).unsafeRunSync()
          }
      }
    }
  }

  test("parser should consume only required part of stream depending on callback return value") {
    val cb: Callback = row => row(0).exists(_.startsWith("2"))
    basicTestWrapper(hasHeader = true) { (testInfo, parser) =>
      val source = Source.fromString(basicCSV(testInfo.testCase, testInfo.separator))
      val input = Reader[IO](chunkSize).read(source)
      parser.process(input)(cb).unsafeRunSync()
      assert(source.hasNext)
    }
  }

  test("parser should provide access to csv data through async callback function") {
    basicTestWrapper(hasHeader = true) { (testInfo, parser) =>
      val input = csvStream(basicCSV(testInfo.testCase, testInfo.separator))
      val count = new LongAdder()
      val cb: Callback = row => {
        count.increment()
        row.rowNum match {
          case 1 =>
            assert(row("NAME").contains(testInfo.firstName))
            assert(row("VALUE").contains(testInfo.firstValue))
            true
          case 3 =>
            assert(row("NAME").contains(testInfo.lastName))
            assert(row("VALUE").contains(testInfo.lastValue))
            true
          case _ => true
        }
      }
      parser.async.process(input, 2)(cb).unsafeRunSync()
      assert(count.intValue() == 3)
    }
  }

  test("parser errors may be examined by callback passed to final impure method") {
    forAll(trimmings) { trim =>
      forAll(errorCases ++ (if (trim) trimErrorCases else noTrimErrorCases)) {
        (
          testCase: String,
          errorCode: String,
          line: Int,
          row: Int,
          col: Option[Int],
          field: Option[String]
        ) =>
          forAll(separators) { separator =>
            val config = CSVConfig().fieldDelimiter(separator).fieldSizeLimit(maxFieldSize)
            val parser = if (trim) config.stripSpaces.parser[IO] else config.parser[IO]
            val source = generateErroneousCSV(testCase, separator)
            val input = Reader[IO](chunkSize).read(source)
            val cdl = new CountDownLatch(1)
            def assertResult(result: Either[Throwable, Unit]): Unit = result match {
              case Right(_) =>
                fail()
              case Left(ex) =>
                assertError(ex, errorCode, line, col, row, field)(())
            }
            val rcb: Either[Throwable, Unit] => Unit = r => {
              cdl.countDown()
              assertResult(r)
            }
            parser.async.process(input)(_ => true).unsafeRunAsync(rcb)
            cdl.await(100, TimeUnit.MILLISECONDS)
            assert(cdl.getCount == 0)
          }
      }
    }
  }

  test("it should be possible to re-map header names to new ones") {
    forAll(basicCases()) { (testCase: String, firstName: String, firstValue: String, _: String, _: String) =>
      forAll(separators) { separator =>
        forAll(headerMappings) { mappedConfig =>
          val header = basicHeader(separator)
          val content = basicContent(testCase, separator)
          val csv = s"$header\n$content"
          val config = mappedConfig.fieldDelimiter(separator)
          val parser = config.parser[IO]
          val stream = csvStream(csv).through(parser.parse)
          val list = stream.compile.toList.unsafeRunSync()
          val head = list.head
          assert(list.nonEmpty)
          assert(head("ID").isEmpty)
          assert(head("IDENT").isDefined)
          assert(head("NAME").contains(firstName))
          assert(head("VAL").contains(firstValue))
          assert(head("VALUE").isEmpty)
          assert(head("OMMITED").isEmpty)
        }
      }
    }
  }

  test("it should be possible to set duplicated header names to get unique ones") {
    forAll(trimmings) { trim =>
      forAll(separators) { separator =>
        forAll(duplicateCases(separator)) { (header, position, replacement) =>
          val content = basicContent("basic", separator)
          val csv = s"$header\n$content"
          val headerMap = Map(position -> replacement)
          val config = CSVConfig().fieldDelimiter(separator).mapHeader(headerMap)
          val parser = if (trim) config.stripSpaces.parser[IO] else config.parser[IO]
          val stream = csvStream(csv).through(parser.parse)
          val list = stream.compile.toList.unsafeRunSync()
          assert(list.nonEmpty)
          assert(list.head(replacement).isDefined)
        }
      }
    }
  }

  private def csvStream(csvString: String) = Reader[IO](chunkSize).read(Source.fromString(csvString))

  private def assertListFirst(list: List[Record], firstName: String, firstValue: String): Unit = {
    val first = list.head
    assert(first.rowNum == 1)
    assertElement(first, firstName, firstValue)
  }

  private def assertListLast(list: List[Record], lastName: String, lastValue: String): Unit = {
    val last = list.last
    assert(last.rowNum == list.size)
    assertElement(last, lastName, lastValue)
  }

  private def assertElement(elem: Record, name: String, value: String): Unit = {
    assert(elem("NAME").contains(name))
    assert(elem("VALUE").contains(value))
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
    assert(ex.position.forall(_.line == line))
    assert(ex.position.forall(_.row == row))
    assert(ex.col == col)
    assert(ex.field == field)
    ()
  }

  private def assertIOException(ex: Throwable, errorCode: String): Unit = {
    assert(ex.getMessage == errorCode)
    ()
  }

  private lazy val trimmings = Table("trim", false, true)

  private lazy val trimmingWs = Table(
    ("trim", "whitespace"),
    (false, " "),
    (true, "")
  )

  private lazy val emptyContentWs = Table(
    ("trim", "whitespace"),
    (false, ""),
    (true, " "),
    (true, "\u2029") // paragraph separator, to test with another whitespace
  )

  private lazy val separators = Table("separator", ',', ';', '\t')

  private lazy val headerMappings = Table(
    "headerMapping",
    CSVConfig().mapHeader(Map("ID" -> "IDENT", "VALUE" -> "VAL")),
    CSVConfig().mapHeader(Map(0 -> "IDENT", 3 -> "VAL"))
  )

  private def basicCases(ws: String = "") = Table(
    ("testCase", "firstName", "firstValue", "lastName", "lastValue"),
    ("basic", "Funky Koval", "100.00", "Han Solo", "999.99"),
    ("basic quoted", "Koval, Funky", "100.00", "Solo, Han", "999.99"),
    ("mixed", "Funky Koval", "100.00", "Solo, Han", "999.99"),
    ("spaces", " Funky Koval ", " ", s"$ws${ws}Han Solo$ws$ws", " 999.99 "),
    ("empty values", "", "", "", ""),
    ("crlf", "Funky Koval", "", "", ""),
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
      case "crlf" =>
        s""""1"${s}Funky Koval${s}01.01.2001$s\r
          |"2"${s}Eva Solo${s}31.12.2012${s}"123.45"\r
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
    ("unmatched quotation with escaped one", "unmatchedQuotation", 2, 1, Some(3), Some("NAME")),
    ("field too long", "fieldTooLong", 2, 1, Some(103), Some("NAME")),
    ("field too long through unmatched quotation", "fieldTooLong", 3, 1, Some(11), Some("NAME")),
    ("malformed header", "unclosedQuotation", 1, 0, Some(10), None),
    ("duplicated header", "duplicatedHeader", 1, 0, None, Some("VALUE")),
    ("no content", "missingHeader", 1, 0, None, None),
    ("io exception", "message", 0, 0, None, None)
  )

  private lazy val noTrimErrorCases = Table(
    ("testCase", "errorCode", "lineNum", "colNum", "rowNum", "field"),
    ("unclosed quotation after spaces", "unclosedQuotation", 2, 1, Some(5), Some("NAME")),
    ("unclosed/unmatched quotation with trailing spaces", "unclosedQuotation", 2, 1, Some(5), Some("NAME"))
  )

  private lazy val trimErrorCases = Table(
    ("testCase", "errorCode", "lineNum", "colNum", "rowNum", "field"),
    ("unclosed/unmatched quotation with trailing spaces", "unmatchedQuotation", 2, 1, Some(5), Some("NAME"))
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
      case "unclosed quotation after spaces" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1$s  "Funky Koval"${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unclosed/unmatched quotation with trailing spaces" =>
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
      case "duplicated header" =>
        s"""ID${s}NAME${s}VALUE${s}VALUE
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

  private def emptyCases(ws: String) = Table(
    ("testCase", "content"),
    ("empty", ""),
    ("space", s"$ws"),
    ("spaces", s"$ws$ws$ws"),
    ("new line", "\n"),
    ("new lines", "\n\n\n"),
    ("new lines and spaces", s"$ws\n$ws\n$ws\n"),
    ("mixed new lines and spaces", s"$ws$ws\n$ws\n\n$ws$ws$ws\n$ws")
  )

  private def duplicateCases(s: Char) = Table(
    ("header", "position", "replacement"),
    (s"ID${s}VALUE${s}DATE${s}VALUE", 1, "NAME"),
    (s"ID${s}NAME${s}VALUE${s}VALUE", 2, "DATE")
  )
}
