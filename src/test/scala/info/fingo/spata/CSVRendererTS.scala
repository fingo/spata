/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import cats.effect.IO
import fs2.Stream
import info.fingo.spata.CSVConfig.{EscapeAll, EscapeMode, EscapeRequired, EscapeSpaces}
import info.fingo.spata.text.StringRenderer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class CSVRendererTS extends AnyFunSuite with TableDrivenPropertyChecks {
  type StreamErrorHandler = Throwable => Stream[IO, Char]

  case class Data(id: Int, name: String, date: LocalDate, value: Double)

  implicit val ldsr: StringRenderer[LocalDate] =
    (value: LocalDate) => DateTimeFormatter.ofPattern("dd.MM.yyyy").format(value)

  test("renderer should process records matching provided or implicit header") {
    forAll(separators) { separator =>
      forAll(headerModes) { headerMode =>
        forAll(escapeModes) { escapeMode =>
          forAll(testCases) { (_: String, headerCase: String, contentCase: String, headerModes: List[String]) =>
            if (headerModes.contains(headerMode)) {
              val hdr = header(headerCase)
              val recs = records(contentCase, separator, hdr)
              val renderer = config(separator, escapeMode, headerMode).renderer[IO]()
              val stream = Stream(recs: _*).unNone.covaryAll[IO, Record]
              val pipe = if (headerMode == "explicit") renderer.render(hdr) else renderer.render
              val out = stream.through(pipe)
              val res = out.compile.toList.unsafeRunSync().mkString
              val content = rendered(headerCase, contentCase, separator, escapeMode, headerMode)
              assert(res == content)
            }
          }
        }
      }
    }
  }

  test("renderer should process records converted from case classes") {
    forAll(separators) { separator =>
      forAll(headerModes) { headerMode =>
        forAll(escapeModes) { escapeMode =>
          forAll(testCases) { (_: String, headerCase: String, contentCase: String, headerModes: List[String]) =>
            if (headerModes.contains(headerMode)) {
              val hdr = header(headerCase)
              val clss = classes(contentCase, separator)
              val renderer = config(separator, escapeMode, headerMode).renderer[IO]()
              val stream = Stream(clss: _*).covaryAll[IO, Data].map(Record.from(_))
              val pipe = if (headerMode != "implicit") renderer.render(hdr) else renderer.render
              val out = stream.through(pipe)
              val res = out.compile.toList.unsafeRunSync().mkString
              val content = rendered(headerCase, contentCase, separator, escapeMode, headerMode)
              assert(res == content)
            }
          }
        }
      }
    }
  }

  test("renderer should raise error if any record does not match the header") {
    val hdr = header("basic")
    val recs = Some(Record.fromPairs("x" -> "y")) :: records("basic", ',', hdr)
    val stream = Stream(recs: _*).unNone.covaryAll[IO, Record]
    val renderer = CSVRenderer[IO]()
    val outHdr = stream.through(renderer.render(hdr)).attempt
    val resHdr = outHdr.compile.toList.unsafeRunSync()
    assert(resHdr.last.isLeft)
    assert(resHdr.map(_.getOrElse('?')).mkString.startsWith(hdr.names.mkString(",")))
    val outNoHdr = stream.through(renderer.render).attempt
    val resNoHdr = outNoHdr.compile.toList.unsafeRunSync()
    assert(resNoHdr.last.isLeft)
    assert(resNoHdr.head.contains('x'))
  }

  private def config(separator: Char, escapeMode: EscapeMode, headerMode: String) = {
    val c = CSVConfig().fieldDelimiter(separator)
    val cc = escapeMode match {
      case EscapeAll => c.escapeAll()
      case EscapeSpaces => c.escapeSpaces()
      case EscapeRequired => c
    }
    if (headerMode == "none") cc.noHeader() else cc
  }

  private lazy val separators = Table("separator", ',', ';', '\t')

  private lazy val headerModes = Table("mode", "implicit", "explicit", "none")

  private lazy val escapeModes = Table("mode", EscapeRequired, EscapeSpaces, EscapeAll)

  private lazy val testCases = Table(
    ("testCase", "header", "content", "headerMode"),
    ("basic", "basic", "basic", List("explicit", "implicit", "none")),
    ("spaces", "basic", "spaces", List("explicit", "implicit", "none")),
    ("quotes and seps", "basic", "quotes and seps", List("explicit", "implicit", "none")),
    ("no content", "basic", "empty", List("explicit", "none")),
    ("empty", "empty", "empty", List("implicit", "none"))
  )

  private def header(testCase: String): Header = testCase match {
    case "basic" => Header("id", "name", "date", "value")
    case "empty" => Header()
  }

  private def records(testCase: String, separator: Char, header: Header): List[Option[Record]] = testCase match {
    case "basic" =>
      Record("1", "Funky Koval", "01.01.2001", "100.0")(header) ::
        Record("2", "Eva Solo", "31.12.2012", "123.45")(header) ::
        Record("3", "Han Solo", "09.09.1999", "999.99")(header) ::
        Nil
    case "quotes and seps" =>
      Record("1", s"Funky Koval$separator", "01.01.2001", "100.0")(header) ::
        Record("2", "Eva\nSolo", "31.12.2012", "123.45")(header) ::
        Record("3", """"Han" Solo""", "09.09.1999", "999.99")(header) ::
        Nil
    case "spaces" =>
      Record("1", "Funky Koval", "01.01.2001", "100.0")(header) ::
        Record("2", "Eva Solo ", "31.12.2012", "123.45")(header) ::
        Record("3", "  Han Solo", "09.09.1999", "999.99")(header) ::
        Nil
    case "empty" => Nil
  }

  private def classes(testCase: String, separator: Char): List[Data] = testCase match {
    case "basic" =>
      Data(1, "Funky Koval", LocalDate.of(2001, 1, 1), 100.00) ::
        Data(2, "Eva Solo", LocalDate.of(2012, 12, 31), 123.45) ::
        Data(3, "Han Solo", LocalDate.of(1999, 9, 9), 999.99) ::
        Nil
    case "quotes and seps" =>
      Data(1, s"Funky Koval$separator", LocalDate.of(2001, 1, 1), 100.00) ::
        Data(2, "Eva\nSolo", LocalDate.of(2012, 12, 31), 123.45) ::
        Data(3, """"Han" Solo""", LocalDate.of(1999, 9, 9), 999.99) ::
        Nil
    case "spaces" =>
      Data(1, "Funky Koval", LocalDate.of(2001, 1, 1), 100.00) ::
        Data(2, "Eva Solo ", LocalDate.of(2012, 12, 31), 123.45) ::
        Data(3, "  Han Solo", LocalDate.of(1999, 9, 9), 999.99) ::
        Nil
    case "empty" => Nil
  }

  private def rendered(
    headerCase: String,
    contentCase: String,
    separator: Char,
    escapeMode: EscapeMode,
    headerMode: String
  ): String = {
    val header = if (headerMode == "none") "" else renderedHeader(headerCase, separator, escapeMode)
    val content = renderedContent(contentCase, separator, escapeMode)
    List(header, content).filterNot(_.isEmpty).mkString("\n")
  }

  private def renderedHeader(testCase: String, separator: Char, escapeMode: EscapeMode): String = {
    val s = separator
    testCase match {
      case "basic" =>
        escapeMode match {
          case EscapeAll => s""""id"$s"name"$s"date"$s"value""""
          case _ => s"""id${s}name${s}date${s}value"""
        }
      case "empty" => ""
    }
  }

  private def renderedContent(testCase: String, separator: Char, escapeMode: EscapeMode): String = {
    val s = separator
    testCase match {
      case "basic" =>
        escapeMode match {
          case EscapeAll =>
            s""""1"$s"Funky Koval"$s"01.01.2001"$s"100.0"
               |"2"$s"Eva Solo"$s"31.12.2012"$s"123.45"
               |"3"$s"Han Solo"$s"09.09.1999"$s"999.99"""".stripMargin
          case _ =>
            s"""1${s}Funky Koval${s}01.01.2001${s}100.0
               |2${s}Eva Solo${s}31.12.2012${s}123.45
               |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
        }
      case "quotes and seps" =>
        escapeMode match {
          case EscapeAll =>
            s""""1"$s"Funky Koval$s"$s"01.01.2001"$s"100.0"
               |\"2"$s"Eva\nSolo"$s"31.12.2012"$s"123.45"
               |"3"$s"\""Han"" Solo"$s"09.09.1999"$s"999.99"""".stripMargin
          case _ =>
            s"""1$s"Funky Koval$s"${s}01.01.2001${s}100.0
               |2$s"Eva\nSolo"${s}31.12.2012${s}123.45
               |3$s"\""Han"" Solo"${s}09.09.1999${s}999.99""".stripMargin
        }
      case "spaces" =>
        escapeMode match {
          case EscapeAll =>
            s""""1"$s"Funky Koval"$s"01.01.2001"$s"100.0"
               |"2"$s"Eva Solo "$s"31.12.2012"$s"123.45"
               |"3"$s"  Han Solo"$s"09.09.1999"$s"999.99"""".stripMargin
          case EscapeSpaces =>
            s"""1${s}Funky Koval${s}01.01.2001${s}100.0
               |2$s"Eva Solo "${s}31.12.2012${s}123.45
               |3$s"  Han Solo"${s}09.09.1999${s}999.99""".stripMargin
          case EscapeRequired =>
            s"""1${s}Funky Koval${s}01.01.2001${s}100.0
               |2${s}Eva Solo ${s}31.12.2012${s}123.45
               |3$s  Han Solo${s}09.09.1999${s}999.99""".stripMargin
        }
      case "empty" => ""
    }
  }
}
