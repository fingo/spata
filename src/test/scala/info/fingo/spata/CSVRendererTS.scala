/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.charset.Charset
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Codec
import cats.effect.IO
import fs2.{Pipe, Stream}
import info.fingo.spata.CSVConfig.{EscapeAll, EscapeMode, EscapeRequired, EscapeSpaces}
import info.fingo.spata.io.Writer
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
              val renderer = config(separator, escapeMode, headerMode).renderer[IO]
              val stream = Stream(recs: _*).covaryAll[IO, Record]
              val out = stream.through(render(renderer, hdr, headerMode))
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
              val renderer = config(separator, escapeMode, headerMode).renderer[IO]
              val stream = Stream(clss: _*).covaryAll[IO, Data].map(Record.from(_))
              val out = stream.through(render(renderer, hdr, headerMode))
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
    val recs = Record.fromPairs("x" -> "y") :: records("basic", ',', hdr)
    val stream = Stream(recs: _*).covaryAll[IO, Record]
    val outHdr = stream.through(CSVRenderer[IO].render(hdr)).attempt
    val resHdr = outHdr.compile.toList.unsafeRunSync()
    assert(resHdr.last.isLeft)
    assert(resHdr.map(_.getOrElse('?')).mkString.startsWith(hdr.names.mkString(",")))
    val outNoHdr = stream.through(CSVRenderer[IO].render).attempt
    val resNoHdr = outNoHdr.compile.toList.unsafeRunSync()
    assert(resNoHdr.last.isLeft)
    assert(resNoHdr.head.contains('x'))
  }

  test("renderer should convert records to rows") {
    forAll(separators) { separator =>
      forAll(headerModes) { headerMode => // header mode must not influence row creation
        forAll(escapeModes) { escapeMode =>
          forAll(testCases) { (_: String, _: String, contentCase: String, headerModes: List[String]) =>
            if (headerModes.contains(headerMode)) {
              val recs = records(contentCase, separator)
              val renderer = config(separator, escapeMode, headerMode).renderer[IO]
              val stream = Stream(recs: _*).covaryAll[IO, Record]
              val out = stream.through(renderer.rows).intersperse("\n")
              val res = out.compile.toList.unsafeRunSync().mkString
              // header is never generated for parser.rows
              val content = rendered("empty", contentCase, separator, escapeMode, headerMode)
              assert(res == content)
            }
          }
        }
      }
    }
  }

  test("renderer converted data should be properly encoded and handled by writer") {
    val charset = Charset.forName("ISO-8859-2")
    implicit val codec = new Codec(charset)
    forAll(separators) { separator =>
      forAll(headerModes) { headerMode =>
        forAll(escapeModes) { escapeMode =>
          forAll(testCases) { (_: String, headerCase: String, contentCase: String, headerModes: List[String]) =>
            if (headerModes.contains(headerMode)) {
              val os = new ByteArrayOutputStream()
              val hdr = header(headerCase)
              val recs = records(contentCase, separator, hdr)
              val renderer = config(separator, escapeMode, headerMode).renderer[IO]
              val stream = Stream(recs: _*).covaryAll[IO, Record]
              val out =
                stream.through(render(renderer, hdr, headerMode)).through(Writer[IO].write(IO[OutputStream](os)))
              out.compile.drain.unsafeRunSync() // run
              val content = rendered(headerCase, contentCase, separator, escapeMode, headerMode)
              assert(os.toByteArray.sameElements(content.getBytes(charset)))
            }
          }
        }
      }
    }
  }

  test("it should be possible to re-map header names to new ones") {
    forAll(separators) { separator =>
      forAll(headerModes) { headerMode =>
        forAll(escapeModes) { escapeMode =>
          forAll(headerMappings) { mappedConfig =>
            forAll(testCases) { (_: String, headerCase: String, contentCase: String, headerModes: List[String]) =>
              if (headerModes.contains(headerMode)) {
                val hdr = header(headerCase)
                val recs = records(contentCase, separator, hdr)
                val renderer = config(separator, escapeMode, headerMode, mappedConfig.headerMap).renderer[IO]
                val stream = Stream(recs: _*).covaryAll[IO, Record]
                val out = stream.through(render(renderer, hdr, headerMode))
                val res = out.compile.toList.unsafeRunSync().mkString
                val content = rendered(headerCase, contentCase, separator, escapeMode, headerMode, true)
                assert(res == content)
              }
            }
          }
        }
      }
    }
  }

  private def config(
    separator: Char,
    escapeMode: EscapeMode,
    headerMode: String,
    headerMap: HeaderMap = NoHeaderMap
  ): CSVConfig = {
    val c = CSVConfig().fieldDelimiter(separator)
    val cc = escapeMode match {
      case EscapeAll => c.escapeAll
      case EscapeSpaces => c.escapeSpaces
      case EscapeRequired => c
    }
    if (headerMode == "none") cc.noHeader else cc.mapHeader(headerMap)
  }

  private def render(renderer: CSVRenderer[IO], header: Header, headerMode: String): Pipe[IO, Record, Char] =
    if (headerMode == "explicit") renderer.render(header) else renderer.render

  private lazy val separators = Table("separator", ',', ';', '\t')

  private lazy val headerModes = Table("mode", "implicit", "explicit", "none")

  private lazy val escapeModes = Table("mode", EscapeRequired, EscapeSpaces, EscapeAll)

  private lazy val headerMappings = Table(
    "headerMapping",
    CSVConfig().mapHeader(Map("id" -> "ident", "value" -> "val")),
    CSVConfig().mapHeader(Map(0 -> "ident", 3 -> "val"))
  )

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

  private def records(testCase: String, separator: Char, header: Header): List[Record] = testCase match {
    case "basic" =>
      Record("1", "Funky Koval", "01.01.2001", "100.0")(header) ::
        Record("2", "Eva Solo", "31.12.2012", "123.45")(header) ::
        Record("3", "Koziołek Matołek", "09.09.1999", "999.99")(header) ::
        Nil
    case "quotes and seps" =>
      Record("1", s"Funky Koval$separator", "01.01.2001", "100.0")(header) ::
        Record("2", "Eva\nSolo", "31.12.2012", "123.45")(header) ::
        Record("3", """"Koziołek" Matołek""", "09.09.1999", "999.99")(header) ::
        Nil
    case "spaces" =>
      Record("1", "Funky Koval", "01.01.2001", "100.0")(header) ::
        Record("2", "Eva Solo ", "31.12.2012", "123.45")(header) ::
        Record("3", "  Koziołek Matołek", "09.09.1999", "999.99")(header) ::
        Nil
    case "empty" => Nil
  }

  private def records(testCase: String, separator: Char): List[Record] = testCase match {
    case "basic" =>
      Record.fromValues("1", "Funky Koval", "01.01.2001", "100.0") ::
        Record.fromValues("2", "Eva Solo", "31.12.2012", "123.45") ::
        Record.fromValues("3", "Koziołek Matołek", "09.09.1999", "999.99") ::
        Nil
    case "quotes and seps" =>
      Record.fromValues("1", s"Funky Koval$separator", "01.01.2001", "100.0") ::
        Record.fromValues("2", "Eva\nSolo", "31.12.2012", "123.45") ::
        Record.fromValues("3", """"Koziołek" Matołek""", "09.09.1999", "999.99") ::
        Nil
    case "spaces" =>
      Record.fromValues("1", "Funky Koval", "01.01.2001", "100.0") ::
        Record.fromValues("2", "Eva Solo ", "31.12.2012", "123.45") ::
        Record.fromValues("3", "  Koziołek Matołek", "09.09.1999", "999.99") ::
        Nil
    case "empty" => Nil
  }

  private def classes(testCase: String, separator: Char): List[Data] = testCase match {
    case "basic" =>
      Data(1, "Funky Koval", LocalDate.of(2001, 1, 1), 100.00) ::
        Data(2, "Eva Solo", LocalDate.of(2012, 12, 31), 123.45) ::
        Data(3, "Koziołek Matołek", LocalDate.of(1999, 9, 9), 999.99) ::
        Nil
    case "quotes and seps" =>
      Data(1, s"Funky Koval$separator", LocalDate.of(2001, 1, 1), 100.00) ::
        Data(2, "Eva\nSolo", LocalDate.of(2012, 12, 31), 123.45) ::
        Data(3, """"Koziołek" Matołek""", LocalDate.of(1999, 9, 9), 999.99) ::
        Nil
    case "spaces" =>
      Data(1, "Funky Koval", LocalDate.of(2001, 1, 1), 100.00) ::
        Data(2, "Eva Solo ", LocalDate.of(2012, 12, 31), 123.45) ::
        Data(3, "  Koziołek Matołek", LocalDate.of(1999, 9, 9), 999.99) ::
        Nil
    case "empty" => Nil
  }

  private def rendered(
    headerCase: String,
    contentCase: String,
    separator: Char,
    escapeMode: EscapeMode,
    headerMode: String,
    mapped: Boolean = false
  ): String = {
    val header = if (headerMode == "none") "" else renderedHeader(headerCase, separator, escapeMode, mapped)
    val content = renderedContent(contentCase, separator, escapeMode)
    List(header, content).filterNot(_.isEmpty).mkString("\n")
  }

  private def renderedHeader(testCase: String, separator: Char, escapeMode: EscapeMode, mapped: Boolean): String = {
    val s = separator
    testCase match {
      case "basic" =>
        if (mapped)
          escapeMode match {
            case EscapeAll => s""""ident"$s"name"$s"date"$s"val""""
            case _ => s"""ident${s}name${s}date${s}val"""
          }
        else
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
               |"3"$s"Koziołek Matołek"$s"09.09.1999"$s"999.99"""".stripMargin
          case _ =>
            s"""1${s}Funky Koval${s}01.01.2001${s}100.0
               |2${s}Eva Solo${s}31.12.2012${s}123.45
               |3${s}Koziołek Matołek${s}09.09.1999${s}999.99""".stripMargin
        }
      case "quotes and seps" =>
        escapeMode match {
          case EscapeAll =>
            s""""1"$s"Funky Koval$s"$s"01.01.2001"$s"100.0"
               |\"2"$s"Eva\nSolo"$s"31.12.2012"$s"123.45"
               |"3"$s"\""Koziołek"" Matołek"$s"09.09.1999"$s"999.99"""".stripMargin
          case _ =>
            s"""1$s"Funky Koval$s"${s}01.01.2001${s}100.0
               |2$s"Eva\nSolo"${s}31.12.2012${s}123.45
               |3$s"\""Koziołek"" Matołek"${s}09.09.1999${s}999.99""".stripMargin
        }
      case "spaces" =>
        escapeMode match {
          case EscapeAll =>
            s""""1"$s"Funky Koval"$s"01.01.2001"$s"100.0"
               |"2"$s"Eva Solo "$s"31.12.2012"$s"123.45"
               |"3"$s"  Koziołek Matołek"$s"09.09.1999"$s"999.99"""".stripMargin
          case EscapeSpaces =>
            s"""1${s}Funky Koval${s}01.01.2001${s}100.0
               |2$s"Eva Solo "${s}31.12.2012${s}123.45
               |3$s"  Koziołek Matołek"${s}09.09.1999${s}999.99""".stripMargin
          case EscapeRequired =>
            s"""1${s}Funky Koval${s}01.01.2001${s}100.0
               |2${s}Eva Solo ${s}31.12.2012${s}123.45
               |3$s  Koziołek Matołek${s}09.09.1999${s}999.99""".stripMargin
        }
      case "empty" => ""
    }
  }
}
