/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import java.time.LocalDate
import cats.effect.IO
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class CSVRendererTS extends AnyFunSuite with TableDrivenPropertyChecks {

  case class Data(id: Int, name: String, date: LocalDate, value: Double)

  test("renderer should process records matching provided or implicit header") {
    forAll(separators) { separator =>
      val config = CSVConfig().fieldDelimiter(separator)
      forAll(headerModes) { headerMode =>
        forAll(testCases) { (_: String, headerCase: String, contentCase: String, headerModes: List[String]) =>
          if (headerModes.contains(headerMode)) {
            val hdr = header(headerCase)
            val recs = records(contentCase, hdr)
            val renderer = config.renderer[IO]()
            val stream = Stream(recs: _*).unNone.covaryAll[IO, Record]
            val pipe = if (headerMode == "explicit") renderer.render(hdr) else renderer.render
            val out = stream.through(pipe)
            val res = out.compile.toList.unsafeRunSync().mkString
            val content = rendered(headerCase, contentCase, separator)
            assert(res == content)
          }
        }
      }
    }
  }

  test("renderer should process records converted from case classes") {
    forAll(separators) { separator =>
      val config = CSVConfig().fieldDelimiter(separator)
      forAll(headerModes) { headerMode =>
        forAll(testCases) { (_: String, headerCase: String, contentCase: String, headerModes: List[String]) =>
          if (headerModes.contains(headerMode)) {
            val hdr = header(headerCase)
            val clss = classes(contentCase)
            val renderer = config.renderer[IO]()
            val stream = Stream(clss: _*).covaryAll[IO, Data].map(Record.from(_))
            val pipe = if (headerMode == "explicit") renderer.render(hdr) else renderer.render
            val out = stream.through(pipe)
            val res = out.compile.toList.unsafeRunSync().mkString
            val content = rendered(headerCase, contentCase, separator)
            assert(res == content)
          }
        }
      }
    }
  }

  private lazy val separators = Table("separator", ',', ';', '\t')

  private lazy val headerModes = Table("mode", "implicit", "explicit")

  private lazy val testCases = Table(
    ("testCase", "header", "content", "headerMode"),
    ("basic", "basic", "basic", List("explicit", "implicit")),
    ("no content", "basic", "empty", List("explicit")),
    ("no content", "empty", "empty", List("implicit"))
  )

  private def header(testCase: String): Header = testCase match {
    case "basic" => Header("id", "name", "date", "value")
    case "basic quoted" => Header(""""id"""", """"name"""", """"date"""", """"value"""")
    case "empty" => Header()
  }

  private def records(testCase: String, header: Header): List[Option[Record]] = testCase match {
    case "basic" =>
      Record("1", "Funky Koval", "2001-01-01", "100.0")(header) ::
        Record("2", "Eva Solo", "2012-12-31", "123.45")(header) ::
        Record("3", "Han Solo", "1999-09-09", "999.99")(header) ::
        Nil
    case "basic quoted" =>
      Record(""""1"""", """"Funky Koval"""", """"2001-01-01"""", """"100.0"""")(header) ::
        Record(""""2"""", """"Eva Solo"""", """"2012-12-31"""", """"123.45"""")(header) ::
        Record(""""3"""", """"Han Solo"""", """"1999-09-09"""", """"999.99"""")(header) ::
        Nil
    case "empty" => Nil
  }

  private def classes(testCase: String): List[Data] = testCase match {
    case "basic" =>
      Data(1, "Funky Koval", LocalDate.of(2001, 1, 1), 100.00) ::
        Data(2, "Eva Solo", LocalDate.of(2012, 12, 31), 123.45) ::
        Data(3, "Han Solo", LocalDate.of(1999, 9, 9), 999.99) ::
        Nil
    case "empty" => Nil
  }

  private def rendered(headerCase: String, contentCase: String, separator: Char): String = {
    val header = renderedHeader(headerCase, separator)
    val content = renderedContent(contentCase, separator)
    List(header, content).filterNot(_.isEmpty).mkString("\n")
  }

  private def renderedHeader(testCase: String, separator: Char): String = {
    val s = separator
    testCase match {
      case "basic" => s"""id${s}name${s}date${s}value"""
      case "basic quoted" => s""""id"$s"name"$s"date"$s"value""""
      case "empty" => ""
    }
  }

  private def renderedContent(testCase: String, separator: Char): String = {
    val s = separator
    testCase match {
      case "basic" =>
        s"""1${s}Funky Koval${s}2001-01-01${s}100.0
           |2${s}Eva Solo${s}2012-12-31${s}123.45
           |3${s}Han Solo${s}1999-09-09${s}999.99""".stripMargin
      case "basic quoted" =>
        s""""1"$s"Koval, Funky"$s"2001-01-01"$s"100.0"
           |"2"$s"Solo, Eva"$s"2012-12-31"$s"123.45"
           |"3"$s"Solo, Han"$s"1999-09-09"$s"999.99"""".stripMargin
      case "empty" => ""
    }
  }
}
