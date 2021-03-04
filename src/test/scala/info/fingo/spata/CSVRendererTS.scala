/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import cats.effect.IO
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class CSVRendererTS extends AnyFunSuite with TableDrivenPropertyChecks {

  test("renderer should process records matching provided or implicit header") {
    forAll(separators) { separator =>
      val config = CSVConfig().fieldDelimiter(separator)
      forAll(headerModes) { headerMode =>
        forAll(testCases) { (_: String, headerCase: String, contentCase: String, headerModes: List[String]) =>
          if (headerModes.contains(headerMode)) {
            val hdr = header(headerCase)
            val recs = records(contentCase, hdr)
            val renderer = new CSVRenderer[IO](config)
            val stream = Stream(recs: _*).covaryAll[IO, Record]
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
    case "basic" => new Header("id", "name", "date", "value")
    case "basic quoted" => new Header(""""id"""", """"name"""", """"date"""", """"value"""")
    case "empty" => new Header()
  }

  private def records(testCase: String, header: Header): List[Record] = testCase match {
    case "basic" =>
      new Record("1", "Funky Koval", "01.01.2001", "100.00")(header) ::
        new Record("2", "Eva Solo", "31.12.2012", "123.45")(header) ::
        new Record("3", "Han Solo", "09.09.1999", "999.99")(header) ::
        Nil
    case "basic quoted" =>
      new Record(""""1"""", """"Funky Koval"""", """"01.01.2001"""", """"100.00"""")(header) ::
        new Record(""""2"""", """"Eva Solo"""", """"31.12.2012"""", """"123.45"""")(header) ::
        new Record(""""3"""", """"Han Solo"""", """"09.09.1999"""", """"999.99"""")(header) ::
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
        s"""1${s}Funky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "basic quoted" =>
        s""""1"$s"Koval, Funky"$s"01.01.2001"$s"100.00"
           |"2"$s"Solo, Eva"$s"31.12.2012"$s"123.45"
           |"3"$s"Solo, Han"$s"09.09.1999"$s"999.99"""".stripMargin
      case "empty" => ""
    }
  }
}
