/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import scala.io.Source
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import info.fingo.spata.io.Reader

class CSVConfigTS extends AnyFunSuite:

  test("Config should be build correctly"):
    val config = CSVConfig().fieldDelimiter(';').noHeader.fieldSizeLimit(100)
    val expected = CSVConfig(';', '\n', '"', hasHeader = false, NoHeaderMap, trimSpaces = false, Some(100))
    assert(config == expected)

  test("Config should allow parser creation with proper settings"):
    val rs = 0x1E.toChar
    val content = s"'value 1A'|'value ''1B'$rs'value 2A'|'value ''2B'"
    val config = CSVConfig().fieldDelimiter('|').quoteMark('\'').recordDelimiter(rs).noHeader
    val data = Reader[IO].read(Source.fromString(content))
    val parser = config.parser[IO]
    val result = parser.get(data).unsafeRunSync()
    assert(result.length == 2)
    assert(result.head.size == 2)
    assert(result.head(1).contains("value '1B"))

  test("Config should allow renderer creation with proper settings"):
    val rs = 0x1E.toChar
    val records = Seq(
      Record.fromPairs("A" -> "value 1A", "B" -> "value '1B"),
      Record.fromPairs("A" -> "value 2A", "B" -> "value '2B")
    )
    val csv = s"'value 1A'|'value ''1B'$rs'value 2A'|'value ''2B'"
    val config = CSVConfig().fieldDelimiter('|').quoteMark('\'').recordDelimiter(rs).noHeader.escapeAll
    val stream = Stream(records*).covaryAll[IO, Record]
    val renderer = config.renderer[IO]
    val result = stream.through(renderer.render).compile.toList.unsafeRunSync().mkString
    assert(result == csv)

  test("Config should clearly present its composition through toString"):
    val c1 = CSVConfig(',', '\n', '"')
    assert(c1.toString == """CSVConfig(',', '\n', '"', header, no mapping, no trimming, escape required)""")
    val c2 = CSVConfig('\t', '\r', '\'', hasHeader = false, Map("x" -> "y"), trimSpaces = true, Some(100))
    assert(
      c2.toString == """CSVConfig('\t', '\r', ''', no header, header mapping, space trimming, 100, escape required)"""
    )
    val c3 = CSVConfig(';', ' ', '\"', escapeMode = CSVConfig.EscapeSpaces)
    assert(c3.toString == """CSVConfig(';', ' ', '"', header, no mapping, no trimming, escape spaces)""")
    val c4 = CSVConfig('\u001F', '\u001E', '|', fieldSizeLimit = Some(256), escapeMode = CSVConfig.EscapeAll)
    assert(c4.toString == """CSVConfig('␣', '␣', '|', header, no mapping, no trimming, 256, escape all)""")
