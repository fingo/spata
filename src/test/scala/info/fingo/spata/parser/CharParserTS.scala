/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.parser

import cats.effect.IO
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import Config._
import CharStates._
import CharFailures._

class CharParserTS extends AnyFunSuite with TableDrivenPropertyChecks {

  test("Char parser should correctly parse provided input without trimming") {
    val parser = new CharParser[IO](sep, rs, qt, false)
    forAll(regularCases ++ spaceNoTrimCases) { (_, input, output) =>
      val result = parse(parser, input)
      assert(result == output)
    }
  }

  test("Char parser should correctly parse provided input with trimming") {
    val parser = new CharParser[IO](sep, rs, qt, true)
    forAll(regularCases ++ spaceTrimCases) { (_, input, output) =>
      val result = parse(parser, input)
      assert(result == output)
    }
  }

  test("Char parser should return error info for malformed input while parsing without trimming") {
    val parser = new CharParser[IO](sep, rs, qt, false)
    forAll(malformedCases ++ malformedNoTrimCases) { (_, input, output) =>
      val result = parse(parser, input)
      assert(result == output)
    }
  }

  test("Char parser should return error info for malformed input while parsing with trimming") {
    val parser = new CharParser[IO](sep, rs, qt, true)
    forAll(malformedCases ++ malformedTrimCases) { (_, input, output) =>
      val result = parse(parser, input)
      assert(result == output)
    }
  }

  private def parse(parser: CharParser[IO], input: String) = {
    val stream = Stream(input.toIndexedSeq: _*).through(parser.toCharResults)
    stream.compile.toList.unsafeRunSync()
  }

  private lazy val regularCases = Table(
    ("testCase", "input", "output"),
    ("basic", "abc", List(csr('a'), csr('b'), csr('c'), csf)),
    ("sepEnd", s"abc$sep", List(csr('a'), csr('b'), csr('c'), csff, csf)),
    ("sepMid", s"ab${sep}c", List(csr('a'), csr('b'), csff, csr('c'), csf)),
    ("sepBeg", s"${sep}abc", List(csff, csr('a'), csr('b'), csr('c'), csf)),
    ("rowEnd", s"abc$rs", List(csr('a'), csr('b'), csr('c'), csfr, csf)),
    ("rowMid", s"ab${rs}c", List(csr('a'), csr('b'), csfr, csr('c'), csf)),
    ("rowBeg", s"${rs}abc", List(csfr, csr('a'), csr('b'), csr('c'), csf)),
    ("sepRowMidEnd", s"ab${sep}c$rs", List(csr('a'), csr('b'), csff, csr('c'), csfr, csf)),
    ("sepRowEndEnd", s"abc$sep$rs", List(csr('a'), csr('b'), csr('c'), csff, csfr, csf)),
    ("sepRowMidMid", s"ab$sep${rs}c", List(csr('a'), csr('b'), csff, csfr, csr('c'), csf)),
    ("sepRowBegMid", s"${sep}ab${rs}c", List(csff, csr('a'), csr('b'), csfr, csr('c'), csf)),
    ("sepRowBegBeg", s"$sep${rs}abc", List(csff, csfr, csr('a'), csr('b'), csr('c'), csf)),
    ("sepRowSwap", s"abc$rs$sep", List(csr('a'), csr('b'), csr('c'), csfr, csff, csf)),
    ("quote", s"${qt}abc$qt", List(csq, csq('a'), csq('b'), csq('c'), cses, csf)),
    ("qtEscMid", s"${qt}ab$qt${qt}c$qt", List(csq, csq('a'), csq('b'), cses, csq('"'), csq('c'), cses, csf)),
    ("qtEscBeg", s"$qt$qt${qt}abc$qt", List(csq, cses, csq('"'), csq('a'), csq('b'), csq('c'), cses, csf)),
    ("qtEscEnd", s"${qt}abc$qt$qt$qt", List(csq, csq('a'), csq('b'), csq('c'), cses, csq('"'), cses, csf)),
    ("qtSpIn", s"$qt ab c $qt", List(csq, csq(' '), csq('a'), csq('b'), csq(' '), csq('c'), csq(' '), cses, csf)),
    ("qtSepEndIn", s"${qt}abc$sep$qt", List(csq, csq('a'), csq('b'), csq('c'), csq(sep), cses, csf)),
    ("qtSepMidIn", s"${qt}ab${sep}c$qt", List(csq, csq('a'), csq('b'), csq(sep), csq('c'), cses, csf)),
    ("qtSepBegIn", s"$qt${sep}abc$qt", List(csq, csq(sep), csq('a'), csq('b'), csq('c'), cses, csf)),
    ("qtSepOut", s"$sep${qt}abc$qt$sep", List(csff, csq, csq('a'), csq('b'), csq('c'), cses, csff, csf)),
    ("qtRowEndIn", s"${qt}abc$rs$qt", List(csq, csq('a'), csq('b'), csq('c'), csq(rs), cses, csf)),
    ("qtRowMidIn", s"${qt}ab${rs}c$qt", List(csq, csq('a'), csq('b'), csq(rs), csq('c'), cses, csf)),
    ("qtRowBegIn", s"$qt${rs}abc$qt", List(csq, csq(rs), csq('a'), csq('b'), csq('c'), cses, csf)),
    ("qtRowOut", s"$rs${qt}abc$qt$rs", List(csfr, csq, csq('a'), csq('b'), csq('c'), cses, csfr, csf)),
    ("crlf", s"abc\r$rs", List(csr('a'), csr('b'), csr('c'), csr, csfr, csf)),
    ("cr", "ab\rc", List(csr('a'), csr('b'), csr, csr('c'), csf)),
    ("empty", "", List(csf))
  )

  private lazy val spaceNoTrimCases = Table(
    ("testCase", "input", "output"),
    ("spaces", "  ab c ", List(csr(' '), csr(' '), csr('a'), csr('b'), csr(' '), csr('c'), csr(' '), csf)),
    (
      "spSepRowEnd",
      s" \tab $sep c $rs",
      List(csr(' '), csr('\t'), csr('a'), csr('b'), csr(' '), csff, csr(' '), csr('c'), csr(' '), csfr, csf)
    ),
    ("spSepDbl", s"ab $sep${sep}c", List(csr('a'), csr('b'), csr(' '), csff, csff, csr('c'), csf))
  )

  private lazy val spaceTrimCases = Table(
    ("testCase", "input", "output"),
    ("spaces", "  ab c ", List(css, css, csr('a'), csr('b'), cst(' '), csr('c'), cst(' '), csf)),
    (
      "spSepRowEnd",
      s" \tab $sep c $rs",
      List(css, css('\t'), csr('a'), csr('b'), cst(' '), csff, css, csr('c'), cst(' '), csfr, csf)
    ),
    ("spSepDbl", s"ab $sep${sep}c", List(csr('a'), csr('b'), cst(' '), csff, csff, csr('c'), csf)),
    ("qtSpOut", s" ${qt}abc$qt  ", List(css, csq, csq('a'), csq('b'), csq('c'), cses, cse, cse, csf)),
    ("qtSpMix", s"${qt}abc $qt  ", List(csq, csq('a'), csq('b'), csq('c'), csq(' '), cses, cse, cse, csf))
  )

  private lazy val malformedCases = Table(
    ("testCase", "input", "output"),
    ("unquotedQuote", "a\"bc", List(csr('a'), cfcq)),
    ("unescapedQuote", "\"a\"bc\"", List(csq, csq('a'), cses, cfeq)),
    ("singleQuote", "\"abc", List(csq, csq('a'), csq('b'), csq('c'), cfmq)),
    ("onlyQuote", "\"", List(csq, cfmq))
  )

  private lazy val malformedNoTrimCases = Table(
    ("testCase", "input", "output"),
    ("unescapedQuoteSp", "\"ab\" c,", List(csq, csq('a'), csq('b'), cses, cfeq)),
    ("qtSpOut", s" ${qt}abc$qt  ", List(csr(' '), cfcq)),
    ("qtSpMix", s"${qt}abc $qt  ", List(csq, csq('a'), csq('b'), csq('c'), csq(' '), cses, cfeq))
  )

  private lazy val malformedTrimCases = Table(
    ("testCase", "input", "output"),
    ("unescapedQuoteSp", "\"ab\" c,", List(csq, csq('a'), csq('b'), cses, cse, cfeq))
  )
}
