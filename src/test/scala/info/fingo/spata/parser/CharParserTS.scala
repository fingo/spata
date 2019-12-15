package info.fingo.spata.parser

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import fs2.Stream
import Config._
import CharStates._
import CharFailures._

class CharParserTS extends AnyFunSuite with TableDrivenPropertyChecks {

  private val parser = new CharParser(sep, rs, qt)

  test("Char parser should correctly parse provided input") {
    forAll(regularCases) { (_, input, output) =>
      val result = parse(input)
      assert(result == output)
    }
  }

  test("Char parser should return error info for malformed input") {
    forAll(malformedCases) { (_, input, output) =>
      val result = parse(input)
      assert(result == output)
    }
  }

  private def parse(input: String) = {
    val stream = Stream(input.toIndexedSeq: _*).through(parser.toCharResults())
    stream.compile.toList.unsafeRunSync()
  }

  private lazy val regularCases = Table(
    ("testCase", "input", "output"),
    ("basic", "abc", List(csr('a'), csr('b'), csr('c'), csfr)),
    ("sepEnd", s"abc$sep", List(csr('a'), csr('b'), csr('c'), csff, csfr)),
    ("sepMid", s"ab${sep}c", List(csr('a'), csr('b'), csff, csr('c'), csfr)),
    ("sepBeg", s"${sep}abc", List(csff, csr('a'), csr('b'), csr('c'), csfr)),
    ("rowEnd", s"abc$rs", List(csr('a'), csr('b'), csr('c'), csfr, csfr)),
    ("rowMid", s"ab${rs}c", List(csr('a'), csr('b'), csfr, csr('c'), csfr)),
    ("rowBeg", s"${rs}abc", List(csfr, csr('a'), csr('b'), csr('c'), csfr)),
    ("sepRowMidEnd", s"ab${sep}c$rs", List(csr('a'), csr('b'), csff, csr('c'), csfr, csfr)),
    ("sepRowEndEnd", s"abc$sep$rs", List(csr('a'), csr('b'), csr('c'), csff, csfr, csfr)),
    ("sepRowMidMid", s"ab$sep${rs}c", List(csr('a'), csr('b'), csff, csfr, csr('c'), csfr)),
    ("sepRowBegMid", s"${sep}ab${rs}c", List(csff, csr('a'), csr('b'), csfr, csr('c'), csfr)),
    ("sepRowBegBeg", s"$sep${rs}abc", List(csff, csfr, csr('a'), csr('b'), csr('c'), csfr)),
    ("sepRowSwap", s"abc$rs$sep", List(csr('a'), csr('b'), csr('c'), csfr, csff, csfr)),
    ("spaces", "  ab c ", List(css, css, csr('a'), csr('b'), cst(' '), csr('c'), cst(' '), csfr)),
    ("spSepRowEnd", s" ab $sep c $rs", List(css, csr('a'), csr('b'), cst(' '), csff, css, csr('c'), cst(' '), csfr, csfr)),
    ("spSepDbl", s"ab $sep${sep}c", List(csr('a'), csr('b'), cst(' '), csff, csff, csr('c'), csfr)),
    ("quote", s"${qt}abc$qt", List(csq, csq('a'), csq('b'), csq('c'), cses, csfr)),
    ("qtEscMid", s"${qt}ab$qt${qt}c$qt", List(csq, csq('a'), csq('b'), cses, csq('"'), csq('c'), cses, csfr)),
    ("qtEscBeg", s"$qt$qt${qt}abc$qt", List(csq, cses, csq('"'), csq('a'), csq('b'), csq('c'), cses, csfr)),
    ("qtEscEnd", s"${qt}abc$qt$qt$qt", List(csq, csq('a'), csq('b'), csq('c'), cses, csq('"'), cses, csfr)),
    ("qtSpIn", s"$qt ab c $qt", List(csq, csq(' '), csq('a'), csq('b'), csq(' '), csq('c'), csq(' '), cses, csfr)),
    ("qtSpOut", s" ${qt}abc$qt  ", List(css, csq, csq('a'), csq('b'), csq('c'), cses, cse, cse, csfr)),
    ("qtSpMix", s"${qt}abc $qt  ", List(csq, csq('a'), csq('b'), csq('c'), csq(' '), cses, cse, cse, csfr)),
    ("qtSepEndIn", s"${qt}abc$sep$qt", List(csq, csq('a'), csq('b'), csq('c'), csq(sep), cses, csfr)),
    ("qtSepMidIn", s"${qt}ab${sep}c$qt", List(csq, csq('a'), csq('b'), csq(sep), csq('c'), cses, csfr)),
    ("qtSepBegIn", s"$qt${sep}abc$qt", List(csq, csq(sep), csq('a'), csq('b'), csq('c'), cses, csfr)),
    ("qtSepOut", s"$sep${qt}abc$qt$sep", List(csff, csq, csq('a'), csq('b'), csq('c'), cses, csff, csfr)),
    ("qtRowEndIn", s"${qt}abc$rs$qt", List(csq, csq('a'), csq('b'), csq('c'), csq(rs), cses, csfr)),
    ("qtRowMidIn", s"${qt}ab${rs}c$qt", List(csq, csq('a'), csq('b'), csq(rs), csq('c'), cses, csfr)),
    ("qtRowBegIn", s"$qt${rs}abc$qt", List(csq, csq(rs), csq('a'), csq('b'), csq('c'), cses, csfr)),
    ("qtRowOut", s"$rs${qt}abc$qt$rs", List(csfr, csq, csq('a'), csq('b'), csq('c'), cses, csfr, csfr)),
    ("crlf", s"abc\r$rs", List(csr('a'), csr('b'), csr('c'), csr, csfr, csfr)),
    ("cr", "ab\rc", List(csr('a'), csr('b'), csr, csr('c'), csfr)),
    ("empty", "", List(csfr))
  )

  private lazy val malformedCases = Table(
    ("testCase", "input", "output"),
    ("unquotedQuote", "a\"bc", List(csr('a'), cfcq)),
    ("unescapedQuote", "\"a\"bc\"", List(csq, csq('a'), cses, cfeq)),
    ("unescapedQuoteSp", "\"ab\" c,", List(csq, csq('a'), csq('b'), cses, cse, cfeq)),
    ("singleQuote", "\"abc", List(csq, csq('a'), csq('b'), csq('c'), cfmq)),
    ("onlyQuote", "\"", List(csq, cfmq))
  )
}
