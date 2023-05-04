/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.parser

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import CharParser.CharResult
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import Config.*
import CharStates.*
import CharFailures.*
import RawFields.*
import FieldFailures.*

class FieldParserTS extends AnyFunSuite with TableDrivenPropertyChecks:

  private val parser = new FieldParser[IO](Some(limit))

  test("Field parser should correctly parse provided input"):
    forAll(regularCases): (_, input, output) =>
      val result = parse(input)
      assert(result == output)

  test("Field parser should correctly report malformed input"):
    forAll(failureCases): (_, input, output) =>
      val result = parse(input)
      assert(result == output)

  private def parse(input: List[CharResult]) =
    val stream = Stream(input*).through(parser.toFields)
    stream.compile.toList.unsafeRunSync()

  private lazy val regularCases = Table(
    ("testCase", "input", "output"),
    ("basic", List(csr('a'), csr('b'), csr('c'), csfr), List(rfe("abc", 3))),
    ("sepEnd", List(csr('a'), csr('b'), csr('c'), csff), List(rf("abc", 3))),
    ("sepBeg", List(csff, csr('a'), csr('b'), csr('c')), List(rf("", 0))),
    ("sepMid", List(csr('a'), csr('b'), csff, csr('c')), List(rf("ab", 2))),
    ("sepMidRow", List(csr('a'), csr('b'), csff, csr('c'), csfr), List(rf("ab", 2), rfe("c", 4))),
    (
      "sepMidRow2",
      List(csfr, csr('a'), csr('b'), csff, csr('c'), csfr),
      List(rfe("", 0), rf("ab", 2, 2), rfe("c", 4, 2))
    ),
    ("regular", List(csr('a'), csff, csr('b'), csff, csr('c'), csfr), List(rf("a", 1), rf("b", 3), rfe("c", 5))),
    ("sepOnly", List(csff), List(rf("", 0))),
    ("sep3", List(csff, csff, csff), List(rf("", 0), rf("", 1), rf("", 2))),
    ("eorOnly", List(csfr), List(rfe("", 0))),
    ("eor3", List(csfr, csfr, csfr), List(rfe("", 0), rfe("", 0, 2), rfe("", 0, 3))),
    ("spaces", List(csr('a'), csr(' '), csff, csr(' '), csr('b'), csr('c'), csfr), List(rf("a ", 2), rfe(" bc", 6))),
    ("endsStarts", List(csr('a'), cse, csff, css, csr('b'), csr('c'), csfr), List(rf("a", 2), rfe("bc", 6))),
    ("trailing", List(csr('a'), cst(' '), csff, csr('b'), cst(' '), csr('c'), csfr), List(rf("a", 2), rfe("b c", 6))),
    ("noChar", List(csr('a'), csr, csr('b'), csr('c'), csfr), List(rfe("abc", 4))),
    ("quotes", List(csq, csq('a'), csq('b'), csq('c'), cses, csff), List(rf("abc", 5))),
    ("qtEsc", List(csq, csq('a'), csq('b'), cses, csq(qt), csq('c'), cses, csff), List(rf(s"ab${qt}c", 7))),
    ("qtMix", List(csq, csq('a'), csq('b'), cses, csff, csr('c'), csff), List(rf("ab", 4), rf("c", 6))),
    ("qtSpaces", List(css, csq, csq('a'), csq('b'), csq('c'), cses, cse, csff), List(rf("abc", 7))),
    ("qtSep", List(csq, csq('a'), csq('b'), csq(sep), csq('c'), cses, csff), List(rf(s"ab${sep}c", 6))),
    ("qtRow", List(csq, csq('a'), csq('b'), csq(rs), csq('c'), cses, csff), List(rf(s"ab${rs}c", 2, 2))),
    ("atLimit", List.fill(limit)(csr('a')) :+ csff, List(rf("a" * limit, limit))),
    ("noSep", List(csr('a'), csr('b'), csr('c')), List()),
    ("empty", List(), List())
  )
  private lazy val failureCases = Table(
    ("testCase", "input", "output"),
    ("qtUnclosed", List(csr('a'), csr('b'), cfcq), List(ffcq(3))),
    ("qtUnescaped", List(csq, csq('a'), csq('b'), css, cfeq), List(ffeq(4))),
    ("qtUnmatched", List(csq, csq('a'), csq('b'), csq('c'), cfmq), List(ffmq(1))),
    ("qtUnmatchedNl", List(csq, csq('a'), csq('b'), csq(nl), csq('c'), cfmq), List(ffmq(1))),
    ("qtUnmatchedSp", List(css, csq, csq('a'), csq('b'), csq('c'), cfmq), List(ffmq(2))),
    ("regQtUnclosed", List(csr('a'), csr('b'), csff, csr('c'), cfcq), List(rf("ab", 2), ffcq(5))),
    ("regQtUnescaped", List(csr('a'), csff, csq, csq('b'), csq('c'), css, cfeq), List(rf("a", 1), ffeq(6))),
    ("regQtUnmatched", List(csq('a'), csff, csq, csq('b'), csq('c'), cfmq), List(rf("a", 1), ffmq(3))),
    ("fieldToLong", List.fill(limit + 1)(csr('a')) :+ csff, List(ffrtl(limit + 1)))
  )
