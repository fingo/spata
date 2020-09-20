/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.text

import java.util.Locale
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class BooleanFormatterTS extends AnyFunSuite with TableDrivenPropertyChecks {

  private val locale = new Locale("pl", "PL")

  test("Boolean formatter correctly formats boolean values") {
    forAll(formatters) { (_: String, tt: String, ft: String, formatter: BooleanFormatter) =>
      assert(formatter.format(true) == tt.toLowerCase(locale))
      assert(formatter.format(false) == ft.toLowerCase(locale))
    }
  }

  test("Boolean formatter correctly parses boolean values") {
    forAll(formatters) { (_: String, tt: String, ft: String, formatter: BooleanFormatter) =>
      assert(formatter.parse(tt))
      assert(!formatter.parse(ft))
    }
  }
  test("Boolean formatter should throw ParseError when parsing incorrect input") {
    forAll(formatters) { (_: String, _: String, _: String, formatter: BooleanFormatter) =>
      assertThrows[ParseError] { formatter.parse("wrong") }
      assertThrows[ParseError] { formatter.parse("y") }
    }
  }

  private lazy val formatters = Table(
    ("testCase", "true", "false", "formatter"),
    ("basic", "true", "false", BooleanFormatter("true", "false")),
    ("simple", "yes", "no", BooleanFormatter("yes", "no")),
    ("locale", "PRAWDA", "FAŁSZ", BooleanFormatter("prawda", "fałsz", locale)),
    ("default", "true", "false", BooleanFormatter.default)
  )

}
