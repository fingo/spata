/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.text

import java.util.Locale
import java.text.{DecimalFormat, NumberFormat}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.{DateTimeFormatter, FormatStyle}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class StringRendererTS extends AnyFunSuite with TableDrivenPropertyChecks:

  import StringRenderer.*
  private val locale = new Locale("pl", "PL")
  private val nbsp = '\u00A0'

  test("StringRenderer should correctly render strings") {
    forAll(strings)((_: String, string: String, str: String) =>
      assert(render(string) == str)
      assert(render(Some(string)) == str)
    )
    assert(render(none[String]) == "")
    assert(render(None.orNull[String]) == "")
  }

  test("StringRenderer should correctly render ints") {
    forAll(ints)((_: String, int: Int, str: String) =>
      assert(render[Int](int) == str)
      assert(render(Some(int)) == str)
    )
    assert(render(none[Int]) == "")
  }

  test("StringRenderer should correctly render longs") {
    forAll(longs)((_: String, long: Long, fmt: Option[NumberFormat], str: String) => assertRendering(long, fmt, str))
  }

  test("StringRenderer should correctly render doubles") {
    forAll(doubles)((_: String, double: Double, fmt: Option[DecimalFormat], str: String) =>
      assertRendering(double, fmt, str)
    )
  }

  test("StringRenderer should correctly render big decimals") {
    forAll(decimals)((_: String, decimal: BigDecimal, fmt: Option[DecimalFormat], str: String) =>
      assertRendering(decimal, fmt, str)
    )
    assert(render(None.orNull[BigDecimal]) == "")
  }

  test("StringRenderer should correctly render local dates") {
    forAll(dates)((_: String, date: LocalDate, fmt: Option[DateTimeFormatter], str: String) =>
      assertRendering(date, fmt, str)
    )
    assert(render(None.orNull[LocalDate]) == "")
  }

  test("StringRenderer should correctly render local times") {
    forAll(times)((_: String, time: LocalTime, fmt: Option[DateTimeFormatter], str: String) =>
      assertRendering(time, fmt, str)
    )
    assert(render(None.orNull[LocalTime]) == "")
  }

  test("StringRenderer should correctly render local date-times") {
    forAll(dateTimes)((_: String, dateTime: LocalDateTime, fmt: Option[DateTimeFormatter], str: String) =>
      assertRendering(dateTime, fmt, str)
    )
    assert(render(None.orNull[LocalDateTime]) == "")
  }

  test("StringRenderer should correctly render booleans") {
    forAll(booleans)((_: String, boolean: Boolean, fmt: Option[BooleanFormatter], str: String) =>
      assertRendering(boolean, fmt, str)
    )
  }

  private def none[A]: Option[A] = Option.empty[A]

  private def assertRendering[A, B](value: A, fmt: Option[B], expected: String)(using
    r: FormattedStringRenderer[A, B]
  ) =
    val rr: String = fmt match
      case Some(f) => render(value, f)
      case _ => render(value)(using r)
    assert(rr == expected)
    val rrs: String = fmt match
      case Some(f) => render(Some(value), f)
      case _ => render(Some(value))
    assert(rrs == expected)
    val rrn: String = fmt match
      case Some(f) => render(none[A], f)
      case _ => render(none[A])
    assert(rrn == "")

  private lazy val strings = Table(
    ("testCase", "string", "str"),
    ("basic", "lorem ipsum", "lorem ipsum"),
    ("l18n", "żółw 忍者", "żółw 忍者"),
    ("empty", "", "")
  )

  private lazy val ints = Table(
    ("testCase", "int", "str"),
    ("basic", 123456789, "123456789"),
    ("negative", -123456789, "-123456789"),
    ("zero", 0, "0")
  )

  private lazy val longs = Table(
    ("testCase", "long", "format", "str"),
    ("basic", 123456789L, None, "123456789"),
    ("locale", -123456789L, Some(NumberFormat.getInstance(locale)), s"-123${nbsp}456${nbsp}789"),
    ("zero", 0L, None, "0")
  )

  private lazy val doubles = Table(
    ("testCase", "double", "format", "str"),
    ("basic", 123456.789, None, "123456.789"),
    ("integral", 1234567.00, None, "1234567.0"),
    ("locale", -123456.789, Some(NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat]), s"-123${nbsp}456,789"),
    ("zero", 0.0, None, "0.0")
  )

  private lazy val decimals = Table(
    ("testCase", "decimal", "format", "str"),
    ("basic", BigDecimal(123456.789), None, "123456.789"),
    ("integral", BigDecimal(123456789), None, "123456789"),
    (
      "locale",
      BigDecimal(-123456.789),
      Some(NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat]),
      s"-123${nbsp}456,789"
    ),
    ("zero", BigDecimal(0.0), None, "0.0"),
    ("integralZero", BigDecimal(0), None, "0")
  )

  private lazy val dates = Table(
    ("testCase", "date", "format", "str"),
    ("basic", LocalDate.of(2020, 2, 29), None, "2020-02-29"),
    (
      "locale",
      LocalDate.of(2020, 2, 29),
      Some(DateTimeFormatter.ofLocalizedDate(FormatStyle.SHORT).withLocale(locale)),
      "29.02.2020"
    ),
    ("formatted", LocalDate.of(2020, 2, 29), Some(DateTimeFormatter.ofPattern("dd/MM/yyyy")), "29/02/2020")
  )

  private lazy val times = Table(
    ("testCase", "time", "format", "str"),
    ("basic", LocalTime.of(12, 34, 56), None, "12:34:56"),
    ("milis", LocalTime.of(12, 34, 56, 789_000_000), None, "12:34:56.789"),
    (
      "locale",
      LocalTime.of(12, 34),
      Some(DateTimeFormatter.ofLocalizedTime(FormatStyle.SHORT).withLocale(locale)),
      "12:34"
    ),
    ("formatted", LocalTime.of(12, 34), Some(DateTimeFormatter.ofPattern("hh:mm a")), "12:34 PM")
  )

  private lazy val dateTimes = Table(
    ("testCase", "datetime", "format", "str"),
    ("basic", LocalDateTime.of(2020, 2, 29, 12, 34, 56), None, "2020-02-29T12:34:56"),
    (
      "locale",
      LocalDateTime.of(2020, 2, 29, 12, 34),
      Some(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).withLocale(locale)),
      "29.02.2020, 12:34"
    ),
    (
      "formatted",
      LocalDateTime.of(2020, 2, 29, 12, 34),
      Some(DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm a")),
      "29/02/2020 12:34 PM"
    )
  )

  private lazy val booleans = Table(
    ("testCase", "boolean", "format", "str"),
    ("basic", true, None, "true"),
    ("locale", false, Some(BooleanFormatter("prawda", "fałsz", locale)), "fałsz"),
    ("formatted", true, Some(BooleanFormatter("y", "n")), "y")
  )
