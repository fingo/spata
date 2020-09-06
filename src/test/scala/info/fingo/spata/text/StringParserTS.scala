/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.text

import java.text.{DecimalFormat, NumberFormat}
import java.time.format.{DateTimeFormatter, DateTimeParseException, FormatStyle}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.Locale

import info.fingo.spata.Maybe
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class StringParserTS extends AnyFunSuite with TableDrivenPropertyChecks {

  import StringParser._
  private val locale = new Locale("pl", "PL")
  private val nbsp = '\u00A0'
  private val empty = "empty"

  test("StringParser should correctly parse strings") {
    forAll(strings) { (tc: String, str: String, string: Option[String]) =>
      assert(parse[Option[String]](str) == string)
      assert(parseSafe[Option[String]](str).contains(string))
      if (tc != empty) {
        assert(string.contains(parse[String](str)))
        assert(parseSafe[String](str).toOption == string)
      }
    }
  }

  test("StringParser should correctly parse ints") {
    forAll(ints) { (tc: String, str: String, int: Option[Int]) =>
      assert(parse[Option[Int]](str) == int)
      assert(parseSafe[Option[Int]](str).contains(int))
      if (tc != empty) {
        assert(int.contains(parse[Int](str)))
        assert(parseSafe[Int](str).toOption == int)
      } else
        assert(parseSafe[Int](str).isLeft)
    }
  }

  test("StringParser should correctly parse longs") {
    forAll(longs) { (tc: String, str: String, long: Option[Long], fmt: Option[NumberFormat]) =>
      assertParsing(str, long, fmt, tc)
    }
  }

  test("StringParser should correctly parse doubles") {
    forAll(doubles) { (tc: String, str: String, double: Option[Double], fmt: Option[DecimalFormat]) =>
      assertParsing(str, double, fmt, tc)
    }
  }

  test("StringParser should correctly parse big decimals") {
    forAll(decimals) { (tc: String, str: String, decimal: Option[BigDecimal], fmt: Option[DecimalFormat]) =>
      assertParsing(str, decimal, fmt, tc)
    }
  }

  test("StringParser should correctly parse local dates") {
    forAll(dates) { (tc: String, str: String, date: Option[LocalDate], fmt: Option[DateTimeFormatter]) =>
      assertParsing(str, date, fmt, tc)
    }
  }

  test("StringParser should correctly parse local times") {
    forAll(times) { (tc: String, str: String, time: Option[LocalTime], fmt: Option[DateTimeFormatter]) =>
      assertParsing(str, time, fmt, tc)
    }
  }

  test("StringParser should correctly parse local date-times") {
    forAll(dateTimes) { (tc: String, str: String, dateTime: Option[LocalDateTime], fmt: Option[DateTimeFormatter]) =>
      assertParsing(str, dateTime, fmt, tc)
    }
  }

  test("StringParser should correctly parse booleans") {
    forAll(booleans) { (tc: String, str: String, boolean: Option[Boolean], fmt: Option[BooleanFormatter]) =>
      assertParsing(str, boolean, fmt, tc)
    }
  }

  private def assertParsing[A, B](str: String, expected: Option[A], fmt: Option[B], tc: String)(
    implicit p: FormattedStringParser[A, B]
  ) = {
    val result: Option[A] = fmt match {
      case Some(f) => parse[Option[A]](str, f)
      case _ => parse[Option[A]](str)
    }
    assert(result == expected)
    val maybeO: Maybe[Option[A]] = fmt match {
      case Some(f) => parseSafe[Option[A]](str, f)
      case _ => parseSafe[Option[A]](str)
    }
    assert(maybeO.contains(expected))
    if (tc != empty) {
      val result: A = fmt match {
        case Some(f) => parse[A](str, f)
        case _ => parse[A](str)
      }
      assert(expected.contains(result))
    }
    val maybe: Maybe[A] = fmt match {
      case Some(f) => parseSafe[A](str, f)
      case _ => parseSafe[A](str)
    }
    assert(maybe.toOption == expected)
  }

  test("String parser should throw exception on incorrect input") {
    assertThrows[DataParseException] { parse[Int]("wrong") }
    val exInt = intercept[DataParseException] { parse[Int]("12345678901234567890") }
    assert(exInt.dataType.contains("number"))
    assertThrows[DataParseException] { parse[Long]("wrong") }
    assertThrows[DataParseException] { parse[Long]("123:456:789", NumberFormat.getInstance(locale)) }
    val exDouble = intercept[DataParseException] { parse[Double]("123e1e2") }
    assert(exDouble.dataType.contains("number"))
    assertThrows[DataParseException] {
      parse[Double]("123,456.789", NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat])
    }
    assertThrows[DataParseException] {
      parse[BigDecimal]("123,456.789", NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat])
    }
    val exDate = intercept[DataParseException] { parse[LocalDate]("2020-02-30") }
    assert(exDate.dataType.contains("date/time"))
    assert(exDate.getCause.isInstanceOf[DateTimeParseException])
    assertThrows[DataParseException] {
      parse[LocalDate]("2020-02-28", DateTimeFormatter.ofPattern("dd/MM/yyyy"))
    }
    assertThrows[DataParseException] { parse[LocalTime]("24:24") }
    assertThrows[DataParseException] { parse[LocalDateTime]("wrong") }
    assertThrows[DataParseException] { parse[Boolean]("yes") }
    val exBool = intercept[DataParseException] { parse[Boolean]("yes", BooleanFormatter("y", "n")) }
    assert(exBool.dataType.contains("boolean"))
    assert(exBool.content == "yes")
    val exMessage = intercept[DataParseException] { parse[Int]("1234567890" * 10) }
    assert(exMessage.getMessage.endsWith(s"${DataParseException.infoCutSuffix} as requested number"))
  }

  private lazy val strings = Table(
    ("testCase", "str", "string"),
    ("basic", "lorem ipsum", Some("lorem ipsum")),
    (empty, "", None)
  )

  private lazy val ints = Table(
    ("testCase", "str", "int"),
    ("basic", "123456789", Some(123456789)),
    ("negative", "-123456789", Some(-123456789)),
    ("spaces", " 123456789 ", Some(123456789)),
    (empty, "", None)
  )

  private lazy val longs = Table(
    ("testCase", "str", "long", "format"),
    ("basic", "123456789", Some(123456789L), None),
    ("locale", s"-123${nbsp}456${nbsp}789", Some(-123456789L), Some(NumberFormat.getInstance(locale))),
    ("spaces", " 123456789 ", Some(123456789L), None),
    (empty, "", None, None)
  )

  private lazy val doubles = Table(
    ("testCase", "str", "double", "format"),
    ("basic", "123456.789", Some(123456.789), None),
    ("integral", "123456789", Some(123456789.0), None),
    (
      "locale",
      s"-123${nbsp}456,789",
      Some(-123456.789),
      Some(NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat])
    ),
    (
      "spaces",
      s" -123${nbsp}456,789 ",
      Some(-123456.789),
      Some(NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat])
    ),
    (empty, "", None, None)
  )

  private lazy val decimals = Table(
    ("testCase", "str", "decimal", "format"),
    ("basic", "123456.789", Some(BigDecimal(123456.789)), None),
    ("integral", "123456789", Some(BigDecimal(123456789)), None),
    (
      "locale",
      s"-123${nbsp}456,789",
      Some(BigDecimal(-123456.789)),
      Some(NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat])
    ),
    (empty, "", None, None)
  )

  private lazy val dates = Table(
    ("testCase", "str", "date", "format"),
    ("basic", "2020-02-29", Some(LocalDate.of(2020, 2, 29)), None),
    (
      "locale",
      "29.02.2020",
      Some(LocalDate.of(2020, 2, 29)),
      Some(DateTimeFormatter.ofLocalizedDate(FormatStyle.SHORT).withLocale(locale))
    ),
    ("formatted", "29/02/2020", Some(LocalDate.of(2020, 2, 29)), Some(DateTimeFormatter.ofPattern("dd/MM/yyyy"))),
    ("spaces", " 2020-02-29 ", Some(LocalDate.of(2020, 2, 29)), None),
    (empty, "", None, None)
  )

  private lazy val times = Table(
    ("testCase", "str", "time", "format"),
    ("basic", "12:34:56", Some(LocalTime.of(12, 34, 56)), None),
    ("milis", "12:34:56.789", Some(LocalTime.of(12, 34, 56, 789_000_000)), None),
    (
      "locale",
      "12:34",
      Some(LocalTime.of(12, 34)),
      Some(DateTimeFormatter.ofLocalizedTime(FormatStyle.SHORT).withLocale(locale))
    ),
    ("formatted", "12:34 PM", Some(LocalTime.of(12, 34)), Some(DateTimeFormatter.ofPattern("hh:mm a"))),
    ("spaces", " 12:34 PM ", Some(LocalTime.of(12, 34)), Some(DateTimeFormatter.ofPattern("hh:mm a"))),
    (empty, "", None, None)
  )

  private lazy val dateTimes = Table(
    ("testCase", "str", "datetime", "format"),
    ("basic", "2020-02-29T12:34:56", Some(LocalDateTime.of(2020, 2, 29, 12, 34, 56)), None),
    (
      "locale",
      "29.02.2020, 12:34",
      Some(LocalDateTime.of(2020, 2, 29, 12, 34)),
      Some(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).withLocale(locale))
    ),
    (
      "formatted",
      "29/02/2020 12:34 PM",
      Some(LocalDateTime.of(2020, 2, 29, 12, 34)),
      Some(DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm a"))
    ),
    (empty, "", None, Some(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).withLocale(locale)))
  )

  private lazy val booleans = Table(
    ("testCase", "str", "boolean", "format"),
    ("basic", "true", Some(true), None),
    ("locale", "FAŁSZ", Some(false), Some(BooleanFormatter("prawda", "fałsz", locale))),
    ("formatted", "y", Some(true), Some(BooleanFormatter("y", "n"))),
    (empty, "", None, None)
  )
}
