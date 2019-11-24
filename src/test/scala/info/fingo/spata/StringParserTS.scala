package info.fingo.spata

import java.text.{DecimalFormat, NumberFormat}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.{DateTimeFormatter, FormatStyle}
import java.util.Locale

import org.scalatest.FunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class StringParserTS extends FunSuite with TableDrivenPropertyChecks {

  import StringParser._
  val locale = new Locale("pl", "PL")
  val nbsp = '\u00A0'

  test("StringParser should correctly parse strings") {
    forAll(strings) { (_: String, str: String, string: Option[String]) =>
      assert(parse[String](str) == string)
    }
  }

  test("StringParser should correctly parse ints") {
    forAll(ints) { (_: String, str: String, int: Option[Int]) =>
      assert(parse[Int](str) == int)
    }
  }

  test("StringParser should correctly parse longs") {
    forAll(longs) { (_: String, str: String, long: Option[Long], fmt: Option[NumberFormat]) =>
      val result = fmt match {
        case Some(f) => parse(str,f)
        case _ => parse[Long](str)
      }
      assert(result == long)
    }
  }

  test("StringParser should correctly parse doubles") {
    forAll(doubles) { (_: String, str: String, double: Option[Double], fmt: Option[DecimalFormat]) =>
      val result = fmt match {
        case Some(f) => parse[Double,DecimalFormat](str,f)
        case _ => parse[Double](str)
      }
      assert(result == double)
    }
  }

  test("StringParser should correctly parse big decimals") {
    forAll(decimals) { (_: String, str: String, decimal: Option[BigDecimal], fmt: Option[DecimalFormat]) =>
      val result = fmt match {
        case Some(f) => parse[BigDecimal,DecimalFormat](str,f)
        case _ => parse[BigDecimal](str)
      }
      assert(result == decimal)
    }
  }

  test("StringParser should correctly parse local dates") {
    forAll(localDates) { (_: String, str: String, date: Option[LocalDate], fmt: Option[DateTimeFormatter]) =>
      val result = fmt match {
        case Some(f) => parse[LocalDate,DateTimeFormatter](str,f)
        case _ => parse[LocalDate](str)
      }
      assert(result == date)
    }
  }

  test("StringParser should correctly parse local times") {
    forAll(localTimes) { (_: String, str: String, time: Option[LocalTime], fmt: Option[DateTimeFormatter]) =>
      val result = fmt match {
        case Some(f) => parse[LocalTime,DateTimeFormatter](str,f)
        case _ => parse[LocalTime](str)
      }
      assert(result == time)
    }
  }

  test("StringParser should correctly parse local date-times") {
    forAll(localDateTimes) { (_: String, str: String, dateTime: Option[LocalDateTime], fmt: Option[DateTimeFormatter]) =>
      val result = fmt match {
        case Some(f) => parse[LocalDateTime,DateTimeFormatter](str,f)
        case _ => parse[LocalDateTime](str)
      }
      assert(result == dateTime)
    }
  }

  test("StringParser should correctly parse booleans") {
    forAll(booleans) { (_: String, str: String, boolean: Option[Boolean], fmt: Option[BooleanFormatter]) =>
      val result = fmt match {
        case Some(f) => parse(str,f)
        case _ => parse[Boolean](str)
      }
      assert(result == boolean)
    }
  }

  val strings = Table(
    ("testCase","str","string"),
    ("basic", "lorem ipsum", Some("lorem ipsum")),
    ("empty", "", None)
  )

  val ints = Table(
    ("testCase","str","int"),
    ("basic", "123456789", Some(123456789)),
    ("negative", "-123456789", Some(-123456789)),
    ("empty", "", None)
  )

  val longs = Table(
    ("testCase","str","long","format"),
    ("basic", "123456789", Some(123456789L), None),
    ("locale", s"-123${nbsp}456${nbsp}789", Some(-123456789L), Some(NumberFormat.getInstance(locale))),
    ("empty", "", None, None)
  )

  val doubles = Table(
    ("testCase","str","double","format"),
    ("basic", "123456.789", Some(123456.789), None),
    ("integral", "123456789", Some(123456789.0), None),
    ("locale", s"-123${nbsp}456,789", Some(-123456.789), Some(NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat])),
    ("empty", "", None, None)
  )

  val decimals = Table(
    ("testCase","str","decimal","format"),
    ("basic", "123456.789", Some(BigDecimal(123456.789)), None),
    ("integral", "123456789", Some(BigDecimal(123456789)), None),
    ("locale", s"-123${nbsp}456,789", Some(BigDecimal(-123456.789)), Some(NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat])),
    ("empty", "", None, None)
  )

  val localDates = Table(
    ("testCase","str","date","format"),
    ("basic", "2020-02-29", Some(LocalDate.of(2020,2,29)), None),
    ("locale", "29.02.2020", Some(LocalDate.of(2020,2,29)), Some(DateTimeFormatter.ofLocalizedDate(FormatStyle.SHORT).withLocale(locale))),
    ("formatted", "29/02/2020", Some(LocalDate.of(2020,2,29)), Some(DateTimeFormatter.ofPattern("dd/MM/yyyy"))),
    ("empty", "", None, None)
  )

  val localTimes = Table(
    ("testCase","str","time","format"),
    ("basic", "12:34:56", Some(LocalTime.of(12,34,56)), None),
    ("milis", "12:34:56.789", Some(LocalTime.of(12,34,56, 789_000_000)), None),
    ("locale", "12:34", Some(LocalTime.of(12,34)), Some(DateTimeFormatter.ofLocalizedTime(FormatStyle.SHORT).withLocale(locale))),
    ("formatted", "12:34 PM", Some(LocalTime.of(12,34)), Some(DateTimeFormatter.ofPattern("hh:mm a"))),
    ("empty", "", None, None)
  )

  val localDateTimes = Table(
    ("testCase","str","datetime","format"),
    ("basic", "2020-02-29T12:34:56", Some(LocalDateTime.of(2020,2,29,12,34,56)), None),
    ("locale", "29.02.2020, 12:34", Some(LocalDateTime.of(2020,2,29,12,34)), Some(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).withLocale(locale))),
    ("formatted", "29/02/2020 12:34 PM", Some(LocalDateTime.of(2020,2,29,12,34)), Some(DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm a"))),
    ("empty", "", None, None)
  )

  val booleans = Table(
    ("testCase","str","boolean","format"),
    ("basic", "true", Some(true), None),
    ("locale", "FAŁSZ", Some(false), Some(BooleanFormatter("prawda", "fałsz", locale))),
    ("formatted", "y", Some(true), Some(BooleanFormatter("y", "n"))),
    ("empty", "", None, None)
  )}
