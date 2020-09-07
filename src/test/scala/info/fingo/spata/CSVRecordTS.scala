/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import java.util.Locale
import java.text.{DecimalFormat, DecimalFormatSymbols, NumberFormat}
import java.time.LocalDate
import java.time.format.{DateTimeFormatter, FormatStyle}

import info.fingo.spata.text.StringParser
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class CSVRecordTS extends AnyFunSuite with TableDrivenPropertyChecks {

  private val locale = new Locale("pl", "PL")
  private val dfs = new DecimalFormatSymbols(locale)
  private val date = LocalDate.of(2020, 2, 22)
  private val value = BigDecimal(9999.99)
  private val num = -123456L
  private val nbsp = '\u00A0'

  test("Record allows retrieving individual values") {
    forAll(basicCases) { (_: String, name: String, sDate: String, sValue: String) =>
      val header = CSVHeader("name", "date", "value")
      val record = createRecord(name, sDate, sValue)(header)
      assert(record.size == 3)
      assert(record.toString == s"$name,$sDate,$sValue")
      assert(record("name") == name)
      assert(record(0) == name)
      assert(record.get[String]("name") == name)
      assert(record.seek[String]("name").contains(name))
      assert(record.get[LocalDate]("date") == date)
      assert(record.seek[LocalDate]("date").contains(date))
      assert(record.get[BigDecimal]("value") == value)
      assert(record.get[Double]("value") == value.doubleValue)
    }
  }

  test("Record allows retrieving optional values") {
    forAll(optionals) { (_: String, name: String, sDate: String, sValue: String) =>
      val header = CSVHeader("name", "date", "value")
      val record = createRecord(name, sDate, sValue)(header)
      assert(record.size == 3)
      assert(record.toString == s"$name,$sDate,$sValue")
      assert(record.get[Option[String]]("name").forall(_ == name))
      assert(record.get[Option[LocalDate]]("date").forall(_ == date))
      assert(record.get[Option[BigDecimal]]("value").forall(_ == value))
      assert(record.seek[Option[BigDecimal]]("value").exists(_.forall(_ == value)))
      assert(record.get[Option[Double]]("value").forall(_ == value.doubleValue))
      assert(record.seek[Option[Double]]("value").exists(_.forall(_ == value.doubleValue)))
    }
  }

  test("Record allows retrieving formatted values") {
    forAll(formatted) {
      (
        _: String,
        sNum: String,
        numFmt: NumberFormat,
        sDate: String,
        dateFmt: DateTimeFormatter,
        sValue: String,
        valueFmt: DecimalFormat
      ) =>
        val header = CSVHeader("num", "date", "value")
        val record = createRecord(sNum, sDate, sValue)(header)
        assert(record.get[Long]("num", numFmt) == num)
        assert(record.seek[Long]("num", numFmt).contains(num))
        assert(record.get[LocalDate]("date", dateFmt) == date)
        assert(record.get[BigDecimal]("value", valueFmt) == value)
    }
  }

  test("Record parsing may throw exception") {
    forAll(incorrect) { (testCase: String, name: String, sDate: String, sValue: String) =>
      val header = CSVHeader("name", "date", "value")
      val record = createRecord(name, sDate, sValue)(header)
      val dtf = DateTimeFormatter.ofPattern("dd.MM.yy")
      if (testCase != "missingValue")
        assert(record.get[String]("name") == name)
      else {
        assert(record.get[String]("name") == "")
        assert(record.get[Option[String]]("name").isEmpty)
        assert(record.get[Option[LocalDate]]("date").isEmpty)
        assert(record.get[Option[BigDecimal]]("value").isEmpty)
      }
      assertThrows[CSVDataException] { record.get[LocalDate]("date") }
      assert(record.seek[LocalDate]("date").isLeft)
      assert(record.seek[LocalDate]("wrong").isLeft)
      assert(record.seek[LocalDate]("date", dtf).isLeft)
      assert(record.seek[LocalDate]("wrong", dtf).isLeft)
      assertThrows[CSVDataException] { record.get[BigDecimal]("value") }
    }
  }

  test("Record may be converted to case class") {
    case class Data(name: String, value: Double, date: LocalDate)
    forAll(basicCases) { (_: String, name: String, sDate: String, sValue: String) =>
      val header = CSVHeader("name", "date", "value")
      val record = createRecord(name, sDate, sValue)(header)
      val md = record.to[Data]()
      assert(md.isRight)
      assert(md.contains(Data(name, value.doubleValue, date)))
    }
  }

  test("Record may be converted to case class with optional fields") {
    case class Data(name: String, value: Option[Double], date: Option[LocalDate])
    forAll(optionals) { (_: String, name: String, sDate: String, sValue: String) =>
      val header = CSVHeader("name", "date", "value")
      val record = createRecord(name, sDate, sValue)(header)
      val md = record.to[Data]()
      assert(md.isRight)
      assert(md.forall(_.name == name))
      if (sValue.trim.isEmpty)
        assert(md.forall(_.value.isEmpty))
      else
        assert(md.forall(_.value.contains(value)))
    }
  }

  test("Record may be converted to case class with custom formatting") {
    case class Data(num: Long, value: BigDecimal, date: LocalDate)
    forAll(formatted) {
      (
        _: String,
        sNum: String,
        numFmt: NumberFormat,
        sDate: String,
        dateFmt: DateTimeFormatter,
        sValue: String,
        valueFmt: DecimalFormat
      ) =>
        val header = CSVHeader("num", "date", "value")
        val record = createRecord(sNum, sDate, sValue)(header)
        implicit val nsp: StringParser[Long] = (str: String) => numFmt.parse(str).longValue()
        implicit val ldsp: StringParser[LocalDate] = (str: String) => LocalDate.parse(str.strip, dateFmt)
        implicit val dsp: StringParser[BigDecimal] =
          (str: String) => valueFmt.parse(str).asInstanceOf[java.math.BigDecimal]
        val md = record.to[Data]()
        assert(md.isRight)
        assert(md.contains(Data(num, value, date)))
    }
  }

  test("Converting record to case class yields Left[Throwable, _] on incorrect input") {
    forAll(incorrect) { (_: String, name: String, sDate: String, sValue: String) =>
      case class Data(name: String, value: Double, date: LocalDate)
      val header = CSVHeader("name", "date", "value")
      val record = createRecord(name, sDate, sValue)(header)
      implicit val ldsp: StringParser[LocalDate] =
        (str: String) => LocalDate.parse(str.strip, DateTimeFormatter.ofPattern("dd.MM.yy"))
      val md = record.to[Data]()
      assert(md.isLeft)
    }
  }

  test("Record may be converted to tuples") {
    type Data = (String, LocalDate, BigDecimal)
    forAll(basicCases) { (_: String, name: String, sDate: String, sValue: String) =>
      val header = CSVHeader("_1", "_2", "_3")
      val record = createRecord(name, sDate, sValue)(header)
      val md = record.to[Data]()
      assert(md.isRight)
      assert(md.contains((name, date, value)))
    }
  }

  private def createRecord(name: String, date: String, value: String)(header: CSVHeader): CSVRecord =
    CSVRecord(Vector(name, date, value), 1, 1)(header).toOption.get

  private lazy val basicCases = Table(
    ("testCase", "name", "sDate", "sValue"),
    ("basic", "Funky Koval", "2020-02-22", "9999.99"),
    ("lineBreaks", "Funky\nKoval", "2020-02-22", "9999.99"),
    ("spaces", "Funky Koval", " 2020-02-22 ", " 9999.99 ")
  )

  private lazy val optionals = Table(
    ("testCase", "name", "sDate", "sValue"),
    ("basic", "Funky Koval", "2020-02-22", "9999.99"),
    ("spaces", "Funky Koval", " ", " "),
    ("empty", "", "", "")
  )

  private lazy val formatted = Table(
    ("testCase", "sNum", "numFmt", "sDate", "dateFmt", "sValue", "valueFmt"),
    (
      "locale",
      "-123456",
      NumberFormat.getInstance(locale),
      "22.02.2020",
      DateTimeFormatter.ofLocalizedDate(FormatStyle.SHORT).withLocale(locale),
      "9999,99",
      NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat]
    ),
    (
      "format",
      "-123,456",
      new DecimalFormat("#,###"),
      "22.02.20",
      DateTimeFormatter.ofPattern("dd.MM.yy"),
      "9,999.990",
      new DecimalFormat("#,###.000")
    ),
    (
      "formatLocale",
      s"-123${nbsp}456",
      new DecimalFormat("#,###", dfs),
      "22.02.20",
      DateTimeFormatter.ofPattern("dd.MM.yy", locale),
      s"9${nbsp}999,990",
      new DecimalFormat("#,###.000", dfs)
    )
  )

  private lazy val incorrect = Table(
    ("testCase", "name", "sDate", "sValue"),
    ("wrongFormat", "Funky Koval", "2020-02-30", "9999,99"),
    ("wrongType", "2020-02-22", "Funky Koval", "true"),
    ("missingValue", "", "", "")
  )
}
