/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import java.text.{DecimalFormat, DecimalFormatSymbols, NumberFormat}
import java.time.LocalDate
import java.time.format.{DateTimeFormatter, FormatStyle}
import java.util.Locale
import info.fingo.spata.Record.ProductOps
import info.fingo.spata.error.DataError
import info.fingo.spata.text.{StringParser, StringRenderer}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class RecordTS extends AnyFunSuite with TableDrivenPropertyChecks {

  private val locale = new Locale("pl", "PL")
  private val dfs = new DecimalFormatSymbols(locale)
  private val date = LocalDate.of(2020, 2, 22)
  private val value = BigDecimal(9999.99)
  private val num = -123456L
  private val nbsp = '\u00A0'

  test("Record allows retrieving individual values") {
    forAll(basicCases) { (_: String, name: String, sDate: String, sValue: String) =>
      val header = Header("name", "date", "value")
      val record = createRecord(name, sDate, sValue)(header)
      assert(record.size == 3)
      assert(record.toString == s"$name,$sDate,$sValue")
      assert(record("name").contains(name))
      assert(record.unsafe("name") == name)
      assert(record(0).contains(name))
      assert(record.unsafe(0) == name)
      assert(record.get[String]("name").contains(name))
      assert(record.unsafe.get[String]("name") == name)
      assert(record.get[LocalDate]("date").contains(date))
      assert(record.unsafe.get[LocalDate]("date") == date)
      assert(record.unsafe.get[BigDecimal]("value") == value)
      assert(record.unsafe.get[Double]("value") == value.doubleValue)
    }
  }

  test("Record allows retrieving optional values") {
    forAll(optionals) { (_: String, name: String, sDate: String, sValue: String) =>
      val header = Header("name", "date", "value")
      val record = createRecord(name, sDate, sValue)(header)
      assert(record.size == 3)
      assert(record.toString == s"$name,$sDate,$sValue")
      assert(record.get[Option[String]]("name").exists(_.forall(_ == name)))
      assert(record.unsafe.get[Option[String]]("name").forall(_ == name))
      assert(record.get[Option[LocalDate]]("date").exists(_.forall(_ == date)))
      assert(record.unsafe.get[Option[LocalDate]]("date").forall(_ == date))
      assert(record.get[Option[BigDecimal]]("value").exists(_.forall(_ == value)))
      assert(record.unsafe.get[Option[BigDecimal]]("value").forall(_ == value))
      assert(record.get[Option[Double]]("value").exists(_.forall(_ == value.doubleValue)))
      assert(record.unsafe.get[Option[Double]]("value").forall(_ == value.doubleValue))
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
        val header = Header("num", "date", "value")
        val record = createRecord(sNum, sDate, sValue)(header)
        assert(record.get[Long]("num", numFmt).contains(num))
        assert(record.unsafe.get[Long]("num", numFmt) == num)
        assert(record.get[LocalDate]("date", dateFmt).contains(date))
        assert(record.unsafe.get[LocalDate]("date", dateFmt) == date)
        assert(record.get[BigDecimal]("value", valueFmt).contains(value))
        assert(record.unsafe.get[BigDecimal]("value", valueFmt) == value)
    }
  }

  test("Record parsing may return error or throw exception") {
    forAll(incorrect) { (testCase: String, name: String, sDate: String, sValue: String) =>
      val header = Header("name", "date", "value")
      val record = createRecord(name, sDate, sValue)(header)
      val dtf = DateTimeFormatter.ofPattern("dd.MM.yy")
      if (testCase != "missingValue")
        assert(record.unsafe.get[String]("name") == name)
      else {
        assert(record.unsafe.get[String]("name") == "")
        assert(record.unsafe.get[Option[String]]("name").isEmpty)
        assert(record.unsafe.get[Option[LocalDate]]("date").isEmpty)
        assert(record.unsafe.get[Option[BigDecimal]]("value").isEmpty)
      }
      assert(record.get[LocalDate]("date").isLeft)
      assert(record.get[LocalDate]("wrong").isLeft)
      assert(record.get[LocalDate]("date", dtf).isLeft)
      assert(record.get[LocalDate]("wrong", dtf).isLeft)
      assertThrows[DataError] { record.unsafe.get[LocalDate]("date") }
      assertThrows[DataError] { record.unsafe.get[BigDecimal]("value") }
    }
  }

  test("Record may be converted to case class") {
    case class Data(name: String, value: Double, date: LocalDate)
    forAll(basicCases) { (_: String, name: String, sDate: String, sValue: String) =>
      val header = Header("name", "date", "value")
      val record = createRecord(name, sDate, sValue)(header)
      val md = record.to[Data]()
      assert(md.isRight)
      assert(md.contains(Data(name, value.doubleValue, date)))
    }
  }

  test("Record may be converted to case class with optional fields") {
    case class Data(name: String, value: Option[Double], date: Option[LocalDate])
    forAll(optionals) { (_: String, name: String, sDate: String, sValue: String) =>
      val header = Header("name", "date", "value")
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
        val header = Header("num", "date", "value")
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

  test("Converting record to case class yields Left[ContentError, _] on incorrect input") {
    case class Data(name: String, value: Double, date: LocalDate)
    forAll(incorrect) { (_: String, name: String, sDate: String, sValue: String) =>
      val header = Header("name", "date", "value")
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
      val header = Header("_1", "_2", "_3")
      val record = createRecord(name, sDate, sValue)(header)
      val md = record.to[Data]()
      assert(md.isRight)
      assert(md.contains((name, date, value)))
    }
  }

  test("Records may be created from sequence of string values") {
    forAll(basicCases) { (_: String, name: String, sDate: String, sValue: String) =>
      val header = Header("name", "date", "value")
      val record = Record(name, sDate, sValue)(header)
      assert(record.header == header)
      assert(record("name").contains(name))
      assert(record("value").contains(sValue))
    }
  }

  test("Header is shrunk or extended if its length does not match values length") {
    val header = Header("name", "date", "value")
    val name = "Funky Koval"
    val rs = Record(name, "01.01.2001")(header)
    assert(rs.header.size == 2)
    assert(rs("name").contains(name))
    val re = Record(name, "01.01.2001", "3.14", "0")(header)
    assert(re.header.size == 4)
    assert(re("name").contains(name))
    assert(re("_4").contains("0"))
  }

  test("Records may be created from string key-value pairs") {
    forAll(basicCases) { (_: String, name: String, sDate: String, sValue: String) =>
      val record = Record.fromPairs("name" -> name, "date" -> sDate, "value" -> sValue)
      assert(record.header.names == Header("name", "date", "value").names)
      assert(record("name").contains(name))
      assert(record("date").contains(sDate))
      assert(record("value").contains(sValue))
    }
  }

  test("Records may be created from list of values") {
    forAll(basicCases) { (_: String, name: String, sDate: String, sValue: String) =>
      val record = Record.fromValues(name, sDate, sValue)
      assert(record.header.names == Header("_1", "_2", "_3").names)
      assert(record("_1").contains(name))
      assert(record("_2").contains(sDate))
      assert(record("_3").contains(sValue))
    }
  }

  test("Records may be built from typed values") {
    forAll(basicCases) { (_: String, name: String, sDate: String, sValue: String) =>
      val record: Record =
        Record.builder
          .add("name", name)
          .add("date", LocalDate.parse(sDate.strip()))
          .add("value", sValue.strip().toDouble)
      assert(record.header.names == Header("name", "date", "value").names)
      assert(record("name").contains(name))
      assert(record("date").contains(sDate.strip()))
      assert(record("value").contains(sValue.strip()))
    }
  }

  test("Records may be created from case classes") {
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
        implicit val nsp: StringParser[Long] = (str: String) => numFmt.parse(str).longValue()
        implicit val ldsp: StringParser[LocalDate] = (str: String) => LocalDate.parse(str.strip, dateFmt)
        implicit val dsp: StringParser[BigDecimal] =
          (str: String) => valueFmt.parse(str).asInstanceOf[java.math.BigDecimal]
        val data = Data(
          StringParser.parse[Long](sNum).getOrElse(0L),
          StringParser.parse[BigDecimal](sValue).getOrElse(BigDecimal(0.0)),
          StringParser.parse[LocalDate](sDate).getOrElse(LocalDate.now())
        )
        implicit val lr: StringRenderer[Long] = (l: Long) => numFmt.format(l)
        implicit val dsr: StringRenderer[BigDecimal] = (bd: BigDecimal) => valueFmt.format(bd)
        implicit val ldsr: StringRenderer[LocalDate] = (ld: LocalDate) => dateFmt.format(ld)
        val record = data.toRecord()
        assert(record("num").contains(sNum.strip()))
        assert(record("date").contains(sDate.strip()))
        assert(record("value").contains(sValue.strip()))
    }
  }

  test("Records may be created from tuples") {
    type Data = (String, LocalDate, BigDecimal)
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
        implicit val nsp: StringParser[Long] = (str: String) => numFmt.parse(str).longValue()
        implicit val ldsp: StringParser[LocalDate] = (str: String) => LocalDate.parse(str.strip, dateFmt)
        implicit val dsp: StringParser[BigDecimal] =
          (str: String) => valueFmt.parse(str).asInstanceOf[java.math.BigDecimal]
        val data = (
          StringParser.parse[Long](sNum).getOrElse(0L),
          StringParser.parse[BigDecimal](sValue).getOrElse(BigDecimal(0.0)),
          StringParser.parse[LocalDate](sDate).getOrElse(LocalDate.now())
        )
        implicit val lr: StringRenderer[Long] = (l: Long) => numFmt.format(l)
        implicit val dsr: StringRenderer[BigDecimal] = (bd: BigDecimal) => valueFmt.format(bd)
        implicit val ldsr: StringRenderer[LocalDate] = (ld: LocalDate) => dateFmt.format(ld)
        val record = data.toRecord()
        assert(record("_1").contains(sNum.strip()))
        assert(record("_2").contains(sValue.strip()))
        assert(record("_3").contains(sDate.strip()))
    }
  }

  private def createRecord(name: String, date: String, value: String)(header: Header): Record =
    Record.create(Vector(name, date, value), 1, 1)(header).toOption.getOrElse(Record()(header))

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
      s"-123${nbsp}456",
      NumberFormat.getInstance(locale),
      "22.02.2020",
      DateTimeFormatter.ofLocalizedDate(FormatStyle.SHORT).withLocale(locale),
      s"9${nbsp}999,99",
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
      s"(123${nbsp}456)",
      new DecimalFormat("#,###;(#,###)", dfs),
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
    ("mixedData", "Funky Koval", "2020-02-30 foo", "9999.99 foo"),
    ("missingValue", "", "", "")
  )
}
