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

class RecordTS extends AnyFunSuite with TableDrivenPropertyChecks:

  private val locale = new Locale("pl", "PL")
  private val dfs = new DecimalFormatSymbols(locale)
  private val date = LocalDate.of(2020, 2, 22)
  private val value = BigDecimal(9999.99)
  private val num = -123456L
  private val nbsp = '\u00A0'

  test("record allows retrieving individual values") {
    val header = Header("name", "date", "value")
    forAll(basicCases)((_: String, name: String, sDate: String, sValue: String) =>
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
    )
  }

  test("record allows retrieving optional values") {
    val header = Header("name", "date", "value")
    forAll(optionals)((_: String, name: String, sDate: String, sValue: String) =>
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
    )
  }

  test("record allows retrieving formatted values") {
    val header = Header("num", "date", "value")
    forAll(formatted)(
      (
        _: String,
        sNum: String,
        numFmt: NumberFormat,
        sDate: String,
        dateFmt: DateTimeFormatter,
        sValue: String,
        valueFmt: DecimalFormat
      ) =>
        val record = createRecord(sNum, sDate, sValue)(header)
        assert(record.get[Long]("num", numFmt).contains(num))
        assert(record.unsafe.get[Long]("num", numFmt) == num)
        assert(record.get[LocalDate]("date", dateFmt).contains(date))
        assert(record.unsafe.get[LocalDate]("date", dateFmt) == date)
        assert(record.get[BigDecimal]("value", valueFmt).contains(value))
        assert(record.unsafe.get[BigDecimal]("value", valueFmt) == value)
    )
  }

  test("record parsing may return error or throw exception") {
    val header = Header("name", "date", "value")
    forAll(incorrect)((testCase: String, name: String, sDate: String, sValue: String) =>
      val record = createRecord(name, sDate, sValue)(header)
      val dtf = DateTimeFormatter.ofPattern("dd.MM.yy")
      if testCase != "missingValue" then assert(record.unsafe.get[String]("name") == name)
      else
        assert(record.unsafe.get[String]("name") == "")
        assert(record.unsafe.get[Option[String]]("name").isEmpty)
        assert(record.unsafe.get[Option[LocalDate]]("date").isEmpty)
        assert(record.unsafe.get[Option[BigDecimal]]("value").isEmpty)
      assert(record.get[LocalDate]("date").isLeft)
      assert(record.get[LocalDate]("wrong").isLeft)
      assert(record.get[LocalDate]("date", dtf).isLeft)
      assert(record.get[LocalDate]("wrong", dtf).isLeft)
      assertThrows[DataError](record.unsafe.get[LocalDate]("date"))
      assertThrows[DataError](record.unsafe.get[BigDecimal]("value"))
    )
  }

  test("record may be converted to case class") {
    case class Data(name: String, value: Double, date: LocalDate)
    val header = Header("name", "date", "value")
    forAll(basicCases)((_: String, name: String, sDate: String, sValue: String) =>
      val record = createRecord(name, sDate, sValue)(header)
      val md = record.to[Data]
      assert(md.isRight)
      assert(md.contains(Data(name, value.doubleValue, date)))
    )
  }

  test("record may be converted to case class with optional fields") {
    case class Data(name: String, value: Option[Double], date: Option[LocalDate])
    val header = Header("name", "date", "value")
    forAll(optionals)((_: String, name: String, sDate: String, sValue: String) =>
      val record = createRecord(name, sDate, sValue)(header)
      val md = record.to[Data]
      assert(md.isRight)
      assert(md.forall(_.name == name))
      if sValue.trim.isEmpty then assert(md.forall(_.value.isEmpty))
      else assert(md.forall(_.value.contains(value)))
    )
  }

  test("record may be converted to case class with custom formatting") {
    case class Data(num: Long, value: BigDecimal, date: LocalDate)
    val header = Header("num", "date", "value")
    forAll(formatted)(
      (
        _: String,
        sNum: String,
        numFmt: NumberFormat,
        sDate: String,
        dateFmt: DateTimeFormatter,
        sValue: String,
        valueFmt: DecimalFormat
      ) =>
        val record = createRecord(sNum, sDate, sValue)(header)
        given nsp: StringParser[Long] with
          def apply(str: String) = numFmt.parse(str).longValue()
        given ldsp: StringParser[LocalDate] with
          def apply(str: String) = LocalDate.parse(str.strip, dateFmt)
        given dsp: StringParser[BigDecimal] with
          def apply(str: String) = valueFmt.parse(str).asInstanceOf[java.math.BigDecimal]
        val md = record.to[Data]
        assert(md.isRight)
        assert(md.contains(Data(num, value, date)))
    )
  }

  test("converting record to case class yields Left[ContentError, _] on incorrect input") {
    case class Data(name: String, value: Double, date: LocalDate)
    val header = Header("name", "date", "value")
    forAll(incorrect)((_: String, name: String, sDate: String, sValue: String) =>
      val record = createRecord(name, sDate, sValue)(header)
      given ldsp: StringParser[LocalDate] with
        def apply(str: String) = LocalDate.parse(str.strip, DateTimeFormatter.ofPattern("dd.MM.yy"))
      val md = record.to[Data]
      assert(md.isLeft)
    )
  }

  test("converting record to case class yields Left[ContentError, _] for incorrect record structure") {
    case class Data(name: String, value: Double, date: LocalDate)
    val toSmall = Record.fromPairs(("name", "some name"))
    val tsConverted = toSmall.to[Data]
    assert(tsConverted.isLeft)
    val header = Header("name", "date", "bad")
    forAll(basicCases)((_: String, name: String, sDate: String, sValue: String) =>
      val badNames = createRecord(name, sDate, sValue)(header)
      val bnConverted = badNames.to[Data]
      assert(bnConverted.isLeft)
    )
  }

  test("record may be converted to tuples") {
    type Data = (String, LocalDate, BigDecimal)
    val header = Header("_1", "_2", "_3")
    forAll(basicCases)((_: String, name: String, sDate: String, sValue: String) =>
      val record = createRecord(name, sDate, sValue)(header)
      val md = record.to[Data]
      assert(md.isRight)
      assert(md.contains((name, date, value)))
    )
  }

  test("records may be created from sequence of string values") {
    val header = Header("name", "date", "value")
    forAll(basicCases)((_: String, name: String, sDate: String, sValue: String) =>
      val record = Record(name, sDate, sValue)(header)
      assert(record.header == header)
      assert(record("name").contains(name))
      assert(record("value").contains(sValue))
    )
  }

  test("header is shrunk or extended if its length does not match values length") {
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

  test("records may be created from string key-value pairs") {
    forAll(basicCases)((_: String, name: String, sDate: String, sValue: String) =>
      val record = Record.fromPairs("name" -> name, "date" -> sDate, "value" -> sValue)
      assert(record.header.names == Header("name", "date", "value").names)
      assert(record("name").contains(name))
      assert(record("date").contains(sDate))
      assert(record("value").contains(sValue))
    )
  }

  test("records may be created from list of values") {
    forAll(basicCases)((_: String, name: String, sDate: String, sValue: String) =>
      val record = Record.fromValues(name, sDate, sValue)
      assert(record.header.names == Header("_1", "_2", "_3").names)
      assert(record("_1").contains(name))
      assert(record("_2").contains(sDate))
      assert(record("_3").contains(sValue))
    )
  }

  test("records may be built from typed values") {
    forAll(basicCases)((_: String, name: String, sDate: String, sValue: String) =>
      val record: Record =
        Record.builder
          .add("name", name)
          .add("date", LocalDate.parse(sDate.strip()))
          .add("value", sValue.strip().toDouble)
      assert(record.header.names == Header("name", "date", "value").names)
      assert(record("name").contains(name))
      assert(record("date").contains(sDate.strip()))
      assert(record("value").contains(sValue.strip()))
    )
  }

  test("records may be created from case classes") {
    case class Data(num: Long, value: BigDecimal, date: LocalDate)
    forAll(formatted)(
      (
        _: String,
        sNum: String,
        numFmt: NumberFormat,
        sDate: String,
        dateFmt: DateTimeFormatter,
        sValue: String,
        valueFmt: DecimalFormat
      ) =>
        given nsp: StringParser[Long] with
          def apply(str: String) = numFmt.parse(str).longValue()
        given ldsp: StringParser[LocalDate] with
          def apply(str: String) = LocalDate.parse(str.strip, dateFmt)
        given dsp: StringParser[BigDecimal] with
          def apply(str: String) = valueFmt.parse(str).asInstanceOf[java.math.BigDecimal]
        val data = Data(
          StringParser.parse[Long](sNum).getOrElse(0L),
          StringParser.parse[BigDecimal](sValue).getOrElse(BigDecimal(0.0)),
          StringParser.parse[LocalDate](sDate).getOrElse(LocalDate.now())
        )
        given lr: StringRenderer[Long] with
          def apply(l: Long) = numFmt.format(l)
        given dsr: StringRenderer[BigDecimal] with
          def apply(bd: BigDecimal) = valueFmt.format(bd)
        given ldsr: StringRenderer[LocalDate] with
          def apply(ld: LocalDate) = dateFmt.format(ld)
        val record = data.toRecord
        assert(record("num").contains(sNum.strip()))
        assert(record("date").contains(sDate.strip()))
        assert(record("value").contains(sValue.strip()))
    )
  }

  test("records may be created from tuples") {
    type Data = (String, LocalDate, BigDecimal)
    forAll(formatted)(
      (
        _: String,
        sNum: String,
        numFmt: NumberFormat,
        sDate: String,
        dateFmt: DateTimeFormatter,
        sValue: String,
        valueFmt: DecimalFormat
      ) =>
        given nsp: StringParser[Long] with
          def apply(str: String) = numFmt.parse(str).longValue()
        given ldsp: StringParser[LocalDate] with
          def apply(str: String) = LocalDate.parse(str.strip, dateFmt)
        given dsp: StringParser[BigDecimal] with
          def apply(str: String) = valueFmt.parse(str).asInstanceOf[java.math.BigDecimal]
        val data = (
          StringParser.parse[Long](sNum).getOrElse(0L),
          StringParser.parse[BigDecimal](sValue).getOrElse(BigDecimal(0.0)),
          StringParser.parse[LocalDate](sDate).getOrElse(LocalDate.now())
        )
        given lr: StringRenderer[Long] with
          def apply(l: Long) = numFmt.format(l)
        given dsr: StringRenderer[BigDecimal] with
          def apply(bd: BigDecimal) = valueFmt.format(bd)
        given ldsr: StringRenderer[LocalDate] with
          def apply(ld: LocalDate) = dateFmt.format(ld)
        val record = data.toRecord
        assert(record("_1").contains(sNum.strip()))
        assert(record("_2").contains(sValue.strip()))
        assert(record("_3").contains(sDate.strip()))
    )
  }

  test("records may be updated by name or index") {
    val valBefore = "999.99"
    val valAfter = "111.11"
    val record: Record = Record.builder
      .add("name", "Moomin")
      .add("date", LocalDate.now().toString)
      .add("value", valBefore)
    assert(record("value").contains(valBefore))
    val updatedByKey = record.updated("value", valAfter)
    assert(updatedByKey("value").contains(valAfter))
    assert(updatedByKey("name") == record("name"))
    assert(updatedByKey("date") == record("date"))
    val updatedByIdx = record.updated(2, valAfter)
    assert(updatedByIdx("value").contains(valAfter))
    assert(updatedByIdx("name") == record("name"))
    assert(updatedByIdx("date") == record("date"))
    val unchanged = record.updated("incorrect", "something")
    assert(record == unchanged)
  }

  test("records may be updated with function") {
    val nameBefore = "Moomin"
    val nameAfter = "Moomin".toUpperCase
    val record: Record = Record.builder
      .add("name", nameBefore)
      .add("date", LocalDate.now().toString)
      .add("value", "999")
    assert(record("name").contains(nameBefore))
    val updatedByKey = record.updatedWith("name")(s => s.toUpperCase)
    assert(updatedByKey("name").contains(nameAfter))
    assert(updatedByKey("date") == record("date"))
    assert(updatedByKey("value") == record("value"))
    val updatedByIdx = record.updatedWith(0)(s => s.toUpperCase)
    assert(updatedByIdx("name").contains(nameAfter))
    assert(updatedByIdx("date") == record("date"))
    assert(updatedByIdx("value") == record("value"))
  }

  test("records may be updated without accessing header") {
    val record = Record.fromValues("1", "John Doe", "2001-01-01")
    val updated = record.updated(1, "Jane Doe")
    assert(record(1).contains("John Doe"))
    assert(updated(1).contains("Jane Doe"))
  }

  test("records may be modified using typed function") {
    val fun: Double => Double = x => 9 * x
    val valBefore = 111.11
    val valAfter = fun(valBefore)
    val now = LocalDate.now
    val record: Record = Record.builder
      .add("name", "Moomin")
      .add("date", now.toString)
      .add("value", StringRenderer.render(valBefore))
    assert(record.get[Double]("value").contains(valBefore))
    val altered = record.altered("value")(fun)
    assert(altered.isRight)
    altered.map(ar =>
      assert(ar.get[Double]("value").contains(valAfter))
      assert(ar("name") == record("name"))
      assert(ar("date") == record("date"))
    )
    val doubleAltered = for
      r1 <- record.altered("value")(fun)
      r2 <- r1.altered("date")((dt: LocalDate) => dt.minusDays(1))
    yield r2
    assert(doubleAltered.isRight)
    doubleAltered.map(ar =>
      assert(ar.get[Double]("value").contains(valAfter))
      assert(ar.get[LocalDate]("date").forall(_.isBefore(now)))
      assert(ar("name") == record("name"))
    )
  }

  test("records may be modified by adding or removing values through builder") {
    val name = "Moomin"
    val id = 1
    val valBefore = 111.11
    val valAfter = 999.99
    val record: Record = Record.builder
      .add("name", name)
      .add("date", LocalDate.now().toString)
      .add("value", StringRenderer.render(valBefore))
    assert(record.get[Double]("value").contains(valBefore))
    assert(record.get[Int]("id").isLeft)
    val altered = record.patch.remove("date").add("value", valAfter).add("id", id).get
    assert(altered.get[String]("name").contains(name))
    assert(altered.get[Int]("id").contains(id))
    assert(altered.get[LocalDate]("date").isLeft)
    assert(record.get[Double]("value").contains(valBefore)) // due to duplicated header
    assert(altered.size == record.size + 1) // due to duplicated header
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
