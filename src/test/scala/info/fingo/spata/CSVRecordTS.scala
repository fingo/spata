package info.fingo.spata

import java.util.Locale
import java.text.{DecimalFormat, DecimalFormatSymbols, NumberFormat}
import java.time.LocalDate
import java.time.format.{DateTimeFormatter, FormatStyle}

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
      val header: Map[String, Int] = Map("name" -> 0, "date" -> 1, "value" -> 2)
      val record = createRecord(name, sDate, sValue)(header)
      assert(record.getString("name") == name)
      assert(record.get[String]("name").contains(name))
      val dDate = record.get[LocalDate]("date")
      assert(dDate.contains(date))
      val bdValue = record.get[BigDecimal]("value")
      assert(bdValue.contains(value))
      val dValue = record.get[Double]("value")
      assert(dValue.contains(value.doubleValue))
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
        val header: Map[String, Int] = Map("num" -> 0, "date" -> 1, "value" -> 2)
        val record = createRecord(sNum, sDate, sValue)(header)
        val lNum = record.get[Long]("num", numFmt)
        assert(lNum.contains(num))
        val dDate = record.get[LocalDate]("date", dateFmt)
        assert(dDate.contains(date))
        val bdValue = record.get[BigDecimal]("value", valueFmt)
        assert(bdValue.contains(value))
    }
  }

  private def createRecord(name: String, date: String, value: String)(
    implicit header: Map[String, Int]
  ): CSVRecord =
    CSVRecord(Vector(name, date, value), 1, 1).toOption.get

  private lazy val basicCases = Table(
    ("testCase", "name", "sDate", "sValue"),
    ("basic", "Fanky Koval", "2020-02-22", "9999.99"),
    ("lineBreaks", "Fanky\nKoval", "2020-02-22", "9999.99"),
    ("spaces", "Fanky Koval", " 2020-02-22 ", " 9999.99 ")
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
}
