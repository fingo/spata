package info.fingo.spata

import java.util.Locale
import java.text.{DecimalFormat, NumberFormat}
import java.time.LocalDate
import java.time.format.{DateTimeFormatter, FormatStyle}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class CSVRecordTS extends AnyFunSuite with TableDrivenPropertyChecks {

  private val locale = new Locale("pl", "PL")

  test("Record allows retrieving individual values") {
    forAll(basicCases) { (_: String, name: String, sDate: String, sValue: String, lineNum: Int, rowNum: Int) =>
      val header: Map[String, Int] = Map("name" -> 0, "date" -> 1, "value" -> 2)
      val record = createRecord(name, sDate, sValue, lineNum, rowNum)(header)
      assert(record.getString("name") == name)
      assert(record.get[String]("name").contains(name))
      val dDate = record.get[LocalDate]("date")
      assert(dDate.isInstanceOf[Option[LocalDate]])
      assert(dDate.map(_.toString).contains(sDate.trim))
      val bdValue = record.get[BigDecimal]("value")
      assert(bdValue.isInstanceOf[Option[BigDecimal]])
      assert(bdValue.map(_.toString).contains(sValue.trim))
      val dValue = record.get[Double]("value")
      assert(dValue.isInstanceOf[Option[Double]])
      assert(dValue.map(_.toString).contains(sValue.trim))
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
        valueFmt: DecimalFormat,
        lineNum: Int,
        rowNum: Int
      ) =>
        val header: Map[String, Int] = Map("num" -> 0, "date" -> 1, "value" -> 2)
        val record = createRecord(sNum, sDate, sValue, lineNum, rowNum)(header)
        val lNum = record.get[Long, NumberFormat]("num", numFmt)
        assert(lNum.isInstanceOf[Option[Long]])
        assert(lNum.map(_.toString).contains(sNum.trim))
    }
  }

  private def createRecord(name: String, date: String, value: String, lineNum: Int, rowNum: Int)(
    implicit header: Map[String, Int]
  ): CSVRecord =
    CSVRecord(Vector(name, date, value), lineNum, rowNum).toOption.get

  private lazy val basicCases = Table(
    ("testCase", "name", "date", "value", "lineNum", "rowNum"),
    ("basic", "Fanky Koval", "2020-02-02", "999.99", 1, 1),
    ("lineBreaks", "Fanky\nKoval", "2020-02-02", "999.99", 3, 4),
    ("spaces", "Fanky Koval", " 2020-02-02 ", " 999.99 ", 1, 1)
  )

  private lazy val formatted = Table(
    ("testCase", "num", "numFmt", "date", "dateFmt", "value", "valueFmt", "lineNum", "rowNum"),
    (
      "locale",
      "-123456",
      NumberFormat.getInstance(locale),
      "31.03.2020",
      DateTimeFormatter.ofLocalizedDate(FormatStyle.SHORT).withLocale(locale),
      "999,99",
      NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat],
      1,
      1
    ),
    ( // TODO
      "format",
      "-123456",
      NumberFormat.getInstance(locale),
      "31.03.2020",
      DateTimeFormatter.ofLocalizedDate(FormatStyle.SHORT).withLocale(locale),
      "999,99",
      NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat],
      1,
      1
    )
  )
}
