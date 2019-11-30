package info.fingo.spata

import java.time.LocalDate

import org.scalatest.FunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class CSVRecordTS extends FunSuite with TableDrivenPropertyChecks {

  private implicit val header: Map[String,Int] = Map("name" -> 0, "date" -> 1, "value" -> 2)

  test("Record allows retrieving individual values") {
    forAll(basicCases) { (_: String, name: String, date: String, value: String, lineNum: Int, rowNum: Int) =>
      val record = createRecord(name,date,value,lineNum,rowNum)
      assert(record.getString("name") == name)
      assert(record.get[LocalDate]("date").contains(LocalDate.parse(date)))
      assert(record.get[BigDecimal]("value").contains(BigDecimal(value)))
    }
  }

  private def createRecord(name: String, date: String, value: String, lineNum: Int, rowNum: Int): CSVRecord =
    CSVRecord(Vector(name, date, value), lineNum, rowNum).toOption.get

  private lazy val basicCases = Table(
    ("testCase","name","date","value","lineNum","rowNum"),
    ("basic","Fanky Koval","2020-02-02","999.99", 1, 1),
    ("lineBreaks","Fanky\nKoval","2020-02-02","999.99", 3, 4)
  )
}
