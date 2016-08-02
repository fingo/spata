package info.fingo.csv

import org.scalatest.FunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.io.Source

class CSVReaderTS extends FunSuite with TableDrivenPropertyChecks {

  test("Reader should read basic csv data") {
    forAll(cases) { (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
      forAll(separators) { separator =>
        val source = generateBasicCSV(testCase,separator)
        val reader = new CSVReader(source, separator)
        val it = reader.iterator
        assert(it.hasNext)
        val firstRow = it.next
        assert(firstRow.getString("NAME") == firstName)
        assert(firstRow.getString("VALUE") == firstValue)
        it.next
        val lastRow = it.next
        assert(lastRow.getString("NAME") == lastName)
        assert(lastRow.getString("VALUE") == lastValue)
        assert(!it.hasNext)
      }
    }
  }

  val separators = Table("separator",',',';','\t')
  val cases = Table(
    ("testCase","firstName","firstValue","lastName","lastValue"),
    ("basic","Fanky Koval","100.00","Han Solo","999.99"),
    ("basic quoted","Koval, Fanky","100.00","Solo, Han","999.99"),
    ("mixed","Fanky Koval","100.00","Solo, Han","999.99"),
    ("spaces"," Fanky Koval "," "," "," 999.99 "),
    ("empty values","","","",""),
    ("double quotes","\"Fanky\" Koval","\"100.00\"","Solo, \"Han\"","999\".\"99")
  )

  def generateBasicCSV(testCase: String, separator: Char): Source = {
    val s = separator
    val csv = testCase match {
      case "basic" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1${s}Fanky Koval${s}01.01.2001${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "basic quoted" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |"1"$s"Koval, Fanky"$s"01.01.2001"$s"100.00"
            |"2"$s"Solo, Eva"$s"31.12.2012"$s"123.45"
            |"3"$s"Solo, Han"$s"09.09.1999"$s"999.99"""".stripMargin
      case "mixed" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |"1"${s}Fanky Koval${s}01.01.2001$s"100.00"
            |"2"$s"Solo, Eva"$s"31.12.2012"$s"123.45"
            |"3"$s"Solo, Han"${s}09.09.1999${s}999.99""".stripMargin
      case "spaces" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |"1"$s Fanky Koval ${s}01.01.2001$s" "
            |"2"${s}Eva Solo${s}31.12.2012${s}123.45
            |"3"$s" "${s}09.09.1999$s" 999.99 """".stripMargin
      case "empty values" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |"1"$s${s}01.01.2001$s
            |"2"${s}Eva Solo${s}31.12.2012${s}123.45
            |"3"$s""${s}09.09.1999$s""""".stripMargin
      case "double quotes" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |"1"$s"^Fanky^ Koval"$s"01.01.2001"$s"^100.00^"
            |"2"$s"Solo, Eva"$s"31.12.2012"$s"123.45"
            |"3"$s"Solo, ^Han^"$s"09.09.1999"$s"999^.^99"""".stripMargin.replace("^","\"\"")
      case _ => throw new RuntimeException("Unknown test case")
    }
    Source.fromString(csv)
  }
}
