package info.fingo.csv

import org.scalatest.FunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.io.Source

class CSVReaderTS extends FunSuite with TableDrivenPropertyChecks {

  val separators = Table("separator",',',';','\t')

  test("Reader should read basic csv data") {
    forAll(basicCases) { (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
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

  val basicCases = Table(
    ("testCase","firstName","firstValue","lastName","lastValue"),
    ("basic","Fanky Koval","100.00","Han Solo","999.99"),
    ("basic quoted","Koval, Fanky","100.00","Solo, Han","999.99"),
    ("mixed","Fanky Koval","100.00","Solo, Han","999.99"),
    ("spaces"," Fanky Koval "," "," "," 999.99 "),
    ("empty values","","","",""),
    ("double quotes","\"Fanky\" Koval","\"100.00\"","Solo, \"Han\"","999\".\"99"),
    ("line breaks","Fanky\nKoval","100.00","\nHan Solo","999.99\n"),
    ("empty lines","Fanky Koval","100.00","Han Solo","999.99")
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
            |"1"$s" Fanky Koval "${s}01.01.2001$s" "
            |"2"$s Eva Solo ${s}31.12.2012${s}123.45
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
      case "line breaks" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1$s"Fanky\nKoval"${s}01.01.2001${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3$s"\nHan Solo"${s}09.09.1999$s"999.99\n"""".stripMargin
      case "empty lines" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |
           |1${s}Fanky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99
           |""".stripMargin
      case _ => throw new RuntimeException("Unknown test case")
    }
    Source.fromString(csv)
  }

  test("Reader should clearly report errors in source data") {
    forAll(errorCases) { (testCase: String, errorCode: String, line: Option[Int], col: Option[Int], row: Option[Int], field: Option[String]) =>
      forAll(separators) { separator =>
        val source = generateErroneousCSV(testCase,separator)
        val reader = new CSVReader(source, separator)
        val it = reader.iterator
        val ex = intercept[CSVException] {
          it.next
        }
        assert(ex.messageCode == errorCode)
        assert(ex.line == line)
        assert(ex.col == col)
        assert(ex.row == row)
        assert(ex.field == field)
      }
    }
  }

  val errorCases = Table(
    ("testCase","errorCode","lineNum","colNum","rowNum","field"),
    ("missing value","valuesNumber",Some(2),None,Some(1),None),
    ("missing value with empty lines","valuesNumber",Some(3),None,Some(1),None),
    ("too many values","valuesNumber",Some(2),None,Some(1),None),
    ("unclosed quotation","wrongQuotation",Some(2),None,Some(1),None),
    ("unclosed quotation with empty lines","wrongQuotation",Some(3),None,Some(1),None),
    ("unescaped quotation","wrongQuotation",Some(2),None,Some(1),None),
    ("unmatched quotation","prematureEOF",Some(4),None,Some(1),None)
  )

  def generateErroneousCSV(testCase: String, separator: Char): Source = {
    val s = separator
    val csv = testCase match {
      case "missing value" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1${s}Fanky Koval${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "missing value with empty lines" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |
           |1${s}Fanky Koval${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "too many values" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1${s}Fanky Koval${s}XYZ${s}01.01.2001${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unclosed quotation" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1${s}Fanky" Koval${s}01.01.2001${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unclosed quotation with empty lines" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |
           |1${s}Fanky" Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unescaped quotation" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1$s"Fanky" Koval"${s}01.01.2001${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unmatched quotation" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
            |1$s"Fanky Koval${s}01.01.2001${s}100.00
            |2${s}Eva Solo${s}31.12.2012${s}123.45
            |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
    }
    Source.fromString(csv)
  }
}
