package info.fingo.spata

import info.fingo.spata.CSVReader.CSVCallback
import org.scalatest.FunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.io.Source

class CSVReaderTS extends FunSuite with TableDrivenPropertyChecks {

  val separators = Table("separator",',',';','\t')
  val maxFieldSize = Some(100)

  test("Reader should read basic csv data") {
    forAll(basicCases) { (testCase: String, firstName: String, firstValue: String, lastName: String, lastValue: String) =>
      forAll(separators) { separator =>
        val source = generateBasicCSV(testCase,separator)
        val reader = new CSVReader(separator, maxFieldSize)
        var count = 0
        val cb: CSVCallback = row => {
          count += 1
          row.rowNum match {
            case 1 =>
              assert(row.getString("NAME") == firstName)
              assert(row.getString("VALUE") == firstValue)
              true
            case 3 =>
              assert(row.getString("NAME") == lastName)
              assert(row.getString("VALUE") == lastValue)
              true
            case _ => true
          }
        }
        reader.read(source, cb)
        assert(count == 3)
      }
    }
  }

  test("Reader should clearly report errors in source data") {
    forAll(errorCases) { (testCase: String, errorCode: String, line: Option[Int], col: Option[Int], row: Option[Int], field: Option[String]) =>
      forAll(separators) { separator =>
        val source = generateErroneousCSV(testCase, separator)
        val reader = new CSVReader(separator, maxFieldSize)
        val ex = intercept[CSVException] {
          reader.read(source, _ => true)
        }
        assert(ex.messageCode == errorCode)
        assert(ex.line == line)
        assert(ex.col == col)
        assert(ex.row == row)
        assert(ex.field == field)
      }
    }
  }

  test("Reader should consume only required part of stream") {
    val cb: CSVCallback = row => if(row.row(0).startsWith("2")) false else true
    forAll(basicCases) { (testCase: String, _: String, _: String, _: String, _: String) =>
      forAll(separators) { separator =>
        val source = generateBasicCSV(testCase, separator)
        val reader = new CSVReader(separator, maxFieldSize)
        reader.read(source, cb)
        assert(source.hasNext)
      }
    }
  }

  val basicCases = Table(
    ("testCase","firstName","firstValue","lastName","lastValue"),
    ("basic","Fanky Koval","100.00","Han Solo","999.99"),
    ("basic quoted","Koval, Fanky","100.00","Solo, Han","999.99"),
    ("mixed","Fanky Koval","100.00","Solo, Han","999.99"),
    ("spaces"," Fanky Koval "," ","Han Solo"," 999.99 "),
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
            |"3"$s  Han Solo  ${s}09.09.1999$s" 999.99 """".stripMargin
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

  val errorCases = Table(
    ("testCase","errorCode","lineNum","colNum","rowNum","field"),
    ("missing value","valuesNumber",Some(2),None,Some(1),None),
    ("missing value with empty lines","valuesNumber",Some(3),None,Some(1),None),
    ("too many values","valuesNumber",Some(2),None,Some(1),None),
    ("unclosed quotation","unclosedQuotation",Some(2),Some(8),Some(1),Some("NAME")),
    ("unclosed quotation with empty lines","unclosedQuotation",Some(3),Some(8),Some(1),Some("NAME")),
    ("unescaped quotation","unescapedQuotation",Some(2),Some(9),Some(1),Some("NAME")),
    ("unmatched quotation","unmatchedQuotation",Some(2),Some(3),Some(1),Some("NAME")),
    ("unmatched quotation with trailing spaces","unmatchedQuotation",Some(2),Some(5),Some(1),Some("NAME")),
    ("unmatched quotation with escaped one","unmatchedQuotation",Some(2),Some(3),Some(1),Some("NAME")),
    ("field too long","fieldTooLong",Some(2),Some(103),Some(1),Some("NAME")),
    ("field too long through unmatched quotation","fieldTooLong",Some(3),Some(11),Some(1),Some("NAME"))
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
      case "unmatched quotation with trailing spaces" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1$s  "Fanky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "unmatched quotation with escaped one" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1$s"Fanky Koval""${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "field too long" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1${s}Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "field too long through unmatched quotation" =>
        s"""ID${s}NAME${s}DATE${s}VALUE
           |1$s"Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval Fanky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
    }
    Source.fromString(csv)
  }
}
