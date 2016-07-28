package info.fingo.csv

import org.scalatest.FunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.io.Source

class CSVReaderTS extends FunSuite with TableDrivenPropertyChecks {

  val separators = Table("separator",',',';','\t')

  test("Reader should read basic csv data") {
    forAll(separators) { separator =>
      val source = generateBasicCSV(separator)
      val reader = new CSVReader(source,separator)
      val it = reader.iterator
      assert(it.hasNext)
      val firstRow = it.next
      assert(firstRow.getString("NAME") == "Josh")
    }
  }

  def generateBasicCSV(separator: Char): Source = {
    val s = separator
    val csv = s"""ID${s}NAME${s}DATE${s}VALUE
      |1${s}Josh${s}01.01.2001${s}100.00
      |2${s}Eva${s}31.12.2012${s}123.45
      |3${s}Nick${s}09.09.1999${s}999.99""".stripMargin
    Source.fromString(csv)
  }
}
