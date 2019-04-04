package info.fingo.csv

import scala.collection.Iterator
import scala.io.Source

class CSVReader(source: Source, separator: Char) {

  private val parser = Parser.builder(source).fieldDelimiter(separator).build()

  // caption -> position
  implicit private val headerIndex: Map[String,Int] = {
    val captions = parser.next().fields
    captions.zipWithIndex.toMap
  }
  var lineNum = 1
  var rowNum = 0

  val iterator: Iterator[CSVRow] = new Iterator[CSVRow] {
    def hasNext: Boolean = interceptParserException(parser.hasNext)
    def next: CSVRow = {
      wrapRow()
    }
  }

  private def wrapRow() = {
    val rawRow = interceptParserException(parser.next())
    rowNum += 1
    lineNum += rawRow.numOfLines
    val row = new CSVRow(rawRow.fields, lineNum, rowNum)
    row
  }

  private def interceptParserException[A](f: => A): A = {
    try {
      f
    }
    catch {
      case ex: CSVException =>
        throw new CSVException(ex.message, ex.messageCode, ex.line.orElse(Some(1)).map(_ + lineNum), ex.col, Some(rowNum + 1), None)
    }
  }
}
