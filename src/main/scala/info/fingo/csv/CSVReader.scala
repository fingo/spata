package info.fingo.csv

import scala.collection.Iterator
import scala.io.Source

class CSVReader(source: Source, separator: Char) {

  private val lines = source.getLines

  // caption -> position
  implicit private val headerIndex: Map[String,Int] = {
    val headerLine = lines.next
    val captions = headerLine.split(separator)
    captions.zipWithIndex.toMap
  }
  var lineNum = 1
  var rowNum = 0

  val iterator: Iterator[CSVRow] = new Iterator[CSVRow] {
    def hasNext = {
      lines.hasNext
    }
    def next = {
      parseRow()
    }
  }

  private def parseRow() = {
    rowNum += 1
    val parser = new CSVLineParser(separator,rowNum)
    do {
      if(!lines.hasNext)
        throw new CSVException("Bad format: premature end of file (unmatched quotation?)","prematureEOF",lineNum,rowNum)
      lineNum += 1
      parser.parse(lines.next,lineNum)
    } while(!parser.finished)
    new CSVRow(parser.data,lineNum,rowNum)
  }
}
