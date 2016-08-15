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

  val iterator: Iterator[CSVRow] = new Iterator[CSVRow] {
    def hasNext = {
      lines.hasNext
    }
    def next = {
      parseRow()
    }
  }

  private def parseRow() = {
    val parser = new CSVLineParser(separator)
    do {
      if(!lines.hasNext)
        throw new CSVException("Bad format: premature end of file (unclosed quotation?)")
      parser.parse(lines.next)
    } while(!parser.finished)
    new CSVRow(parser.data)
  }
}
