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
    var values = CSVLineParser.parse(lines.next,separator)
    while (!values.isComplete) {
      if(!lines.hasNext)
        throw new CSVException("Unclosed quotation")
      values = CSVLineParser.parse(lines.next, separator, values)
    }
    new CSVRow(values.data)
  }
}
