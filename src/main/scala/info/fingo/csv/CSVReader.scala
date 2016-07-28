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
      parseRow(lines.next)
    }
  }

  private def parseRow(row: String) = {
    val fields = row.split(separator)
    new CSVRow(fields)
  }
}
