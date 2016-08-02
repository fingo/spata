package info.fingo.csv

import scala.collection.immutable.VectorBuilder

object CSVLineParser {
  val CSVEscapeChar = '"'

  def parse(line: String, separator: Char): IndexedSeq[String] = {
    var isInQuotes = false
    var wasQuote = false
    var field = ""
    var record = new VectorBuilder[String]()
    for(char <- line) {
      char match {
        case `separator` =>
          if(!isInQuotes || wasQuote) {
            record += field
            field = ""
            isInQuotes = false
            wasQuote = false
          }
          else {
            field += char
            wasQuote = false
          }
        case CSVEscapeChar =>
          if(!isInQuotes && field == "") {
            isInQuotes = true
          }
          else if(isInQuotes) {
            if(wasQuote) {
              field += char
              wasQuote = false
            }
            else
              wasQuote = true
          }
          else
            throw new RuntimeException("bad format")
        case _ =>
          field += char
          wasQuote = false
      }
    }
    record += field
    record.result()
  }
}
