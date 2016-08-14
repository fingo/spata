package info.fingo.csv

import scala.collection.immutable.VectorBuilder

object CSVLineParser {
  val CSVEscapeChar = '"'

  class CSVLineParseResult() {
    var isInQuotes = false
    var wasQuote = false
    var field = ""
    val values = new VectorBuilder[String]()

    def isComplete = !isInQuotes
    def data = values.result()
  }

  def parse(line: String, separator: Char, result: CSVLineParseResult = new CSVLineParseResult): CSVLineParseResult = {
    for(char <- line) {
      char match {
        case `separator` =>
          if(!result.isInQuotes || result.wasQuote) {
            result.values += result.field
            result.field = ""
            result.isInQuotes = false
            result.wasQuote = false
          }
          else {
            result.field += char
            result.wasQuote = false
          }
        case CSVEscapeChar =>
          if(!result.isInQuotes && result.field == "") {
            result.isInQuotes = true
          }
          else if(result.isInQuotes) {
            if(result.wasQuote) {
              result.field += char
              result.wasQuote = false
            }
            else
              result.wasQuote = true
          }
          else
            throw new RuntimeException("bad format")
        case _ =>
          result.field += char
          result.wasQuote = false
      }
    }
    if(result.wasQuote)
      result.isInQuotes = false
    if(result.isInQuotes)
      result.field += "\n"
    else {
      result.values += result.field
      result.field = ""
    }
    result
  }
}
