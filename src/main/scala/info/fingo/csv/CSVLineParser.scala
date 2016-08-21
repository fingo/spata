package info.fingo.csv

import scala.collection.immutable.VectorBuilder

private[csv] class CSVLineParser(val separator: Char, val rowNum: Int) {
  val CSVEscapeChar = '"'

  private var isInQuotes = false
  private var wasQuote = false
  private var value = ""
  private val values = new VectorBuilder[String]()
  private var fieldsNumber = 0

  def data = values.result()
  def finished = fieldsNumber > 0 && !isInQuotes

  def parse(line: String, lineNum: Int): Boolean = {
    for(char <- line) {
      char match {
        case `separator` => handleSeparator()
        case CSVEscapeChar => handleEscapeChar(lineNum)
        case _ => handleOrdinaryChar(char,lineNum)
      }
    }
    handleEndOfLine()
    finished
  }

  private def handleSeparator(): Unit = {
    if(!isInQuotes || wasQuote) {
      collectValue()
      isInQuotes = false
      wasQuote = false
    }
    else {
      value += separator
      wasQuote = false
    }
  }

  private def handleEscapeChar(lineNum: Int): Unit = {
    if(!isInQuotes && value.isEmpty) {
      isInQuotes = true
    }
    else if(isInQuotes) {
      if(wasQuote) {
        value += CSVEscapeChar
        wasQuote = false
      }
      else
        wasQuote = true
    }
    else
      throw new CSVException("Bad format: not enclosed or not escaped quotation","wrongQuotation",lineNum,rowNum)
  }

  private def handleOrdinaryChar(char: Char, lineNum: Int): Unit = {
    if(wasQuote)
      throw new CSVException("Bad format: not enclosed or not escaped quotation","wrongQuotation",lineNum,rowNum)
    value += char
    wasQuote = false
  }

  private def handleEndOfLine(): Unit = {
    if(wasQuote)
      isInQuotes = false
    if(isInQuotes)
      value += "\n"
    else
      collectValue()
  }

  private def collectValue(): Unit = {
    values += value
    fieldsNumber += 1
    value = ""
  }
}
