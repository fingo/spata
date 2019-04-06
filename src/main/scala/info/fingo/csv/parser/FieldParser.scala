package info.fingo.csv.parser

import scala.annotation.tailrec

private class FieldParser(chars: Iterator[Char], fieldDelimiter: Char, recordDelimiter: Char, quote: Char) {
  import FieldParser._
  import CharParser._
  import CharParser.CharPosition._

  private val charParser = CharParser(fieldDelimiter, recordDelimiter, quote)

  def parseField(counters: ParsingCounters): FieldResult = {
    @tailrec
    def loop(sb: StringBuilder, state: CharState, counters: ParsingCounters): (CharResult, ParsingCounters) = {
      if(!chars.hasNext || state.finished)
        (finish(state), counters)
      else {
        charParser.parseChar(chars.next(), state) match {
          case newState: CharState =>
            newState.char.map(sb.append)
            val newCounters =
              if(newState.isNewLine) counters.nextLine()
              else if(newState.hasChar) counters.nextChar()
              else counters.nextPosition()
            loop(sb, newState, newCounters)
          case failure => (failure, counters.nextPosition())
        }
      }
    }

    val sb = StringBuilder.newBuilder
    val (result, updatedCounters) = loop(sb, CharState(None, Start), counters)
    result match {
      case state: CharState =>
        RawField(sb.toString().dropRight(state.toTrim), updatedCounters, state.position == FinishedRecord)
      case failure: CharFailure =>
        FieldFailure(failure.code, failure.message, updatedCounters)
    }
  }

  private def finish(state: CharState): CharResult = {
    if(state.position == Quoted)
      CharFailure("prematureEOF", "Bad format: premature end of file (unmatched quotation?)")
    else if(!chars.hasNext)
      CharState(None, FinishedRecord, state.toTrim)
    else
      state
  }
}

private object FieldParser {
  sealed trait FieldResult
  case class FieldFailure(code: String, message: String, counters: ParsingCounters) extends  FieldResult
  case class RawField(value: String, counters: ParsingCounters, endOfRecord: Boolean = false) extends FieldResult

  def apply(chars: Iterator[Char], fieldDelimiter: Char, recordDelimiter: Char, quote: Char) =
    new FieldParser(chars, fieldDelimiter, recordDelimiter, quote)
}
