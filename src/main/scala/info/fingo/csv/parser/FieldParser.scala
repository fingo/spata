package info.fingo.csv.parser

import scala.annotation.tailrec

private class FieldParser(chars: Iterator[Char], fieldDelimiter: Char, recordDelimiter: Char, quote: Char) {
  import FieldParser._
  import CharParser._
  import CharParser.CharPosition._

  private val charParser = CharParser(fieldDelimiter, recordDelimiter, quote)

  def parseField(counters: ParsingCounters): FieldResult = {
    @tailrec
    def loop(sb: StringBuilder, state: CharState, counters: ParsingCounters, spaces: SpaceCounts)
    : (CharResult, ParsingCounters, SpaceCounts) = {
      if(!chars.hasNext || state.finished)
        (finish(state), counters, spaces)
      else {
        charParser.parseChar(chars.next(), state) match {
          case newState: CharState =>
            newState.char.map(sb.append)
            val newCounters =
              if(newState.isNewLine) counters.nextLine()
              else if(newState.hasChar) counters.nextChar()
              else counters.nextPosition()
            val newSpaceCounts = newState.position match {
              case Start => spaces.incLeading()
              case End => spaces.incTrailing()
              case _ => spaces
            }
            loop(sb, newState, newCounters, newSpaceCounts)
          case failure => (failure, counters, spaces)
        }
      }
    }

    val sb = StringBuilder.newBuilder
    val (result, updatedCounters, spaces) = loop(sb, CharState(None, Start), counters, SpaceCounts())
    result match {
      case state: CharState =>
        RawField(sb.toString().dropRight(state.toTrim), updatedCounters, state.position == FinishedRecord)
      case failure: CharFailure =>
        val reportedCounters = failure.code match {
          case "unescapedQuotation" => updatedCounters.add(position = -spaces.trailing)
          case "unmatchedQuotation" => counters.copy(counters.position + spaces.leading + 1, 0)
          case _ => updatedCounters.nextPosition()
        }
        FieldFailure(failure.code, failure.message, reportedCounters)
    }
  }

  private def finish(state: CharState): CharResult = {
    if(state.position == Quoted)
      CharFailure("unmatchedQuotation", "Bad format: unmatched quotation (premature end of file)")
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

  case class SpaceCounts(leading: Int = 0, trailing: Int = 0) {
    def incLeading(): SpaceCounts = copy(leading = this.leading + 1)
    def incTrailing(): SpaceCounts = copy(trailing = this.trailing + 1)
  }

  def apply(chars: Iterator[Char], fieldDelimiter: Char, recordDelimiter: Char, quote: Char) =
    new FieldParser(chars, fieldDelimiter, recordDelimiter, quote)
}
