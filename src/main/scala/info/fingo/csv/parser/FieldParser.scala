package info.fingo.csv.parser

import scala.annotation.tailrec

import ParsingErrorCode._

private class FieldParser(
  chars: Iterator[Char],
  fieldDelimiter: Char,
  recordDelimiter: Char,
  quote: Char,
  rowSizeLimit: Option[Int]
) {
  import FieldParser._
  import CharParser._
  import CharParser.CharPosition._

  private val charParser = CharParser(fieldDelimiter, recordDelimiter, quote)

  def parseField(counters: ParsingCounters): FieldResult = {
    @tailrec
    def loop(sb: StringBuilder, state: CharState, counters: ParsingCounters, spaces: SpaceCounts)
    : (CharResult, ParsingCounters, SpaceCounts) = {
      val limitExceeded = rowTooLong(counters)
      if(!chars.hasNext || state.finished || limitExceeded)
        (finish(state, limitExceeded), counters, spaces)
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
          case UnclosedQuotation => updatedCounters.nextPosition()
          case UnescapedQuotation => updatedCounters.add(position = -spaces.trailing)
          case UnmatchedQuotation => counters.copy(counters.position + spaces.leading + 1, 0)
          case RowTooLong => updatedCounters
        }
        FieldFailure(failure.code, reportedCounters)
    }
  }

  private def finish(state: CharState, rowTooLong: Boolean = false): CharResult = {
    if(rowTooLong)
      CharFailure(RowTooLong)
    else if(state.position == Quoted)
      CharFailure(UnmatchedQuotation)
    else if(!chars.hasNext)
      CharState(None, FinishedRecord, state.toTrim)
    else
      state
  }

  private def rowTooLong(counters: ParsingCounters): Boolean =
    rowSizeLimit.exists(_ < counters.characters)
}

private object FieldParser {
  sealed trait FieldResult
  case class FieldFailure(code: ErrorCode, counters: ParsingCounters) extends  FieldResult
  case class RawField(value: String, counters: ParsingCounters, endOfRecord: Boolean = false) extends FieldResult

  case class SpaceCounts(leading: Int = 0, trailing: Int = 0) {
    def incLeading(): SpaceCounts = copy(leading = this.leading + 1)
    def incTrailing(): SpaceCounts = copy(trailing = this.trailing + 1)
  }

  def apply(chars: Iterator[Char], fieldDelimiter: Char, recordDelimiter: Char, quote: Char, maxRecordSize: Option[Int]) =
    new FieldParser(chars, fieldDelimiter, recordDelimiter, quote, maxRecordSize)
}
