package info.fingo.csv

import scala.annotation.tailrec
import scala.collection.immutable.VectorBuilder

class Parser(
  chars: Iterator[Char],
  fieldDelimiter: Char = ',',
  recordDelimiter: Char = '\n',
  quote: Char = '"'
) extends Iterator[RawRow] {

  private var nextRow: Option[RawRow] = None

  import Parser._
  import Parser.CharPosition._

  override def hasNext: Boolean = {
    nextRow match {
      case Some(row) if !row.isEmpty => true
      case _ if !chars.hasNext => false
      case _ =>
        val numOfLinesAbove = nextRow.map(_.numOfLines).getOrElse(0)
        nextRow = Some(readRow(numOfLinesAbove))
        hasNext
    }
  }

  override def next(): RawRow = {
    if(!hasNext)
      throw new NoSuchElementException("Calling next on empty iterator")
    val row = nextRow.get
    nextRow = None
    row
  }

  private def readRow(numOfNewLinesAbove: Int = 0): RawRow = {
    @tailrec
    def loop(fields: VectorBuilder[String], counters: Counters): Counters = {
      parseField(counters) match {
        case field: RawField =>
          fields += field.value
          if(field.endOfRecord)
            field.counters
          else
            loop(fields, field.counters)
        case failure: FieldFailure =>
          throw new CSVException(failure.message, failure.code, failure.counters.newLines, failure.counters.position,1)
      }
    }

    val fields = new VectorBuilder[String]
    val counters = loop(fields, Counters(0, 0, numOfNewLinesAbove + 1))
    RawRow(fields.result(), counters.newLines)
  }

  @inline
  private def isDelimiter(c: Char): Boolean = c == fieldDelimiter || c == recordDelimiter

  private def parseField(counters: Counters): FieldResult = {
    @tailrec
    def loop(sb: StringBuilder, state: CharState, counters: Counters): (CharResult, Counters) = {
      if(!chars.hasNext || state.finished)
        (finish(state), counters)
      else {
        parseChar(chars.next(), state) match {
          case newState: CharState =>
            newState.char.map(sb.append)
            val charsToAdd = if(newState.char.isDefined) 1 else 0
            val newLinesToAdd = if(newState.isNewLine) 1 else 0
            val newPosition = if(newState.isNewLine) 0 else counters.position + 1
            val newCounters = Counters(newPosition, counters.characters + charsToAdd, counters.newLines + newLinesToAdd)
            loop(sb, newState, newCounters)
          case failure => (failure, counters.copy(position = counters.position + 1))
        }
      }
    }

    def finish(state: CharState): CharResult = {
      if(state.position == Quoted)
        CharFailure("prematureEOF", "Bad format: premature end of file (unmatched quotation?)")
      else if(!chars.hasNext)
        CharState(None, FinishedRecord, state.toTrim)
      else
        state
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

  private def parseChar(char: Char, state: CharState): CharResult =
    char match {
      case `quote` if state.position == Start => CharState(None, Quoted)
      case `quote` if state.position == Quoted => CharState(None, Escape)
      case `quote` if state.position == Escape => CharState(Some(quote), Quoted)
      case `quote` => CharFailure( "wrongQuotation", "Bad format: not enclosed or not escaped quotation")
      case CR if recordDelimiter == LF && state.position != Quoted => CharState(None, state.position)
      case c if isDelimiter(c) && state.position == Quoted => CharState(Some(c), Quoted)
      case `fieldDelimiter` => CharState(None, FinishedField, state.toTrim)
      case `recordDelimiter` => CharState(None, FinishedRecord, state.toTrim)
      case c if c.isWhitespace && state.atBoundary => CharState(None, state.position)
      case c if c.isWhitespace && state.position == Escape => CharState(None, End)
      case c if c.isWhitespace && state.position == Regular => CharState(Some(c), Regular, state.toTrim + 1)
      case _ if state.position == Escape || state.position == End => CharFailure("wrongQuotation", "Bad format: not enclosed or not escaped quotation")
      case c if state.position == Start => CharState(Some(c), Regular)
      case c => CharState(Some(c), state.position)
    }
}

object Parser {

  val LF: Char = 0x0A.toChar
  val CR: Char = 0x0D.toChar

  private object CharPosition extends Enumeration {
    type Position = Value
    val Start, Regular, Quoted, Escape, End, FinishedField, FinishedRecord = Value
  }
  import CharPosition._

  sealed private trait CharResult
  private case class CharFailure(code: String, message: String) extends CharResult
  private case class CharState(char: Option[Char], position: Position, toTrim: Int = 0) extends CharResult {
    def finished: Boolean = position == FinishedField || position == FinishedRecord
    def atBoundary: Boolean = position == Start || position == End
    def isNewLine: Boolean = char.contains(LF)
  }

  private case class Counters(position: Int = 0, characters: Int = 0, newLines: Int = 0)

  sealed private trait FieldResult
  private case class FieldFailure(code: String, message: String, counters: Counters) extends  FieldResult
  private case class RawField(value: String, counters: Counters, endOfRecord: Boolean = false) extends FieldResult

  def builder(chars: Iterator[Char]): Builder = new Builder(chars)

  class Builder(chars: Iterator[Char]) {
    private[this] var fieldDelimiter = ','
    private[this] var recordDelimiter = '\n'
    private[this] var quote = '"'

    def fieldDelimiter(delimiter: Char): Builder = {
      fieldDelimiter = delimiter
      this
    }

    def recordDelimiter(delimiter: Char): Builder = {
      recordDelimiter = delimiter
      this
    }

    def quote(quote: Char): Builder = {
      this.quote = quote
      this
    }

    def build(): Parser = new Parser(chars, fieldDelimiter, recordDelimiter, quote)
  }
}

private[csv] case class RawRow(fields: IndexedSeq[String], numOfLines: Int) {
  def isEmpty: Boolean = fields.isEmpty || fields.size == 1 && fields.head.isEmpty
}
