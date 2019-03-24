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
  import Parser.Position._

  override def hasNext: Boolean = {
    nextRow match {
      case Some(row) if !row.isEmpty => true
      case _ if !chars.hasNext => false
      case _ =>
        nextRow = Some(readRow())
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

  private def readRow(): RawRow = {
    @tailrec
    def loop(fields: VectorBuilder[String], numOfLines: Int): Int = {
      val rawField = parseField(numOfLines)
      fields += rawField.value
      val numOfNewLines = rawField.numOfLines - 1
      if(rawField.endOfRecord)
        numOfLines + numOfNewLines
      else
        loop(fields, numOfLines + numOfNewLines)
    }

    val fields = new VectorBuilder[String]
    val numOfLines = loop(fields, 1)
    RawRow(fields.result(), numOfLines)
  }

  @inline
  private def isDelimiter(c: Char): Boolean = c == fieldDelimiter || c == recordDelimiter

  private def parseField(lineNumber: Int): RawField = {
    @tailrec
    def loop(sb: StringBuilder, state: State, numOfLines: Int): (State, Int) = {
      if(!chars.hasNext || state.finished)
        finish(state, numOfLines)
      else {
        val nextState = parseChar(chars.next(), state, numOfLines)
        nextState.char.map(sb.append)
        val numOfNewLines = if(nextState.isNewLine) 1 else 0
        loop(sb, nextState, numOfLines + numOfNewLines)
      }
    }

    def finish(state: State, numOfLines: Int): (State, Int) = {
      if(state.position == Quoted)
        throw new CSVException("Bad format: premature end of file (unmatched quotation?)", "prematureEOF", numOfLines, 1)
      if(!chars.hasNext)
        (State(None, FinishedRecord, state.toTrim), numOfLines)
      else
        (state, numOfLines)
    }

    val sb = StringBuilder.newBuilder
    val (state, numOfLines) = loop(sb, State(None, Start), lineNumber)
    RawField(sb.toString().dropRight(state.toTrim), state.position == FinishedRecord, numOfLines)
  }

  private def parseChar(char: Char, state: State, lineNumber: Int): State =
    char match {
      case `quote` if state.position == Start => State(None, Quoted)
      case `quote` if state.position == Quoted => State(None, Escape)
      case `quote` if state.position == Escape => State(Some(quote), Quoted)
      case `quote` => throw new CSVException("Bad format: not enclosed or not escaped quotation", "wrongQuotation", lineNumber, 1)
      case CR if recordDelimiter == LF && state.position != Quoted => State(None, state.position)
      case c if isDelimiter(c) && state.position == Quoted => State(Some(c), Quoted)
      case `fieldDelimiter` => State(None, FinishedField, state.toTrim)
      case `recordDelimiter` => State(None, FinishedRecord, state.toTrim)
      case c if c.isWhitespace && state.atBoundary => State(None, state.position)
      case c if c.isWhitespace && state.position == Escape => State(None, End)
      case c if c.isWhitespace && state.position == Regular => State(Some(c), Regular, state.toTrim + 1)
      case _ if state.position == Escape || state.position == End => throw new CSVException("Bad format: not enclosed or not escaped quotation", "wrongQuotation", lineNumber, 1)
      case c if state.position == Start => State(Some(c), Regular)
      case c => State(Some(c), state.position)
    }
}

object Parser {

  val LF: Char = 0x0A.toChar
  val CR: Char = 0x0D.toChar

  private object Position extends Enumeration {
    type Position = Value
    val Start, Regular, Quoted, Escape, End, FinishedField, FinishedRecord = Value
  }
  import Position._

  private case class State(char: Option[Char], position: Position, toTrim: Int = 0) {
    def finished: Boolean = position == FinishedField || position == FinishedRecord
    def atBoundary: Boolean = position == Start || position == End
    def isNewLine: Boolean = char.contains(LF)
  }

  private case class RawField(value: String, endOfRecord: Boolean = false, numOfLines: Int = 1)

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
