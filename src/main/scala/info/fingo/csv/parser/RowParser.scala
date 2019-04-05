package info.fingo.csv.parser

import scala.annotation.tailrec
import scala.collection.immutable.VectorBuilder

private[csv] class RowParser(
  chars: Iterator[Char],
  fieldDelimiter: Char = ',',
  recordDelimiter: Char = '\n',
  quote: Char = '"'
) extends Iterator[ParsingResult] {

  import FieldParser._

  private val fieldParser = FieldParser(chars, fieldDelimiter, recordDelimiter, quote)

  private var nextRow: Option[ParsingResult] = None

  override def hasNext: Boolean = {
    nextRow match {
      case Some(_: ParsingFailure) => true
      case Some(row: RawRow) if !row.isEmpty => true
      case _ if !chars.hasNext => false
      case _ =>
        val counters = ParsingCounters(0, 0, nextRow.map(_.counters.newLines).getOrElse(0))
        nextRow = Some(readRow(counters))
        hasNext
    }
  }

  override def next(): ParsingResult = {
    if(!hasNext)
      throw new NoSuchElementException("Calling next on empty iterator")
    val row = nextRow.get
    nextRow = None
    row
  }

  private def readRow(counters: ParsingCounters): ParsingResult = {
    @tailrec
    def loop(fields: VectorBuilder[String], counters: ParsingCounters): ParsingResult = {
      fieldParser.parseField(counters) match {
        case field: RawField =>
          fields += field.value
          if(field.endOfRecord)
            RawRow(fields.result(), field.counters)
          else
            loop(fields, field.counters)
        case failure: FieldFailure =>
          ParsingFailure(failure.code, failure.message, failure.counters)
      }
    }

    val fields = new VectorBuilder[String]
    loop(fields, counters.copy(newLines = counters.newLines + 1))
  }
}

private[csv] object RowParser {

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

    def build(): RowParser = new RowParser(chars, fieldDelimiter, recordDelimiter, quote)
  }
}
