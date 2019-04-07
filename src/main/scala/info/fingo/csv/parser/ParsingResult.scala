package info.fingo.csv.parser

private[csv] object ParsingErrorCode {
  sealed abstract class ErrorCode(val message: String) {
    def code: String = {
      val name = this.getClass.getSimpleName.stripSuffix("$")
      val first = name.take(1)
      name.replaceFirst(first, first.toLowerCase)
    }
    override def toString: String = code
  }
  case object UnclosedQuotation extends ErrorCode("Bad format: not enclosed quotation")
  case object UnescapedQuotation extends ErrorCode("Bad format: not escaped quotation")
  case object UnmatchedQuotation extends ErrorCode("Bad format: unmatched quotation (premature end of file)")
  case object RowTooLong extends ErrorCode("Row is longer than provided maximum (unmatched quotation?)")
}

import ParsingErrorCode._

private[csv] sealed trait ParsingResult {
  def counters: ParsingCounters
}
private[csv] case class ParsingFailure(code: ErrorCode, counters: ParsingCounters) extends ParsingResult
private[csv] case class RawRow(fields: IndexedSeq[String], counters: ParsingCounters) extends ParsingResult {
  def isEmpty: Boolean = fields.isEmpty || fields.size == 1 && fields.head.isEmpty
}

private[csv] case class ParsingCounters(position: Int = 0, characters: Int = 0, fieldIndex: Int = 0, newLines: Int = 0) {
  def add(position: Int = 0, characters: Int = 0, fieldIndex: Int = 0, newLines: Int = 0) =
    ParsingCounters(this.position + position, this.characters + characters, this.fieldIndex + fieldIndex, this.newLines + newLines)
  def nextPosition(): ParsingCounters = add(1)
  def nextChar(): ParsingCounters = add(1, 1)
  def nextField(): ParsingCounters = add(fieldIndex = 1)
  def nextLine(): ParsingCounters = ParsingCounters(0, this.characters, this.fieldIndex, this.newLines + 1)
}
