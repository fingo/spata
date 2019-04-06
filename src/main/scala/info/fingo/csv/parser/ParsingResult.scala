package info.fingo.csv.parser

private[csv] sealed trait ParsingResult {
  def counters: ParsingCounters
}
private[csv] case class ParsingFailure(code: String, message: String, counters: ParsingCounters) extends ParsingResult
private[csv] case class RawRow(fields: IndexedSeq[String], counters: ParsingCounters) extends ParsingResult {
  def isEmpty: Boolean = fields.isEmpty || fields.size == 1 && fields.head.isEmpty
}

private[csv] case class ParsingCounters(position: Int = 0, characters: Int = 0, fieldIndex: Int = 0, newLines: Int = 0) {
  def nextPosition(): ParsingCounters = this.copy(position = this.position + 1)
  def nextChar(): ParsingCounters = ParsingCounters(this.position + 1, this.characters + 1, this.fieldIndex, this.newLines)
  def nextField(): ParsingCounters = this.copy(fieldIndex = this.fieldIndex + 1)
  def nextLine(): ParsingCounters = ParsingCounters(0, this.characters, this.fieldIndex, this.newLines + 1)
}
