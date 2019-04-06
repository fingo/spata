package info.fingo.csv.parser

private[csv] sealed trait ParsingResult {
  def counters: ParsingCounters
}
private[csv] case class ParsingFailure(code: String, message: String, counters: ParsingCounters) extends ParsingResult
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
