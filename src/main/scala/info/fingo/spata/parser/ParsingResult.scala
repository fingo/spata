package info.fingo.spata.parser

private[spata] object ParsingErrorCode {
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
  case object FieldTooLong extends ErrorCode("Field is longer than provided maximum (unmatched quotation?)")
  case object MissingHeader extends ErrorCode("Header not found (empty content?)")
  case object FieldsHeaderImbalance extends ErrorCode("Number of values doesn't match header size")
}

import ParsingErrorCode._

private[spata] sealed trait ParsingResult {
  def location: Location
  def recordNum: Int
  def fieldNum: Int
}
private[spata] case class ParsingFailure(code: ErrorCode, location: Location, recordNum: Int, fieldNum: Int) extends ParsingResult
private[spata] case class RawRecord(fields: IndexedSeq[String], location: Location, recordNum: Int) extends ParsingResult {
  def isEmpty: Boolean = fields.isEmpty || fields.size == 1 && fields.head.isEmpty
  def fieldNum: Int = fields.size
}

private[spata] case class Location(position: Int, line: Int = 1) {
  def add(position: Int, line: Int = 0): Location = Location(this.position + position, this.line + line)
  def nextPosition: Location = add(1)
  def nextLine: Location = Location(0, this.line + 1)
}
