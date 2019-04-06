package info.fingo.csv.parser

private class CharParser(fieldDelimiter: Char, recordDelimiter: Char, quote: Char) {
  import CharParser._
  import CharParser.CharPosition._

  @inline
  private def isDelimiter(c: Char): Boolean = c == fieldDelimiter || c == recordDelimiter

  def parseChar(char: Char, state: CharState): CharResult =
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

private object CharParser {
  val LF: Char = 0x0A.toChar
  val CR: Char = 0x0D.toChar

  object CharPosition extends Enumeration {
    type Position = Value
    val Start, Regular, Quoted, Escape, End, FinishedField, FinishedRecord = Value
  }
  import CharPosition._

  sealed trait CharResult
  case class CharFailure(code: String, message: String) extends CharResult
  case class CharState(char: Option[Char], position: Position, toTrim: Int = 0) extends CharResult {
    def finished: Boolean = position == FinishedField || position == FinishedRecord
    def atBoundary: Boolean = position == Start || position == End
    def isNewLine: Boolean = char.contains(LF)
    def hasChar: Boolean = char.isDefined
  }

  def apply(fieldDelimiter: Char, recordDelimiter: Char, quote: Char): CharParser =
    new CharParser(fieldDelimiter, recordDelimiter, quote)
}
