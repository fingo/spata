package info.fingo.spata.parser

import cats.effect.IO
import fs2.{Pipe, Pull, Stream}
import ParsingErrorCode._

/* A finite-state transducer to convert plain source characters into context-dependent symbols,
 * taking into consideration special meaning of some characters (e.g. separators), quoting and escaping.
 */
private[spata] class CharParser(fieldDelimiter: Char, recordDelimiter: Char, quote: Char) {
  import CharParser._
  import CharParser.CharPosition._

  /* Transforms plain characters into context-dependent symbols by providing FS2 pipe. */
  def toCharResults: Pipe[IO, Char, CharResult] = toCharResults(CharState(None, Start))

  private def toCharResults(state: CharState): Pipe[IO, Char, CharResult] = {
    def loop(chars: Stream[IO, Char], state: CharState): Pull[IO, CharResult, Unit] =
      chars.pull.uncons1.flatMap {
        case Some((h, t)) =>
          parseChar(h, state) match {
            case cs: CharState => Pull.output1(cs) >> loop(t, cs)
            case cf: CharFailure => Pull.output1(cf) >> Pull.done
          }
        case None => Pull.output1(endOfStream(state)) >> Pull.done
      }
    chars => loop(chars, state).stream
  }

  private def endOfStream(state: CharState): CharResult =
    state.position match {
      case Quoted => CharFailure(UnmatchedQuotation)
      case _ => CharState(None, FinishedRecord)
    }

  @inline
  private def isDelimiter(c: Char): Boolean = c == fieldDelimiter || c == recordDelimiter

  /* Core translating function - state transitions. */
  private def parseChar(char: Char, state: CharState): CharResult =
    char match {
      case `quote` if state.atBeginning => CharState(None, Quoted)
      case `quote` if state.position == Quoted => CharState(None, Escape)
      case `quote` if state.position == Escape => CharState(Some(quote), Quoted)
      case `quote` => CharFailure(UnclosedQuotation)
      case CR if recordDelimiter == LF && state.position != Quoted => CharState(None, state.position)
      case c if isDelimiter(c) && state.position == Quoted => CharState(Some(c), Quoted)
      case `fieldDelimiter` => CharState(None, FinishedField)
      case `recordDelimiter` => CharState(None, FinishedRecord)
      case c if c.isWhitespace && state.atBoundary => CharState(None, state.position)
      case c if c.isWhitespace && state.position == FinishedField => CharState(None, Start)
      case c if c.isWhitespace && state.position == Escape => CharState(None, End)
      case c if c.isWhitespace && state.isSimple => CharState(Some(c), Trailing)
      case _ if state.position == Escape || state.position == End => CharFailure(UnescapedQuotation)
      case c if state.atBeginning => CharState(Some(c), Regular)
      case c if state.position == Trailing => CharState(Some(c), Regular)
      case c => CharState(Some(c), state.position)
    }
}

private[spata] object CharParser {
  val LF: Char = 0x0A.toChar
  val CR: Char = 0x0D.toChar

  object CharPosition extends Enumeration {
    type CharPosition = Value
    val Start, Regular, Quoted, Trailing, Escape, End, FinishedField, FinishedRecord = Value
  }
  import CharPosition._

  sealed trait CharResult

  case class CharFailure(code: ErrorCode) extends CharResult

  case class CharState(char: Option[Char], position: CharPosition) extends CharResult {
    def isSimple: Boolean = position == Regular || position == Trailing
    def finished: Boolean = position == FinishedField || position == FinishedRecord
    def atBoundary: Boolean = position == Start || position == End
    def atBeginning: Boolean = position == Start || finished
    // TODO: check if any separator is really a new line
    def isNewLine: Boolean = char.contains(LF) || position == FinishedRecord
    def hasChar: Boolean = char.isDefined
  }
}
