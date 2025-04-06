/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.parser

import fs2.Chunk
import fs2.Pipe
import fs2.Pull
import fs2.Stream
import info.fingo.spata.error.ParsingErrorCode.*

/* A finite-state transducer to converter plain source characters into context-dependent symbols,
 * taking into consideration special meaning of some characters (e.g. separators), quoting and escaping.
 */
final private[spata] class CharParser[F[_]](fieldDelimiter: Char, recordDelimiter: Char, quote: Char, trim: Boolean):

  import CharParser.*
  import CharParser.CharPosition.*

  /* Transforms plain characters into context-dependent symbols by providing FS2 pipe. */
  def toCharResults: Pipe[F, Char, CharResult] = toCharResults(CharState(Left(STX), Start))

  /* Parse characters and covert them to CharResults based on previous state. Stop on first failure. */
  private def toCharResults(state: CharState): Pipe[F, Char, CharResult] =
    def loop(chars: Stream[F, Char], state: CharState): Pull[F, CharResult, Unit] =
      chars.pull.uncons.flatMap:
        case Some((h, t)) =>
          val (nextState, resultChunk) = parseChunk(h, state)
          nextState match
            case cs: CharState => Pull.output(resultChunk) >> loop(t, cs)
            case cf: CharFailure => Pull.output(dropFailures(resultChunk)) >> Pull.output1(cf) >> Pull.done
        case None => Pull.output1(endOfStream(state)) >> Pull.done
    chars => loop(chars, state).stream

  /* Parse chunk of characters into chunk of CharResults. */
  private def parseChunk(chunk: Chunk[Char], state: CharState) =
    chunk.mapAccumulate(state: CharResult): (s, c) =>
      val nextState = s match
        case cs: CharState => parseChar(c, cs)
        case cf: CharFailure => cf
      (nextState, nextState)

  private def endOfStream(state: CharState): CharResult =
    state.position match
      case Quoted => CharFailure(UnmatchedQuotation)
      case _ => CharState(Left(ETX), FinishedRecord)

  /* Keep only successful parsing results to output failure state once only. */
  private def dropFailures(chunk: Chunk[CharResult]) = chunk.filter:
    case _: CharState => true
    case _ => false

  private inline def isDelimiter(c: Char): Boolean = c == fieldDelimiter || c == recordDelimiter

  private inline def isWhitespace(c: Char): Boolean =
    trim && c.isWhitespace // without trimming, whitespace is a regular char

  /* Core translating function - state transitions. */
  private def parseChar(char: Char, state: CharState): CharResult =
    char match
      case `quote` if state.atBeginning => CharState(Left(char), Quoted)
      case `quote` if state.position == Quoted => CharState(Left(char), Escape)
      case `quote` if state.position == Escape => CharState(Right(quote), Quoted)
      case `quote` => CharFailure(UnclosedQuotation)
      case CR if recordDelimiter == LF && state.finished => CharState(Left(char), Start)
      case CR if recordDelimiter == LF && state.position != Quoted => CharState(Left(char), state.position)
      case c if isDelimiter(c) && state.position == Quoted => CharState(Right(c), Quoted)
      case `fieldDelimiter` => CharState(Left(char), FinishedField)
      case `recordDelimiter` => CharState(Left(char), FinishedRecord)
      case c if isWhitespace(c) && state.atBoundary => CharState(Left(char), state.position)
      case c if isWhitespace(c) && state.finished => CharState(Left(char), Start)
      case c if isWhitespace(c) && state.position == Escape => CharState(Left(char), End)
      case c if isWhitespace(c) && state.isSimple => CharState(Right(c), Trailing)
      case _ if state.position == Escape || state.position == End => CharFailure(UnescapedQuotation)
      case c if state.atBeginning => CharState(Right(c), Regular)
      case c if state.position == Trailing => CharState(Right(c), Regular)
      case c => CharState(Right(c), state.position)

  val newLineDelimiter: Option[Char] = List(recordDelimiter, fieldDelimiter, quote).find(_ == LF)

private[spata] object CharParser:

  val LF: Char = 0x0A.toChar
  val CR: Char = 0x0D.toChar
  val STX: Char = 0x02.toChar
  val ETX: Char = 0x03.toChar

  object CharPosition extends Enumeration:
    type CharPosition = Value
    val Start, Regular, Quoted, Trailing, Escape, End, FinishedField, FinishedRecord = Value

  import CharPosition._

  sealed trait CharResult

  case class CharFailure(code: ErrorCode) extends CharResult

  /* Represents character with its context - position in CSV.
   * If character retains its direct meaning it is represented as Right.
   * If it is interpreted as a special character or is skipped, it is represented as Left.
   */
  case class CharState(char: Either[Char, Char], position: CharPosition) extends CharResult:
    def isSimple: Boolean = position == Regular || position == Trailing
    def finished: Boolean = position == FinishedField || position == FinishedRecord
    def atBoundary: Boolean = position == Start || position == End
    def atBeginning: Boolean = position == Start || finished
    def isNewLine: Boolean = char.fold(_ == LF, _ == LF)
    def hasChar: Boolean = char.isRight
