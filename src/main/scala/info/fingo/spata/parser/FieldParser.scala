/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.parser

import scala.annotation.tailrec
import fs2.{Chunk, Pipe}
import info.fingo.spata.error.ParsingErrorCode._
import FieldParser._
import CharParser.CharResult

/* Carrier for counters, partial field content and information about finished parsing. */
private[spata] case class StateFP(
  counters: Location,
  lc: LocalCounts,
  buffer: StringBuilder = new StringBuilder,
  done: Boolean = false
) extends State {
  def finish(): StateFP = copy(done = true)
}

/* Converter from character symbols to CSV fields.
 * This class tracks source position while consuming symbols to report it precisely, especially in case of failure.
 */
private[spata] class FieldParser[F[_]](fieldSizeLimit: Option[Int])
  extends ChunkAwareParser[F, CharResult, FieldResult, StateFP] {

  import CharParser._
  import CharParser.CharPosition._

  /* Transforms stream of character symbols into fields by providing FS2 pipe. */
  def toFields: Pipe[F, CharResult, FieldResult] = {
    val start = Location(0)
    parse(StateFP(start, LocalCounts(start)))
  }

  /* Parse chunks of CharResults (converted to list) into chunks of FieldResults.
   * The state value carries partial field content and counters. */
  @tailrec
  final override def parseChunk(
    input: List[CharResult],
    output: Vector[FieldResult],
    state: StateFP
  ): (StateFP, Chunk[FieldResult]) =
    input match {
      case _ if fieldTooLong(state.lc) =>
        val chunk = Chunk.vector(output :+ fail(FieldTooLong, state.counters, state.lc))
        (state.finish(), chunk)
      case (cs: CharState) :: tail if cs.finished =>
        val content = state.buffer.toString().dropRight(state.lc.toTrim)
        val field = RawField(content, state.counters, cs.position == FinishedRecord)
        val newCounters = recalculateCounters(state.counters, cs)
        parseChunk(tail, output :+ field, StateFP(newCounters, LocalCounts(field.counters.nextPosition)))
      case (cs: CharState) :: tail =>
        cs.char.map(state.buffer.append)
        val newCounters = recalculateCounters(state.counters, cs)
        val newLC = recalculateLocalCounts(state.lc, cs)
        parseChunk(tail, output, StateFP(newCounters, newLC, state.buffer))
      case (cf: CharFailure) :: _ =>
        val chunk = Chunk.vector(output :+ fail(cf.code, state.counters, state.lc))
        (state.finish(), chunk)
      case _ => (state, Chunk.vector(output))
    }

  private def fail(error: ErrorCode, counters: Location, lc: LocalCounts): FieldFailure = {
    val rc = recalculateCountersAtFailure(error, counters, lc)
    FieldFailure(error, rc)
  }

  private def recalculateCounters(counters: Location, cs: CharState): Location =
    if (cs.isNewLine) counters.nextLine
    else counters.nextPosition

  private def recalculateLocalCounts(lc: LocalCounts, cs: CharState): LocalCounts =
    cs.position match {
      case Start => lc.incLeading()
      case End => lc.incTrailing()
      case Trailing => lc.incTrimming()
      case _ => if (cs.hasChar) lc.incCharacters() else lc.resetTrimming()
    }

  private def recalculateCountersAtFailure(error: ErrorCode, counters: Location, lc: LocalCounts): Location =
    error match {
      case UnclosedQuotation => counters.nextPosition
      case UnescapedQuotation => counters.add(position = -lc.trailSpaces)
      case UnmatchedQuotation => lc.origin.add(position = lc.leadSpaces + 1)
      case _ => counters
    }

  private def fieldTooLong(lc: LocalCounts): Boolean =
    fieldSizeLimit.exists(_ < lc.characters)
}

private[spata] object FieldParser {
  sealed trait FieldResult
  case class FieldFailure(code: ErrorCode, counters: Location) extends FieldResult
  case class RawField(value: String, counters: Location, endOfRecord: Boolean = false) extends FieldResult

  case class LocalCounts(
    origin: Location,
    characters: Int = 0,
    leadSpaces: Int = 0,
    trailSpaces: Int = 0,
    toTrim: Int = 0
  ) {
    def incCharacters(): LocalCounts = copy(characters = this.characters + 1, toTrim = 0)
    def incLeading(): LocalCounts = copy(leadSpaces = this.leadSpaces + 1, toTrim = 0)
    def incTrailing(): LocalCounts = copy(trailSpaces = this.trailSpaces + 1, toTrim = 0)
    def incTrimming(): LocalCounts = copy(characters = this.characters + 1, toTrim = this.toTrim + 1)
    def resetTrimming(): LocalCounts = copy(toTrim = 0)
  }
}
