/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.parser

import scala.collection.immutable.VectorBuilder
import scala.annotation.tailrec
import fs2.{Chunk, Pipe}
import RecordParser._
import FieldParser._

/* Carrier for record number, partial record content and information about finished parsing. */
private[spata] case class StateRP(
  recNum: Int = 1,
  buffer: VectorBuilder[String] = new VectorBuilder[String](),
  done: Boolean = false
) extends State {
  def finish(): StateRP = copy(done = true)
}

/* Converter from CSV fields to records. */
private[spata] class RecordParser[F[_]] extends ChunkAwareParser[F, FieldResult, RecordResult, StateRP] {

  /* Transforms stream of fields into records by providing FS2 pipe. */
  def toRecords: Pipe[F, FieldResult, RecordResult] = parse(StateRP())

  @tailrec
  final override def parseChunk(
    input: List[FieldResult],
    output: Vector[RecordResult],
    state: StateRP
  ): (StateRP, Chunk[RecordResult]) =
    input match {
      case (rf: RawField) :: tail if rf.endOfRecord =>
        state.buffer += rf.value
        val rr = RawRecord(state.buffer.result(), rf.counters, state.recNum)
        if (rr.isEmpty)
          parseChunk(tail, output, StateRP(state.recNum))
        else
          parseChunk(tail, output :+ rr, StateRP(state.recNum + 1))
      case (rf: RawField) :: tail =>
        state.buffer += rf.value
        parseChunk(tail, output, StateRP(state.recNum, state.buffer))
      case (ff: FieldFailure) :: _ =>
        val fieldNum = state.buffer.result().size + 1
        val chunk = Chunk.vector(output :+ RecordFailure(ff.code, ff.counters, state.recNum, fieldNum))
        (state.finish(), chunk)
      case _ => (state, Chunk.vector(output))
    }
}

private[spata] object RecordParser {
  import info.fingo.spata.error.ParsingErrorCode._

  sealed trait RecordResult {
    def location: Location
    def recordNum: Int
    def fieldNum: Int
  }

  case class RecordFailure(code: ErrorCode, location: Location, recordNum: Int, fieldNum: Int) extends RecordResult

  case class RawRecord(fields: IndexedSeq[String], location: Location, recordNum: Int) extends RecordResult {
    def isEmpty: Boolean = fields.isEmpty || fields.size == 1 && fields.head.isEmpty
    def fieldNum: Int = fields.size
  }
}
