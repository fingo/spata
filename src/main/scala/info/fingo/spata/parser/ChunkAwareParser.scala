/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.parser

import fs2.{Chunk, Pipe, Pull, Stream}

/* Trait for state carriers which shows if processing has been finished */
private[spata] trait State:
  val done: Boolean

/* Template for stream content conversion which preserves chunkiness.
 * F is the effect type, I stream content input type, O output type and S state. */
private[spata] trait ChunkAwareParser[F[_], I, O, S <: State]:

  /* Template method which handles chunk processing. */
  def parse(state: S): Pipe[F, I, O] =
    def loop(is: Stream[F, I], state: S): Pull[F, O, Unit] =
      is.pull.uncons.flatMap:
        case Some((h, t)) =>
          val (nextState, resultChunk) = parseChunk(h.toList, Vector.empty[O], state)
          Pull.output(resultChunk) >> next(t, nextState)
        case None => Pull.done
    def next(fields: Stream[F, I], state: S) = if state.done then Pull.done else loop(fields, state)
    is => loop(is, state).stream

  /* Abstract method responsible for specific chunk data parsing, used recursively.
   * Input and output data are converted to regular collection for convenient handling
   * and state used to carry information between calls.
   * The out is converted back to chunk while returning.
   * The returned state contains counters and consumed but not yielded data. */
  def parseChunk(input: List[I], output: Vector[O], state: S): (S, Chunk[O])
