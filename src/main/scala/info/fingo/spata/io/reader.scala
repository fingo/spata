/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.io

import scala.io.Source
import cats.effect.{Blocker, ContextShift, IO, Resource}
import fs2.Stream

/** Utility to read external data and provide stream of characters. */
object reader {

  /** Reads a CSV source and returns a stream of character.
    * The I/O operations are wrapped in [[cats.effect.IO]] allowing deferred computation.
    * The returned [[fs2.Stream]] allows further input processing in a very flexible, purely functional manner.
    *
    * The caller of this function is responsible for proper resource acquisition and release.
    * This is optimally done with [[fs2.Stream.bracket]], e.g.:
    * {{{
    * val stream = Stream
    *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
    *   .flatMap(reader(_))
    * }}}
    *
    * I/O operations are executed on current thread, without execution context shifting.
    * To shift them to a blocking context, use [[shifting(blocker:cats\.effect\.Blocker)* shifting]].
    *
    * Processing I/O errors, manifested through [[java.io.IOException]],
    * should be handled with [[fs2.Stream.handleErrorWith]].
    * If not handled, they will propagate as exceptions.
    *
    * Character encoding has to be handled while creating [[scala.io.Source]].
    *
    * @param source the source containing CSV content
    * @return the stream of characters
    */
  def read(source: Source): Stream[IO, Char] = Stream.fromIterator[IO][Char](source)

  /** Alias for [[read]].
    *
    * @param source the source containing CSV content
    * @return the stream of characters
    */
  def apply(source: Source): Stream[IO, Char] = read(source)

  /** Provides reader with support of context shifting for I/O operations.
    *
    * @param blocker an execution context to be used for blocking I/O operations
    * @param cs the default execution environment for non-blocking operation
    * @return reader with support for context shifting
    */
  def shifting(blocker: Blocker)(implicit cs: ContextShift[IO]): Shifting =
    new Shifting(Some(blocker))(cs)

  /** Provides reader with support of context shifting for I/O operations.
    * Uses internal, default blocker backed by a new cached thread pool.
    *
    * @param cs the default execution environment for non-blocking operation
    * @return reader with support for context shifting
    */
  def shifting(implicit cs: ContextShift[IO]): Shifting = new Shifting(None)(cs)

  /** Reader which shifts I/O operations to a execution context that is safe to use for blocking operations.
    * If no blocker is provided, a new one, backed by a cached thread pool, is allocated.
    *
    * @param blocker optional execution context to be used for blocking I/O operations
    * @param cs the default execution environment for non-blocking operation
    */
  final class Shifting private[spata] (blocker: Option[Blocker])(implicit cs: ContextShift[IO]) {

    /** Reads a CSV source and returns a stream of character.
      *
      * I/O operations are shifted to an execution context provided by a [[cats.effect.Blocker]].
      *
      * @note This function is much less efficient for most use cases than its non-shifting counterpart, [[reader.read]].
      * This is due to [[scala.io.Source]] character-based iterator,
      * which causes context shift for each fetched character.
      *
      * @see [[reader.read]] for more information.
      *
      * @param source the source containing CSV content
      * @return the stream of characters
      */
    def read(source: Source): Stream[IO, Char] =
      for {
        b <- Stream.resource(br)
        stream <- Stream.fromBlockingIterator[IO][Char](b, source)
      } yield stream

    /** Alias for [[read]].
      *
      * @param source the source containing CSV content
      * @return the stream of characters
      */
    def apply(source: Source): Stream[IO, Char] = read(source)

    /* Wrap provided blocker in dummy-resource or get real resource with new blocker. */
    private def br = blocker.map(b => Resource(IO((b, IO.unit)))).getOrElse(Blocker[IO])
  }
}
