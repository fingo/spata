/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.io

import java.io.InputStream

import scala.io.{BufferedSource, Source}
import cats.effect.{Blocker, ContextShift, IO, Resource}
import fs2.{io, text, Chunk, Stream}

/** Utility to read external data and provide stream of characters. */
object reader {

  private val blockSize = 4096
  private val autoClose = false

  /** Reads a CSV source and returns a stream of character.
    * The I/O operations are wrapped in [[cats.effect.IO]] allowing deferred computation.
    * The returned [[fs2.Stream]] allows further input processing in a very flexible, purely functional manner.
    *
    * The caller of this function is responsible for proper resource acquisition and release.
    * This is optimally done with [[fs2.Stream.bracket]], e.g.:
    * {{{
    * val stream = Stream
    *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
    *   .flatMap(reader.read)
    * }}}
    *
    * I/O operations are executed on current thread, without execution context shifting.
    * To shift them to a blocking context, use [[withBlocker(blocker:cats\.effect\.Blocker)* shifting]].
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

  /** Reads a CSV source and returns a stream of character.
    *
    * @see [[read(source:scala\.io\.Source)* read]] for more information.
    *
    * @param is input stream containing CSV content
    * @return the stream of characters
    */
  def read(is: InputStream): Stream[IO, Char] = read(new BufferedSource(is, blockSize))

  /** Alias for [[read(source:scala\.io\.Source)* read]].
    *
    * @param source the source containing CSV content
    * @return the stream of characters
    */
  def apply(source: Source): Stream[IO, Char] = read(source)

  /** Alias for [[read(is:java\.io\.InputStream)* read]].
    *
    * @param is input stream containing CSV content
    * @return the stream of characters
    */
  def apply(is: InputStream): Stream[IO, Char] = read(is)

  /** Pipe converting stream with CSV source to stream of characters.
    *
    * @example
    * {{{
    * val stream = Stream
    *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
    *   .through(reader.by)
    * }}}
    *
    * @see [[read(source:scala\.io\.Source)* read]] for more information.
    *
    * @return a pipe to converter [[scala.io.Source]] into [[scala.Char]]s
    */
  def by: Stream[IO, Source] => Stream[IO, Char] = s => s.flatMap(read)

  /** Provides reader with support of context shifting for I/O operations.
    *
    * @param blocker an execution context to be used for blocking I/O operations
    * @param cs the default execution environment for non-blocking operation
    * @return reader with support for context shifting
    */
  def withBlocker(blocker: Blocker)(implicit cs: ContextShift[IO]): WithBlocker =
    new WithBlocker(Some(blocker))(cs)

  /** Provides reader with support of context shifting for I/O operations.
    * Uses internal, default blocker backed by a new cached thread pool.
    *
    * @param cs the default execution environment for non-blocking operation
    * @return reader with support for context shifting
    */
  def withBlocker(implicit cs: ContextShift[IO]): WithBlocker = new WithBlocker(None)(cs)

  /** Reader which shifts I/O operations to a execution context that is safe to use for blocking operations.
    * If no blocker is provided, a new one, backed by a cached thread pool, is allocated.
    *
    * @param blocker optional execution context to be used for blocking I/O operations
    * @param cs the default execution environment for non-blocking operation
    */
  final class WithBlocker private[spata] (blocker: Option[Blocker])(implicit cs: ContextShift[IO]) {

    /** Reads a CSV source and returns a stream of character.
      *
      * I/O operations are shifted to an execution context provided by a [[cats.effect.Blocker]].
      *
      * @note This function is much less efficient for most use cases than its non-shifting counterpart,
      * [[reader.read(source:scala\.io\.Source)* reader.read]].
      * This is due to [[scala.io.Source]] character-based iterator,
      * which causes context shift for each fetched character.
      *
      * @see [[reader.read(source:scala\.io\.Source)* reader.read]] for more information.
      *
      * @param source the source containing CSV content
      * @return the stream of characters
      */
    def read(source: Source): Stream[IO, Char] =
      for {
        b <- Stream.resource(br)
        stream <- Stream.fromBlockingIterator[IO][Char](b, source)
      } yield stream

    /** Reads a CSV source and returns a stream of character.
      *
      * @see [[read(source:scala\.io\.Source)* read]] for more information.
      *
      * @param is input stream containing CSV content
      * @return the stream of characters
      */
    def read(is: InputStream): Stream[IO, Char] =
      for {
        blocker <- Stream.resource(br)
        chunk <- io
          .readInputStream(IO(is), blockSize, blocker, autoClose)
          .through(text.utf8Decode)
          .map(s => Chunk.chars(s.toCharArray))
        char <- Stream.chunk(chunk)
      } yield char

    /** Alias for [[read(source:scala\.io\.Source)* read]].
      *
      * @param source the source containing CSV content
      * @return the stream of characters
      */
    def apply(source: Source): Stream[IO, Char] = read(source)

    /** Alias for [[read(is:java\.io\.InputStream)* read]].
      *
      * @param is input stream containing CSV content
      * @return the stream of characters
      */
    def apply(is: InputStream): Stream[IO, Char] = read(is)

    /** Pipe converting stream with CSV source to stream of characters.
      *
      * @see [[read(source:scala\.io\.Source)* read]] for more information.
      *
      * @return a pipe to converter [[scala.io.Source]] into [[scala.Char]]s
      */
    def by: Stream[IO, Source] => Stream[IO, Char] = s => s.flatMap(read)

    /* Wrap provided blocker in dummy-resource or get real resource with new blocker. */
    private def br = blocker.map(b => Resource(IO((b, IO.unit)))).getOrElse(Blocker[IO])
  }
}
