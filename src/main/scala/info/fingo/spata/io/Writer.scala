/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.io

import java.io.OutputStream
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.io.Codec
import cats.effect.{Async, Sync}
import cats.syntax.all._
import fs2.{io, text, Chunk, Pipe, Stream}
import fs2.io.file.{Files => FFiles, Flags, Path => FPath}
import info.fingo.spata.util.Logger

/** Writer interface with writing operations to various destinations.
  * The I/O operations are wrapped in effect `F` (e.g. [[cats.effect.IO]]), allowing deferred computation.
  *
  * Processing I/O errors, manifested through [[java.io.IOException]],
  * should be handled with [[fs2.Stream.handleErrorWith]]. If not handled, they will propagate as exceptions.
  *
  * @tparam F the effect type
  */
sealed trait Writer[F[_]]:

  /** Writes CSV to destination `OutputStream`.
    *
    * @note This function does not close the output stream after use,
    * which is different from default behavior of `fs2-io` functions taking `OutputStream` as parameter.
    * @param fos input stream containing CSV content, wrapped in effect F to defer its creation
    * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
    * @return pipe converting stream of characters to empty one
    */
  def write(fos: F[OutputStream])(using codec: Codec): Pipe[F, Char, Unit]

  /** Writes CSV to destination `OutputStream`.
    *
    * @note This function does not close the output stream after use,
    * which is different from default behavior of `fs2-io` functions taking `OutputStream` as parameter.
    * @param os input stream containing CSV content
    * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
    * @return pipe converting stream of characters to empty one
    */
  def write(os: OutputStream)(using codec: Codec): Pipe[F, Char, Unit]

  /** Writes CSV to target path.
    *
    * @example
    * ```
    * given codec = new Codec(Charset.forName("UTF-8"))
    * val path = Path.of("data.csv")
    * val stream: Stream[IO, Char] = ???
    * val handler: Stream[IO, Unit] = stream.through(Writer[IO].write(path))
    * ```
    *
    * @param path the path to target file
    * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
    * @return pipe converting stream of characters to empty one
    */
  def write(path: Path)(using codec: Codec): Pipe[F, Char, Unit]

  /** Alias for various [[write]] methods.
    *
    * @param csv the CSV data destination
    * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
    * @tparam A type of target
    * @return pipe converting stream of characters to empty one
    */
  def apply[A: Writer.CSV](csv: A)(using codec: Codec): Pipe[F, Char, Unit] = csv match
    case os: OutputStream => write(os)
    case p: Path => write(p)

  /** Alias for [[write]] method.
    *
    * @param csv the CSV data destination
    * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
    * @return pipe converting stream of characters to empty one
    */
  def apply(csv: F[OutputStream])(using codec: Codec): Pipe[F, Char, Unit] = write(csv)

/** Utility to write external data from a stream of characters.
  * It is used through one of its inner classes:
  *  - [[Writer.Plain]] for standard writing operations executed on current thread,
  *  - [[Writer.Shifting]] to support context (thread) shifting for blocking operations
  *  (see [[https://typelevel.org/cats-effect/docs/thread-model#blocking Cats Effect thread model]]).
  *
  * The writing functions in [[Writer.Shifting]] use [[https://fs2.io/#/io fs2-io]] library.
  *
  * In every case, the caller of function taking resource (`java.io.OutputStream`) as a parameter
  * is responsible for its cleanup.
  *
  * Functions writing binary data (to `java.io.InputStream` or taking `java.nio.file.Path`)
  * use given [[scala.io.Codec]] to encode data. If not provided, the default JVM charset is used.
  *
  * For data encoded to `UTF`, the byte order mark (`BOM`) is not added to the output.
  *
  * All of above applies not only to `write` functions but also to `apply`, which internally make use of `write`.
  */
object Writer:

  private val autoClose = false
  private val openOptions = Seq(StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE)

  /** Alias for [[shifting]].
    *
    * @tparam F the effect type, with type classes providing support for suspended execution
    * (typically [[cats.effect.IO]]), `Async` execution environment with blocking and non-blocking thread pools
    * (to shift back and forth) and logging (provided internally by spata).
    * @return `Writer` with support for thread shifting
    */
  def apply[F[_]: Async: Logger]: Shifting[F] = shifting

  /** Provides basic writer executing I/O on current thread.
    *
    * @tparam F the effect type, with type class providing support for delayed execution (typically [[cats.effect.IO]])
    * and logging (provided internally by spata)
    * @return basic `Writer`
    */
  def plain[F[_]: Sync: Logger]: Plain[F] = new Plain[F]()

  /** Provides writer with support of context (thread) shifting for I/O operations.
    * Uses the built-in internal blocking thread pool and default chunks size.
    *
    * @tparam F the effect type, with type classes providing support for suspended execution
    * (typically [[cats.effect.IO]]), `Async` execution environment with blocking and non-blocking thread pools
    * (to shift back and forth) and logging (provided internally by spata).
    * @return `Writer` with support for thread shifting
    */
  def shifting[F[_]: Async: Logger]: Shifting[F] = new Shifting[F]()

  /** Writer which executes I/O operations on current thread, without context (thread) shifting.
    *
    * @tparam F the effect type, with type class providing support for delayed execution (typically [[cats.effect.IO]])
    * and logging (provided internally by spata)
    */
  final class Plain[F[_]: Sync: Logger] private[spata] () extends Writer[F]:

    /** @inheritdoc */
    def write(fos: F[OutputStream])(using codec: Codec): Pipe[F, Char, Unit] =
      (in: Stream[F, Char]) => Stream.eval(fos).flatMap(os => in.through(write(os)))

    /** @inheritdoc */
    def write(os: OutputStream)(using codec: Codec): Pipe[F, Char, Unit] =
      (in: Stream[F, Char]) =>
        in.through(char2byte).evalMap(c => Sync[F].delay(os.write(c.toArray))) ++ Stream.eval(Sync[F].delay(os.flush()))

    /** @inheritdoc */
    def write(path: Path)(using codec: Codec): Pipe[F, Char, Unit] =
      (in: Stream[F, Char]) =>
        Stream
          .bracket(Logger[F].debug(s"Path $path provided as output") *> Sync[F].delay {
            Files.newOutputStream(path, openOptions: _*)
          })(os => Sync[F].delay(os.close()))
          .flatMap(os => in.through(write(os)))

    protected def char2byte(using codec: Codec): Pipe[F, Char, Chunk[Byte]] =
      (in: Stream[F, Char]) => in.chunks.map(_.mkString_("")).through(text.encodeC(codec.charSet))

  /** Writer which shifts I/O operations to a thread pool provided for blocking operations.
    * Uses the built-in internal blocking thread pool.
    *
    * @tparam F the effect type, with type classes providing support for suspended execution
    * (typically [[cats.effect.IO]]), `Async` execution environment with blocking and non-blocking thread pools
    * (to shift back and forth) and logging (provided internally by spata).
    */
  final class Shifting[F[_]: Async: Logger] private[spata] () extends Writer[F]:

    /** @inheritdoc */
    def write(fos: F[OutputStream])(using codec: Codec): Pipe[F, Char, Unit] =
      (in: Stream[F, Char]) =>
        for
          _ <- Logger[F].debugS("Writing data to OutputStream with thread shifting")
          _ <- in.through(char2byte).through(io.writeOutputStream(fos, autoClose)).unitary
        yield ()

    /** @inheritdoc */
    def write(os: OutputStream)(using codec: Codec): Pipe[F, Char, Unit] = write(Sync[F].delay(os))

    /** @inheritdoc
      *
      * @example
      * ```
      * given codec = new Codec(Charset.forName("UTF-8"))
      * val path = Path.of("data.csv")
      * val stream: Stream[IO, Char] = ???
      * val handler: Stream[IO, Unit] = stream.through(wW.shifting[IO].write(path))
      * ```
      */
    def write(path: Path)(using codec: Codec): Pipe[F, Char, Unit] =
      (in: Stream[F, Char]) =>
        for
          _ <- Logger[F].debugS(s"Writing data to path $path with thread shifting")
          _ <- in
            .through(char2byte)
            .through(FFiles[F].writeAll(FPath.fromNioPath(path), Flags.fromOpenOptions(openOptions)))
            .unitary
        yield ()

    protected def char2byte(using codec: Codec): Pipe[F, Char, Byte] =
      _.chunks.map(_.mkString_("")).through(text.encode(codec.charSet))

  /** Representation of CSV data destination, used to witness that certain sources may be used by write operations.
    * @see [[CSV$ CSV]] object.
    */
  sealed trait CSV[-A]

  /** Given instances to witness that given type is supported by `Writer` as CSV destination. */
  object CSV:

    /** Witness that [[java.io.OutputStream]] may be used with `Writer` methods. */
    given outputStreamWitness: CSV[OutputStream]()

    /** Witness that [[java.nio.file.Path]] may be used with `Writer` methods. */
    given pathWitness: CSV[Path]()
