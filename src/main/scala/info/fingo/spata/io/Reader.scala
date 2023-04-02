/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.io

import java.io.InputStream
import java.nio.charset.CharacterCodingException
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.io.{BufferedSource, Codec, Source}
import cats.effect.{Async, Sync}
import cats.syntax.all._
import fs2.io.file.{Files => FFiles, Flags, Path => FPath}
import fs2.{io, text, Chunk, Pipe, Pull, Stream}
import info.fingo.spata.util.Logger
import fs2.RaiseThrowable

/** Reader interface with reading operations from various sources.
  * The I/O operations are wrapped in effect `F` (e.g. [[cats.effect.IO]]), allowing deferred computation.
  * The returned [[fs2.Stream]] allows further input processing in a very flexible, purely functional manner.
  *
  * Processing I/O errors, manifested through [[java.io.IOException]],
  * should be handled with [[fs2.Stream.handleErrorWith]]. If not handled, they will propagate as exceptions.
  *
  * @tparam F the effect type
  */
sealed trait Reader[F[_]]:

  /** Size of data chunk loaded at once when reading from source.
    * See also [[https://fs2.io/guide.html#chunks FS2 Chunks]].
    */
  val chunkSize: Int

  /** Reads a CSV source and returns a stream of character.
    *
    * The caller of this function is responsible for proper resource acquisition and release.
    * This may be done with [[fs2.Stream.bracket]].
    *
    * Character encoding has to be handled while creating [[scala.io.Source]].
    *
    * @example
    * ```
    * val stream = Stream
    *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
    *   .flatMap(Reader[IO].read)
    * ```
    *
    * @param source the source containing CSV content
    * @return the stream of characters
    */
  def read(source: Source): Stream[F, Char]

  /** Reads a CSV source and returns a stream of character.
    *
    * @note This function does not close the input stream after use,
    * which is different from default behavior of `fs2-io` functions taking `InputStream` as parameter.
    *
    * @see [[read(source* read(source)]] for more information.
    *
    * @param fis input stream containing CSV content, wrapped in effect F to defer its creation
    * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
    * @return the stream of characters
    */
  def read(fis: F[InputStream])(using codec: Codec): Stream[F, Char]

  /** Reads a CSV source and returns a stream of character.
    *
    * @note This function does not close the input stream after use,
    * which is different from default behavior of `fs2-io` functions taking `InputStream` as parameter.
    *
    * @see [[read(source* read(source)]] for more information.
    *
    * @param is input stream containing CSV content
    * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
    * @return the stream of characters
    */
  def read(is: InputStream)(using codec: Codec): Stream[F, Char]

  /** Reads a CSV file and returns a stream of character.
    *
    * @example
    * ```
    * given codec = new Codec(Charset.forName("UTF-8"))
    * val path = Path.of("data.csv")
    * val stream = Reader[IO](1024).read(path)
    * ```
    *
    * @param path the path to source file
    * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
    * @return the stream of characters
    */
  def read(path: Path)(using codec: Codec): Stream[F, Char]

  /** Alias for various [[read]] methods.
    *
    * @param csv the CSV data
    * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
    * @tparam A type of source
    * @return the stream of characters
    */
  def apply[A: Reader.CSV](csv: A)(using codec: Codec): Stream[F, Char] = csv match
    case s: Source => read(s)
    case is: InputStream => read(is)
    case p: Path => read(p)

  /** Alias for [[read]].
    *
    * @param fis input stream containing CSV content, wrapped in effect F to defer its creation
    * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
    * @return the stream of characters
    */
  def apply(fis: F[InputStream])(using codec: Codec): Stream[F, Char] = read(fis)

  /** Pipe converting stream with CSV source to stream of characters.
    *
    * @example
    * ```
    * val stream = Stream
    *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
    *   .through(Reader[IO]().by)
    * ```
    *
    * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
    * @tparam A type of source
    * @return a pipe to converter CSV source into [[scala.Char]]s
    */
  def by[A: Reader.CSV](using codec: Codec): Pipe[F, A, Char] = _.flatMap(apply(_))

/** Utility to read external data and provide stream of characters.
  * It is used through one of its inner classes:
  *  - [[Reader.Plain]] for standard reading operations executed on current thread,
  *  - [[Reader.Shifting]] to support context (thread) shifting for blocking operations
  *  (see [[https://typelevel.org/cats-effect/docs/thread-model#blocking Cats Effect thread model]]).
  *
  * The reading functions in [[Reader.Shifting]], except the one reading from [[scala.io.Source]],
  * use [[https://fs2.io/#/io fs2-io]] library.
  *
  * In every case, the caller of function taking resource ([[scala.io.Source]] or `java.io.InputStream`) as a parameter
  * is responsible for its cleanup.
  *
  * Functions reading binary data (from `java.io.InputStream` or taking `java.nio.file.Path`)
  * use given [[scala.io.Codec]] to decode input data. If not provided, the default JVM charset is used.
  *
  * For input data encoded in `UTF`, the byte order mark (`BOM`) is removed automatically.
  * This is done even for functions reading from already decoded [[scala.io.Source]]
  * as long as the given instance of [[scala.io.Codec]] with `UTF` charset is provided.
  *
  * All of above applies not only to `read` functions but also to `apply` and `by`, which internally make use of `read`.
  */
object Reader:

  /** Default size of data chunk: 4096. Read more about chunks in see [[https://fs2.io/guide.html#chunks FS2 Guide]]. */
  val defaultChunkSize = 4096

  private val autoClose = false
  private val bom = "\uFEFF".head
  private val UTFCharsetPrefix = "UTF-"

  /** Alias for [[plain(chunkSize* plain]].
    *
    * @param chunkSize size of data chunk - see [[https://fs2.io/#/guide?id=chunks FS2 Chunks]].
    * @tparam F the effect type, with type class providing support for delayed execution (typically [[cats.effect.IO]])
    * and logging (provided internally by spata)
    * @return basic `Reader`
    */
  def apply[F[_]: Sync: Logger](chunkSize: Int): Plain[F] = plain(chunkSize)

  /** Alias for [[plain]].
    *
    * @tparam F the effect type, with type class providing support for delayed execution (typically [[cats.effect.IO]])
    * and logging (provided internally by spata)
    * @return basic `Reader`
    */
  def apply[F[_]: Sync: Logger]: Plain[F] = plain(defaultChunkSize)

  /** Provides basic reader executing I/O on current thread.
    *
    * @param chunkSize size of data chunk
    * @tparam F the effect type, with type class providing support for delayed execution (typically [[cats.effect.IO]])
    * and logging (provided internally by spata)
    * @return basic `Reader`
    */
  def plain[F[_]: Sync: Logger](chunkSize: Int): Plain[F] = new Plain[F](chunkSize)

  /** Provides basic reader executing I/O on current thread. Uses default chunk size.
    *
    * @tparam F the effect type, with type class providing support for delayed execution (typically [[cats.effect.IO]])
    * and logging (provided internally by spata)
    * @return basic `Reader`
    */
  def plain[F[_]: Sync: Logger]: Plain[F] = new Plain[F](defaultChunkSize)

  /** Provides reader with support of context (thread) shifting for I/O operations.
    * Uses the built-in internal blocking thread pool.
    *
    * @param chunkSize size of data chunk
    * @tparam F the effect type, with type classes providing support for suspended execution
    * (typically [[cats.effect.IO]]), `Async` execution environment with blocking and non-blocking thread pools
    * (to shift back and forth) and logging (provided internally by spata).
    * @return `Reader` with support for thread shifting
    */
  def shifting[F[_]: Async: Logger](chunkSize: Int): Shifting[F] = new Shifting[F](chunkSize)

  /** Provides reader with support of context (thread) shifting for I/O operations.
    * Uses the built-in internal blocking thread pool and default chunks size.
    *
    * @tparam F the effect type, with type classes providing support for suspended execution
    * (typically [[cats.effect.IO]]), `Async` execution environment with blocking and non-blocking thread pools
    * (to shift back and forth) and logging (provided internally by spata).
    * @return `Reader` with support for thread shifting
    */
  def shifting[F[_]: Async: Logger]: Shifting[F] = new Shifting[F](defaultChunkSize)

  /* Skip BOM from UTF encoded streams */
  private def skipBom[F[_]: Logger](using codec: Codec): Pipe[F, Char, Char] =
    stream =>
      if codec.charSet.name.startsWith(UTFCharsetPrefix) then
        Logger[F].debugS("UTF charset provided - skipping BOM if present") >> stream.dropWhile(_ == bom)
      else stream

  /** Reader which executes I/O operations on current thread, without context (thread) shifting.
    *
    * @param chunkSize size of data chunk
    * @tparam F the effect type, with type class providing support for delayed execution (typically [[cats.effect.IO]])
    * and logging (provided internally by spata)
    */
  final class Plain[F[_]: Sync: Logger] private[spata] (override val chunkSize: Int) extends Reader[F]:

    /** @inheritdoc */
    def read(source: Source): Stream[F, Char] =
      Logger[F].debugS("Reading data on current thread") >> Stream.fromIterator[F](source, chunkSize).through(skipBom)

    /** @inheritdoc */
    def read(fis: F[InputStream])(using codec: Codec): Stream[F, Char] =
      Stream.eval(fis).flatMap(is => read(new BufferedSource(is, chunkSize)))

    /** @inheritdoc */
    def read(is: InputStream)(using codec: Codec): Stream[F, Char] = read(new BufferedSource(is, chunkSize))

    /** @inheritdoc */
    def read(path: Path)(using codec: Codec): Stream[F, Char] =
      Stream
        .bracket(Logger[F].debug(s"Path $path provided as input") *> Sync[F].delay {
          Source.fromInputStream(Files.newInputStream(path, StandardOpenOption.READ))
        })(source => Sync[F].delay(source.close()))
        .flatMap(read)

  /** Reader which shifts I/O operations to a thread pool provided for blocking operations.
    * Uses the built-in internal blocking thread pool.
    *
    * @param chunkSize size of data chunk
    * @tparam F the effect type, with type classes providing support for suspended execution
    * (typically [[cats.effect.IO]]), `Async` execution environment with blocking and non-blocking thread pools
    * (to shift back and forth) and logging (provided internally by spata).
    */
  final class Shifting[F[_]: Async: Logger: RaiseThrowable] private[spata] (override val chunkSize: Int)
    extends Reader[F]:

    /** @inheritdoc
      *
      * @example
      * ```
      * val stream = Stream
      *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
      *   .flatMap(Reader.shifting[IO].read)
      * ```
      *
      * @note This function is less efficient for most use cases than its non-shifting counterpart,
      * [[Plain.read(source* Plain.read]].
      * This is due to [[scala.io.Source]] character-based iterator,
      * which causes context shift for each fetched character.
      */
    def read(source: Source): Stream[F, Char] =
      for
        _ <- Logger[F].debugS("Reading data from Source with context shift")
        char <- Stream.fromBlockingIterator[F](source, chunkSize).through(skipBom)
      yield char

    /** @inheritdoc */
    def read(fis: F[InputStream])(using codec: Codec): Stream[F, Char] =
      for
        _ <- Logger[F].debugS("Reading data from InputStream with context shift")
        char <- io.readInputStream(fis, chunkSize, autoClose).through(byte2char)
      yield char

    /** @inheritdoc */
    def read(is: InputStream)(using codec: Codec): Stream[F, Char] = read(Sync[F].delay(is))

    /** @inheritdoc
      *
      * @example
      * ```
      * given codec = new Codec(Charset.forName("ISO-8859-2"))
      * val path = Path.of("data.csv")
      * val stream = Reader.shifting[IO].read(path)
      * ```
      */
    def read(path: Path)(using codec: Codec): Stream[F, Char] =
      for
        _ <- Logger[F].debugS(s"Reading data from path $path with context shift")
        char <- FFiles[F]
          .readAll(FPath.fromNioPath(path), chunkSize, Flags.Read)
          .through(byte2char)
      yield char

    /* Converts stream of bytes to stream of characters using provided codec. */
    private def byte2char(using codec: Codec): Pipe[F, Byte, Char] =
      _.through(text.decodeWithCharset[F](codec.charSet)).chunks
        .map(_.flatMap(s2cc))
        .flatMap(Stream.chunk)
        .through(skipBom)

    /* Converts string to chunk of characters. */
    private def s2cc(s: String): Chunk[Char] = Chunk.charBuffer(CharBuffer.wrap(s))

  /** Representation of CSV data source, used to witness that certain sources may be used by read operations.
    * @see [[CSV$ CSV]] object.
    */
  sealed trait CSV[-A]

  /** Given instances to witness that given type is supported by `Reader` as CSV source. */
  object CSV:

    /** Witness that [[scala.io.Source]] may be used with `Reader` methods. */
    given sourceWitness: CSV[Source] with {}

    /** Witness that [[java.io.InputStream]] may be used with `Reader` methods. */
    given inputStreamWitness: CSV[InputStream] with {}

    /** Witness that [[java.nio.file.Path]] may be used with `Reader` methods. */
    given pathWitness: CSV[Path] with {}
