/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
/*
 * Part of this code (reader.Shifting.decode) is derived under Apache-2.0 license from http4s.
 * Copyright 2013-2020 http4s.org
 */
package info.fingo.spata.io

import java.io.InputStream
import java.nio.charset.CharacterCodingException
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.io.{BufferedSource, Codec, Source}
import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.syntax.all._
import fs2.{io, Chunk, Pipe, Pull, Stream}

/** Utility to read external data and provide stream of characters.
  * It is used through one of its inner classes:
  *  - [[reader.Plain]] for standard reading operations executed on current thread,
  *  - [[reader.Shifting]] to support context (thread) shifting for blocking operations
  *  (see [[https://typelevel.org/cats-effect/concurrency/basics.html#blocking-threads Cats Effect concurrency guide]]).
  *
  * The reading functions in [[reader.Shifting]], except the one reading from [[scala.io.Source]],
  * use [[https://fs2.io/io.html fs2-io]] library.
  *
  * In every case, the caller of function taking resource ([[scala.io.Source]] or `java.io.InputStream`) as a parameter
  * is responsible for its cleanup.
  *
  * Functions reading binary data (from `java.io.InputStream` or taking `java.nio.file.Path`)
  * use implicit [[scala.io.Codec]] to decode input data. If not provided, the default JVM charset is used.
  *
  * For input data encoded in `UTF`, the byte order mark (`BOM`) is removed automatically.
  * This is done even for functions reading from already decoded [[scala.io.Source]]
  * as long as the implicit [[scala.io.Codec]] with `UTF` charset is provided.
  *
  * All of above applies not only to `read` functions but also to `apply` and `by`, which internally make use of `read`.
  */
object reader {

  /** Default size of data chunk: 4096. Read more about chunks in see [[https://fs2.io/guide.html#chunks FS2 Guide]]. */
  val defaultChunkSize = 4096

  private val autoClose = false
  private val bom = "\uFEFF".head
  private val UTFCharsetPrefix = "UTF-"

  /** Alias for [[plain]].
    *
    * @param chunkSize size of data chunk - see [[https://fs2.io/guide.html#chunks FS2 Chunks]].
    * @tparam F the effect type, with type class providing support for delayed execution (typically [[cats.effect.IO]])
    * @return basic reader
    */
  def apply[F[_]: Sync](chunkSize: Int = defaultChunkSize): Plain[F] = plain(chunkSize)

  /** Provides basic reader executing I/O on current thread.
    *
    * @param chunkSize size of data chunk
    * @tparam F the effect type, with type class providing support for delayed execution (typically [[cats.effect.IO]])
    * @return basic reader
    */
  def plain[F[_]: Sync](chunkSize: Int = defaultChunkSize): Plain[F] = new Plain[F](chunkSize)

  /** Provides reader with support of context shifting for I/O operations.
    *
    * @param blocker an execution context to be used for blocking I/O operations
    * @param chunkSize size of data chunk
    * @tparam F the effect type, with type classes providing support for delayed execution (typically [[cats.effect.IO]])
    * and execution environment for non-blocking operation (to shift back to)
    * @return reader with support for context shifting
    */
  def shifting[F[_]: Sync: ContextShift](blocker: Blocker, chunkSize: Int = defaultChunkSize): Shifting[F] =
    new Shifting[F](Some(blocker), chunkSize)

  /** Provides reader with support of context shifting for I/O operations.
    * Uses internal, default blocker backed by a new cached thread pool.
    *
    * @param chunkSize size of data chunk
    * @tparam F the effect type, with type classes providing support for delayed execution (typically [[cats.effect.IO]])
    * and execution environment for non-blocking operation (to shift back to)
    * @return reader with support for context shifting
    */
  def shifting[F[_]: Sync: ContextShift](chunkSize: Int): Shifting[F] = new Shifting[F](None, chunkSize)

  /** Provides reader with support of context shifting for I/O operations.
    * Uses internal, default blocker backed by a new cached thread pool and default chunks size.
    *
    * @tparam F the effect type, with type classes providing support for delayed execution (typically [[cats.effect.IO]])
    * and execution environment for non-blocking operation (to shift back to)
    * @return reader with support for context shifting
    */
  def shifting[F[_]: Sync: ContextShift](): Shifting[F] = new Shifting[F](None, defaultChunkSize)

  /* Skip BOM from UTF encoded streams */
  private def skipBom[F[_]](implicit codec: Codec): Pipe[F, Char, Char] =
    stream =>
      if (codec.charSet.name.startsWith(UTFCharsetPrefix)) stream.dropWhile(_ == bom)
      else stream

  /** Reader interface with reading operations from various sources.
    * The I/O operations are wrapped in effect `F` (e.g. [[cats.effect.IO]]), allowing deferred computation.
    * The returned [[fs2.Stream]] allows further input processing in a very flexible, purely functional manner.
    *
    * Processing I/O errors, manifested through [[java.io.IOException]],
    * should be handled with [[fs2.Stream.handleErrorWith]]. If not handled, they will propagate as exceptions.
    *
    * @tparam F the effect type
    */
  trait Reader[F[_]] {

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
      * {{{
      * val stream = Stream
      *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
      *   .flatMap(reader[IO]().read)
      * }}}
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
      * @see [[read(source:scala\.io\.Source)* read(source)]] for more information.
      *
      * @param is input stream containing CSV content
      * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
      * @return the stream of characters
      */
    def read(is: InputStream)(implicit codec: Codec): Stream[F, Char]

    /** Reads a CSV file and returns a stream of character.
      *
      * @example
      * {{{
      * implicit val codec = new Codec(Charset.forName("UTF-8"))
      * val path = Path.of("data.csv")
      * val stream = reader[IO](1024).read(path)
      * }}}
      *
      * @param path the path to source file
      * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
      * @return the stream of characters
      */
    def read(path: Path)(implicit codec: Codec): Stream[F, Char]

    /** Alias for various `read` methods.
      *
      * @param cvs the CSV data
      * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
      * @tparam A type of source
      * @return the stream of characters
      */
    def apply[A: CSV](cvs: A)(implicit codec: Codec): Stream[F, Char] = cvs match {
      case s: Source => read(s)
      case is: InputStream => read(is)
      case p: Path => read(p)
    }

    /** Pipe converting stream with CSV source to stream of characters.
      *
      * @example
      * {{{
      * val stream = Stream
      *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
      *   .through(reader[IO]().by)
      * }}}
      *
      * @param codec codec used to convert bytes to characters, with default JVM charset as fallback
      * @tparam A type of source
      * @return a pipe to converter CSV source into [[scala.Char]]s
      */
    def by[A: CSV](implicit codec: Codec): Pipe[F, A, Char] = _.flatMap(apply(_))

    /* Read chunk of data from iterator and store in sequence. */
    protected def bufferIterator[A](it: Iterator[A]): Seq[A] =
      for (_ <- 1 to chunkSize if it.hasNext) yield it.next()

    protected def chunkBufferedIterator[A](seq: Seq[A], it: Iterator[A]): Option[(Chunk[A], Iterator[A])] =
      if (seq.isEmpty) None else Some((Chunk.seq(seq), it))
  }

  /** Reader which executes I/O operations on current thread, without context (thread) shifting.
    *
    * @param chunkSize size of data chunk
    * @tparam F the effect type, with type class providing support for delayed execution (typically [[cats.effect.IO]])
    */
  final class Plain[F[_]: Sync] private[spata] (override val chunkSize: Int) extends Reader[F] {

    /** @inheritdoc */
    def read(source: Source): Stream[F, Char] =
      fromIterator[Char](source).through(skipBom)

    /** @inheritdoc */
    def read(is: InputStream)(implicit codec: Codec): Stream[F, Char] =
      read(new BufferedSource(is, chunkSize))

    /** @inheritdoc */
    def read(path: Path)(implicit codec: Codec): Stream[F, Char] =
      Stream
        .bracket(Sync[F].delay {
          Source.fromInputStream(Files.newInputStream(path, StandardOpenOption.READ))
        })(source => Sync[F].delay { source.close() })
        .flatMap(read)

    /* Load next data chunk from iterator. */
    private def getNextChunk[A](it: Iterator[A]): F[Option[(Chunk[A], Iterator[A])]] =
      Sync[F].delay(bufferIterator(it)).map(s => chunkBufferedIterator(s, it))

    /* Creates stream from iterator. In contrast to Stream.fromIterator builds chunks of data. */
    private def fromIterator[A](it: Iterator[A]): Stream[F, A] =
      Stream.unfoldChunkEval(it)(getNextChunk)
  }

  /** Reader which shifts I/O operations to a execution context provided for blocking operations.
    * If no blocker is provided, a new one, backed by a cached thread pool, is allocated.
    *
    * @param blocker optional execution context to be used for blocking I/O operations
    * @param chunkSize size of data chunk
    * @tparam F the effect type, with type classes providing support for delayed execution (typically [[cats.effect.IO]])
    * and execution environment for non-blocking operation ([[cats.effect.ContextShift]]).
    */
  final class Shifting[F[_]: Sync: ContextShift] private[spata] (blocker: Option[Blocker], override val chunkSize: Int)
    extends Reader[F] {

    /** @inheritdoc
      *
      * @example
      * {{{
      * val stream = Stream
      *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
      *   .flatMap(reader.shifting[IO].read)
      * }}}
      *
      * @note This function is much less efficient for most use cases than its non-shifting counterpart,
      * [[Plain.read(source:scala\.io\.Source)* Plain.read]].
      * This is due to [[scala.io.Source]] character-based iterator,
      * which causes context shift for each fetched character.
      */
    def read(source: Source): Stream[F, Char] =
      for {
        b <- Stream.resource(br)
        char <- fromBlockingIterator[Char](source, b).through(reader.skipBom)
      } yield char

    /** @inheritdoc */
    def read(is: InputStream)(implicit codec: Codec): Stream[F, Char] =
      for {
        blocker <- Stream.resource(br)
        char <- io
          .readInputStream(Sync[F].delay(is), chunkSize, blocker, autoClose)
          .through(byte2char)
      } yield char

    /** @inheritdoc
      *
      * @example
      * {{{
      * implicit val codec = new Codec(Charset.forName("ISO-8859-2"))
      * val path = Path.of("data.csv")
      * val stream = reader.shifting[IO]().read(path)
      * }}}
      */
    def read(path: Path)(implicit codec: Codec): Stream[F, Char] =
      for {
        blocker <- Stream.resource(br)
        char <- io.file
          .readAll[F](path, blocker, chunkSize)
          .through(byte2char)
      } yield char

    /* Load next data chunk from iterator shifting execution to blocker. */
    private def getNextChunk[A](it: Iterator[A], blocker: Blocker): F[Option[(Chunk[A], Iterator[A])]] =
      blocker.delay(bufferIterator(it)).map(s => chunkBufferedIterator(s, it))

    /* Creates stream from iterator using blocker. In contrast to Stream.fromBlockingIterator builds chunks of data. */
    private def fromBlockingIterator[A](it: Iterator[A], b: Blocker): Stream[F, A] =
      Stream.unfoldChunkEval(it)(it => getNextChunk(it, b))

    /* Wrap provided blocker in dummy-resource or get real resource with new blocker. */
    private def br =
      blocker.map(b => Resource(Sync[F].delay((b, Sync[F].unit)))).getOrElse(Blocker[F])

    private def byte2char(implicit codec: Codec): Pipe[F, Byte, Char] =
      _.through(decode(codec)).through(reader.skipBom)

    /* Decode bytes to chars based on provided codec.
     * This function is ported from org.http4s.util.decode with slight modifications */
    private def decode(codec: Codec): Pipe[F, Byte, Char] = {
      val decoder = codec.charSet.newDecoder
      val maxCharsPerByte = decoder.maxCharsPerByte().ceil.toInt
      val avgBytesPerChar = (1.0 / decoder.averageCharsPerByte()).ceil.toInt
      val charBufferSize = 128

      def cb2cc(cb: CharBuffer): Chunk[Char] = Chunk.chars(cb.flip.toString.toCharArray)

      _.repeatPull[Char] {
        _.unconsN(charBufferSize * avgBytesPerChar, allowFewer = true).flatMap {
          case Some((chunk, stream)) if chunk.nonEmpty =>
            val bytes = chunk.toArray
            val bb = ByteBuffer.wrap(bytes)
            val cb = CharBuffer.allocate(bytes.length * maxCharsPerByte)
            val cr = decoder.decode(bb, cb, false)
            if (cr.isError) Pull.raiseError[F](new CharacterCodingException)
            else {
              val nextStream = stream.consChunk(Chunk.byteBuffer(bb.slice()))
              Pull.output(cb2cc(cb)).as(Some(nextStream))
            }
          case Some((_, stream)) =>
            Pull.output(Chunk.empty[Char]).as(Some(stream))
          case None =>
            val cb = CharBuffer.allocate(1)
            val cr = decoder.decode(ByteBuffer.allocate(0), cb, true)
            if (cr.isError) Pull.raiseError[F](new CharacterCodingException)
            else {
              decoder.flush(cb)
              Pull.output(cb2cc(cb)).as(None)
            }
        }
      }
    }
  }

  /** Representation of CSV data source, used to witness that certain sources may be used by read operations.
    * @see [[CSV$ CSV]] object.
    */
  sealed trait CSV[-A]

  /** Implicits to witness that given type is supported by reader as CSV source */
  object CSV {

    /** Witness that [[scala.io.Source]] may be used with reader functions */
    implicit object sourceWitness extends CSV[Source]

    /** Witness that [[java.io.InputStream]] may be used with reader functions */
    implicit object inputStreamWitness extends CSV[InputStream]

    /** Witness that [[java.nio.file.Path]] may be used with reader functions */
    implicit object pathWitness extends CSV[Path]

  }
}
