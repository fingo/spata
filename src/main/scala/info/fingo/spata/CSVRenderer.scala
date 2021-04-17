/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import cats.effect.Sync
import fs2.{Chunk, Pipe, Pull, RaiseThrowable, Stream}
import info.fingo.spata.error.HeaderError
import info.fingo.spata.util.Logger

/** A utility for rendering data to CSV representation.
  *
  * The renderer may be created with default configuration:
  * {{{ val renderer = CSVRenderer() }}}
  * or through [[CSVRenderer.config]] helper function to set custom properties:
  * {{{ val renderer = CSVRenderer.config.fieldDelimiter(';').renderer[IO]() }}}
  *
  * Actual rendering is done through one of the 2 groups of methods:
  *  - [[render(* render]] to transform a stream of records into stream of character, which represent full CSV content.
  *  - [[rows]] to convert records to strings representing individual CSV rows.
  *
  * This renderer is normally used with stream supplying data to some external destination,
  * so its computations are wrapped for deferred evaluation into an effect `F`, e.g. [[cats.effect.IO]].
  * Basic parsing does not impose any special requirements on `F`, except its support for raising and handling errors,
  * which requires implicit instance of [[fs2.RaiseThrowable]] which effectively means [[cats.ApplicativeError]].
  *
  * To trigger evaluation, one of the `unsafe` operations on `F` has to be called.
  * Their exact form depends on actual effect in use (e.g. [[cats.effect.IO.unsafeRunSync]]).
  *
  * No method in this class does context (thread) shift and by default they execute synchronously on current thread.
  * Concurrency or asynchronous execution may be introduced through various [[fs2.Stream]] methods.
  *
  * @param config the configuration for CSV rendering (delimiters, header presence etc.)
  * @tparam F the effect type, with a type class providing support for suspended execution
  * (typically [[cats.effect.IO]]) and logging (provided internally by spata)
  */
class CSVRenderer[F[_]: RaiseThrowable](config: CSVConfig) {

  private val sfd = config.fieldDelimiter.toString
  private val srd = config.recordDelimiter.toString
  private val sq = config.quoteMark.toString

  /** Transforms stream of records into stream of characters representing CSV data.
    * This function is intended to be used with [[fs2.Stream.through]].
    *
    * The output is basically [[https://tools.ietf.org/html/rfc4180 RFC 4180]] conform,
    * with the possibility to have custom delimiters and quotes, as configured in [[CSVConfig]].
    *
    * If any record does not have the field required by header,
    * transformation will cause an [[error.HeaderError]], to be handled with [[fs2.Stream.handleErrorWith]].
    * If not handled, the exception will be thrown.
    *
    * @example
    * {{{
    *   val input: Stream[IO, Record] = ???
    *   val renderer = CSVRenderer.config.escapeSpaces().renderer[IO]()
    *   val header = Header("date", "location", "temperature")
    *   val output = input.through(renderer.render(header))
    * }}}
    * @see [[https://fs2.io/ FS2]] documentation for further guidance.
    * @param header header to be potentially written into target stream and used to select required data from records
    * @return a pipe to converter [[Record]]s into [[scala.Char]]s
    */
  def render(header: Header): Pipe[F, Record, Char] = (in: Stream[F, Record]) => {
    val hs = Stream.emit(renderHeader(header))
    val cs = in.map(renderRow(_, header))
    val stream = if (config.hasHeader) hs ++ cs else cs
    stream.through(toChars)
  }

  /** Transforms stream of records into stream of characters representing CSV data.
    * Determines header from first record in stream.
    * This function is intended to be used with [[fs2.Stream.through]].
    *
    * If field content differ (they have fields with different names),
    * transformation will cause an [[error.HeaderError]], to be handled with [[fs2.Stream.handleErrorWith]].
    *
    * @see [[render(* render]] with explicit header for more information.
    * @return a pipe to converter [[Record]]s into [[scala.Char]]s
    */
  def render: Pipe[F, Record, Char] = (in: Stream[F, Record]) => {
    def loop(in: Stream[F, Record], header: Header): Pull[F, Either[HeaderError, String], Unit] =
      in.pull.uncons.flatMap {
        case Some((rc, t)) => Pull.output(rc.map(renderRow(_, header))) >> loop(t, header)
        case None => Pull.done
      }
    val pull = in.pull.uncons1.flatMap {
      case Some((r, t)) => {
        val headerRow = if (config.hasHeader) Pull.output1(renderHeader(r.header)) else Pull.pure(())
        val firstRow = Pull.output1(renderRow(r, r.header))
        headerRow >> firstRow >> loop(t, r.header)
      }
      case None => Pull.done
    }
    pull.stream.through(toChars)
  }

  /** Transforms stream of records into stream of CSV rows.
    *
    * This method accesses records values by index. It does not use field names and does not require header to exist.
    * It does not require records to be the same length, either.
    * With records of different size the resulting output may not form proper CSV content.
    *
    * Records delimiters are to added to the output. To get full CSV content, `Stream.intersperse` should be called:
    * {{{
    *   val renderer: CSVRenderer = CSVRenderer()
    *   val in: Stream[IO, Record] = ???
    *   val out: Stream[IO, String] = in.through(renderer.rows).intersperse("\n")
    * }}}
    * Nevertheless record delimiter is properly escaped in content, so it has to be set accordingly in config.
    *
    * This method does not put header in output stream, regardless of `CSVConfig.hasHeader` setting.
    *
    * @return
    */
  def rows: Pipe[F, Record, String] = (in: Stream[F, Record]) => in.map(_.values.map(escape).mkString(sfd))

  private def toChars: Pipe[F, Either[HeaderError, String], Char] =
    (in: Stream[F, Either[HeaderError, String]]) =>
      in.rethrow
        .intersperse(srd)
        .map(s => Chunk.chars(s.toCharArray))
        .flatMap(Stream.chunk)

  private def renderRow(record: Record, header: Header): Either[HeaderError, String] =
    header.names.map { name =>
      record(name).map(escape).toRight(new HeaderError(Position.none(), name))
    }.foldRight[Either[HeaderError, List[String]]](Right(Nil))((elm, seq) => elm.flatMap(s => seq.map(s :: _)))
      .map(_.mkString(sfd))

  private def renderHeader(header: Header): Either[Nothing, String] = Right(header.names.map(escape).mkString(sfd))

  private def escape(s: String): String = {
    val sdq = doubleQuotes(s)
    val sl = s.length
    config.escapeMode match {
      case CSVConfig.EscapeRequired => if (sdq.length != sl || hasDelimiters(s)) sdq.mkString(sq, "", sq) else s
      case CSVConfig.EscapeSpaces =>
        if (sdq.length != sl || hasDelimiters(s) || s.strip().length != sl) sdq.mkString(sq, "", sq) else s
      case CSVConfig.EscapeAll =>
        sdq.mkString(sq, "", sq)
    }
  }

  private def doubleQuotes(s: String): String = if (s.contains(sq)) s.replace(sq, sq * 2) else s

  @inline private def hasDelimiters(s: String): Boolean =
    s.contains(config.fieldDelimiter) || s.contains(config.recordDelimiter)
}

/** [[CSVRenderer]] companion object with convenience methods to create renderers. */
object CSVRenderer {

  /** Creates a [[CSVRenderer]] with default configuration, as defined in RFC 4180.
    *
    * @tparam F the effect type, with a type class providing support for suspended execution
    * (typically [[cats.effect.IO]]) and logging (provided internally by spata)
    * @return new renderer
    */
  def apply[F[_]: Sync: Logger](): CSVRenderer[F] = new CSVRenderer(config)

  /** Provides default configuration, as defined in RFC 4180. */
  lazy val config: CSVConfig = CSVConfig()
}
