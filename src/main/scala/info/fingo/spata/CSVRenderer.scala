/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import fs2.{Chunk, Pipe, Pull, RaiseThrowable, Stream}
import info.fingo.spata.error.{FieldInfo, HeaderError}

/** A utility for rendering data to CSV representation.
  *
  * The renderer may be created with default configuration:
  * ```
  * val renderer = CSVRenderer[IO]
  * ```
  * or through [[CSVRenderer.config]] helper function to set custom properties:
  * ```
  * val renderer = CSVRenderer.config.fieldDelimiter(';').renderer[IO]
  * ```
  *
  * Actual rendering is done through one of the 2 groups of methods:
  *  - [[render]] to transform a stream of records into stream of character, which represent full CSV content.
  *  - [[rows]] to convert records to strings representing individual CSV rows.
  *
  * This renderer is normally used with stream supplying data to some external destination,
  * so its computations are wrapped for deferred evaluation into an effect `F`, e.g. [[cats.effect.IO]].
  * Basic parsing does not impose any special requirements on `F`, except its support for raising and handling errors,
  * which requires given instance of [[fs2.RaiseThrowable]] which effectively means [[cats.ApplicativeError]].
  *
  * To trigger evaluation, one of the `unsafe` operations on `F` has to be called.
  * Their exact form depends on actual effect in use (e.g. [[cats.effect.IO.unsafeRunSync]]).
  *
  * No method in this class does context (thread) shift and by default they execute synchronously on current thread.
  * Concurrency or asynchronous execution may be introduced through various [[fs2.Stream]] methods.
  *
  * @param config the configuration for CSV rendering (delimiters, header presence etc.)
  * @tparam F the effect type, with a type class providing support for raising and handling errors
  */
final class CSVRenderer[F[_]: RaiseThrowable](config: CSVConfig):

  /* Convenience type representing result of creating CSV content. */
  private type EHE[A] = Either[HeaderError, A]

  // String versions of CSV special characters, provided for convenience.
  private val sfd = config.fieldDelimiter.toString
  private val srd = config.recordDelimiter.toString
  private val sq = config.quoteMark.toString

  /** Transforms stream of records into stream of characters representing CSV data.
    * This function is intended to be used with [[fs2.Stream.through]].
    *
    * The output is basically [[https://tools.ietf.org/html/rfc4180 RFC 4180]] conform,
    * with the possibility to have custom delimiters and quotes, as configured by [[CSVConfig]].
    *
    * If any record does not have the field required by header,
    * transformation will cause an [[error.HeaderError]], to be handled with [[fs2.Stream.handleErrorWith]].
    * If not handled, the exception will be thrown.
    *
    * @example
    * ```
    * val input: Stream[IO, Record] = ???
    * val renderer: CVSRenderer[IO] = CSVRenderer.config.escapeSpaces.renderer[IO]
    * val header: Header = Header("date", "location", "temperature")
    * val output: Stream[IO, Char] = input.through(renderer.render(header))
    * val eff: Stream[IO, Unit] = output.through(Writer[IO].write("output.csv"))
    * ```
    * @see [[https://fs2.io/ FS2]] documentation for further guidance.
    * @param header header to be potentially written into target stream and used to select required data from records
    * @return a pipe to converter [[Record]]s into [[scala.Char]]s
    */
  def render(header: Header): Pipe[F, Record, Char] = (in: Stream[F, Record]) =>
    val hs = if config.hasHeader then Stream.emit(renderHeader(header)) else Stream.empty
    val cs = in.map(renderRow(_, header))
    val stream = hs ++ cs
    stream.through(toChars)

  /** Transforms stream of records into stream of characters representing CSV data.
    * Determines header from first record in stream.
    * This function is intended to be used with [[fs2.Stream.through]].
    *
    * If field content differ (they have fields with different names),
    * transformation will cause an [[error.HeaderError]], to be handled with [[fs2.Stream.handleErrorWith]].
    *
    * @see [[render]] with explicit header for more information.
    * @return a pipe to converter [[Record]]s into [[scala.Char]]s
    */
  def render: Pipe[F, Record, Char] = (in: Stream[F, Record]) =>
    def loop(in: Stream[F, Record], header: Header): Pull[F, Either[HeaderError, String], Unit] =
      in.pull.uncons.flatMap {
        case Some((rc, t)) => Pull.output(rc.map(renderRow(_, header))) >> loop(t, header)
        case None => Pull.done
      }
    val pull = in.pull.uncons1.flatMap {
      case Some((r, t)) =>
        val headerRow = if config.hasHeader then Pull.output1(renderHeader(r.header)) else Pull.pure(())
        val firstRow = Pull.output1(renderRow(r, r.header))
        headerRow >> firstRow >> loop(t, r.header)
      case None => Pull.done
    }
    pull.stream.through(toChars)

  /** Transforms stream of records into stream of CSV rows.
    *
    * This method accesses records values by index. It does not use field names and does not require header to exist.
    * It does not require records to be the same length, either.
    * With records of different size the resulting output may not form proper CSV content.
    *
    * Records delimiters are to be added to the output. To get full CSV content, `Stream.intersperse` should be called:
    * {{{
    *   val renderer: CSVRenderer[IO] = CSVRenderer[IO]
    *   val in: Stream[IO, Record] = ???
    *   val out: Stream[IO, String] = in.through(renderer.rows).intersperse("\n")
    * }}}
    * Nevertheless record delimiter is escaped in content, so it has to be set accordingly in config.
    *
    * This method does not put header in output stream, regardless of `CSVConfig.hasHeader` setting.
    *
    * @return a pipe to converter [[Record]]s into `String`s
    */
  def rows: Pipe[F, Record, String] = (in: Stream[F, Record]) => in.map(_.values.map(escape).mkString(sfd))

  /* Converts stream of strings (or errors) into stream of characters.
   * The resulting stream may "throw" an error, which has to be handled with `handleErrorWith`.
   */
  private def toChars: Pipe[F, EHE[String], Char] =
    (in: Stream[F, EHE[String]]) =>
      in.rethrow
        .intersperse(srd)
        .map(s => Chunk.array[Char](s.toCharArray))
        .flatMap(Stream.chunk)

  /* Renders single record into CSV string. */
  private def renderRow(record: Record, header: Header): EHE[String] =
    header.names
      .map(name => record(name).map(escape).toRight(new HeaderError(Position.none, FieldInfo(name))))
      .foldRight[EHE[List[String]]](Right(Nil))((elm, seq) => elm.flatMap(s => seq.map(s :: _)))
      .map(_.mkString(sfd))

  /* Renders header into CSV string. */
  private def renderHeader(header: Header): Either[Nothing, String] = Right(header.names.map(escape).mkString(sfd))

  /* Escapes record field with delimiters or quote characters. */
  private def escape(s: String): String =
    val sdq = doubleQuotes(s)
    val sl = s.length
    config.escapeMode match
      case CSVConfig.EscapeRequired =>
        if sdq.length != sl || hasDelimiters(s) then sdq.mkString(sq, "", sq) else s
      case CSVConfig.EscapeSpaces =>
        if sdq.length != sl || hasDelimiters(s) || s.strip().length != sl then sdq.mkString(sq, "", sq) else s
      case CSVConfig.EscapeAll =>
        sdq.mkString(sq, "", sq)

  /* Duplicates quote characters to escape them. */
  private def doubleQuotes(s: String): String = if s.contains(sq) then s.replace(sq, sq * 2) else s

  private inline def hasDelimiters(s: String): Boolean =
    s.contains(config.fieldDelimiter.toChar) || s.contains(config.recordDelimiter.toChar)

/** [[CSVRenderer]] companion object with convenience methods to create renderers. */
object CSVRenderer:

  /** Creates a [[CSVRenderer]] with default configuration, as defined in RFC 4180.
    *
    * @tparam F the effect type, with a type class providing support for raising and handling errors
    * @return new renderer
    */
  def apply[F[_]: RaiseThrowable]: CSVRenderer[F] = new CSVRenderer(config)

  /** Provides default CSV configuration, as defined in RFC 4180. */
  lazy val config: CSVConfig = CSVConfig()
