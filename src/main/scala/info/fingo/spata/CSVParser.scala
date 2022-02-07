/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import scala.util.Try
import cats.effect.{Async => CEAsync, Sync}
import fs2.{Pipe, Pull, Stream}
import info.fingo.spata.parser.{CharParser, FieldParser, RecordParser}
import info.fingo.spata.parser.RecordParser.RecordResult
import info.fingo.spata.CSVParser.Callback
import info.fingo.spata.error.{CSVException, ParsingErrorCode, StructureException}
import info.fingo.spata.util.Logger

/** A utility for parsing comma-separated values (CSV) sources.
  * The source is assumed to be [[https://tools.ietf.org/html/rfc4180 RFC 4180]] conform,
  * although some aspects of its format are configurable.
  *
  * The parser may be created with default configuration:
  * {{{ val parser = CSVParser[IO] }}}
  * or through [[CSVParser.config]] helper function to set custom properties:
  * {{{ val parser = CSVParser.config.fieldDelimiter(';').parser[IO] }}}
  *
  * Actual parsing is done through one of the 3 groups of methods:
  *  - [[parse]] to transform a stream of characters into records and process data in a functional way,
  *    which is the recommended approach,
  *  - [[get(stream:fs2\.Stream[F,Char])* get]] to fetch whole source data at once into a list,
  *  - [[process]] to deal with individual records through a callback function.
  *
  * This parser is normally used with stream fetching data from some external source,
  * so its computations are wrapped for deferred evaluation into an effect `F`, e.g. [[cats.effect.IO]].
  * Basic parsing does not impose any special requirements on `F`, except its support for suspended execution,
  * which requires implicit instance of [[cats.effect.Sync]].
  *
  * To trigger evaluation, one of the `unsafe` operations on `F` has to be called.
  * Their exact form depends on actual effect in use (e.g. [[cats.effect.IO.unsafeRunSync]]).
  *
  * No method in this class does context (thread) shift and by default they execute synchronously on current thread.
  * Concurrency or asynchronous execution may be introduced through various [[fs2.Stream]] methods.
  * There is also supporting class [[CSVParser#Async]] available, which provides method for asynchronous callbacks.
  *
  * @constructor Creates parser with provided configuration.
  * @param config the configuration for CSV parsing (delimiters, header presence etc.)
  * @tparam F the effect type, with a type class providing support for suspended execution
  * (typically [[cats.effect.IO]]) and logging (provided internally by spata)
  */
final class CSVParser[F[_]: Sync: Logger](config: CSVConfig) {

  /** Transforms stream of characters representing CSV data into records.
    * This function is intended to be used with [[fs2.Stream.through]].
    * The transformed [[fs2.Stream]] allows further input processing in a very flexible, purely functional manner.
    *
    * Processing of data sources may be achieved by combining this function with [[io.Reader]], e.g.:
    * {{{
    * val stream: Stream[IO, Record] = Stream
    *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
    *   .flatMap(Reader[IO].read)
    *   .through(CSVParser[IO].parse)
    * }}}
    *
    * Transformation may result in [[error.StructureException]], to be handled with [[fs2.Stream.handleErrorWith]].
    * If not handled, the exception will be thrown.
    *
    * @see [[https://fs2.io/ FS2]] documentation for further guidance.
    * @return a pipe to converter [[scala.Char]]s into [[Record]]s
    */
  def parse: Pipe[F, Char, Record] = (in: Stream[F, Char]) => {
    val cp = new CharParser[F](config.fieldDelimiter, config.recordDelimiter, config.quoteMark, config.trimSpaces)
    val fp = new FieldParser[F](config.fieldSizeLimit)
    val rp = new RecordParser[F]()
    val stream = Logger[F].infoS(s"Parsing CSV with $config") >>
      in.through(cp.toCharResults).through(fp.toFields).through(rp.toRecords)
    val pull = if (config.hasHeader) contentWithHeader(stream) else contentWithoutHeader(stream)
    pull.stream.rethrow
      .flatMap(_.toRecords)
      .through(debugCount)
      .handleErrorWith(ex => Logger[F].errorS(ex.getMessage) >> Stream.raiseError(ex))
      .onFinalize(Logger[F].debug("CSV parsing finished"))
  }

  /* Splits source data into header and actual content. */
  private def contentWithHeader(stream: Stream[F, RecordResult]) =
    stream.pull.uncons1.flatMap {
      case Some((h, t)) => Pull.output1(Content(h, t, config.headerMap))
      case None => Pull.raiseError[F](new StructureException(ParsingErrorCode.MissingHeader, Position.some(0, 1)))
    }

  /* Adds numeric header to source data - provides record size to construct it. */
  private def contentWithoutHeader(stream: Stream[F, RecordResult]) =
    stream.pull.peek1.flatMap {
      case Some((h, s)) => Pull.output1(Content(h.fieldNum, s, config.headerMap))
      case None => Pull.output1(Content(0, Stream.empty[F], config.headerMap))
    }

  /* Provided info about number of parsed records. This introduces additional overhead and is done in debug mode only.
   * Please note, that this information will be not available if stream processing ends prematurely -
   * in case of an error, as result of take(n) etc. */
  private def debugCount: Pipe[F, Record, Record] = in =>
    if (Logger[F].isDebug)
      in.noneTerminate
        .mapAccumulate(0) { (s, o) =>
          o match {
            case Some(r) => (r.rowNum, o)
            case None => (s, None)
          }
        }
        .flatMap { case (s, o) =>
          o match {
            case Some(r) => Stream.emit(r)
            case None => Logger[F].debugS(s"CSV fully parsed, $s rows processed") >> Stream.empty
          }
        }
    else in

  /** Fetches whole source content into list of records.
    *
    * This function should be used only for small source data sets to avoid memory overflow.
    *
    * @param stream the source stream containing CSV content
    * @return the list of records
    * @throws error.StructureException in case of flawed CSV structure
    */
  @throws[StructureException]("in case of flawed CSV structure")
  def get(stream: Stream[F, Char]): F[List[Record]] = get(stream, None)

  /** Fetches requested number of CSV records into a list.
    *
    * This functions stops processing source data as soon as the limit is reached.
    * It mustn't be called twice for the same data source however - first call may consume more elements than requested,
    * leaving the source pointer at an unpredictable position.
    *
    * @param stream the source stream containing CSV content
    * @param limit the number of records to get
    * @return the list of records
    * @throws error.StructureException in case of flawed CSV structure
    */
  @throws[StructureException]("in case of flawed CSV structure")
  def get(stream: Stream[F, Char], limit: Long): F[List[Record]] =
    get(stream, Some(limit))

  /* Loads all or provided number of records into a list. */
  private def get(stream: Stream[F, Char], limit: Option[Long]): F[List[Record]] = {
    val s = stream.through(parse)
    val limited = limit match {
      case Some(l) => s.take(l)
      case _ => s
    }
    limited.compile.toList
  }

  /** Processes each CSV record with provided callback functions to execute some side effects.
    * Stops processing input as soon as the callback function returns false or stream is exhausted.
    *
    * @param stream the source stream containing CSV content
    * @param cb the callback function to operate on each CSV record and produce some side effect.
    * It should return `true` to continue the process with next record or `false` to stop processing the input.
    * @return unit effect, used as a handle to trigger evaluation
    * @throws error.CSVException in case of flawed CSV structure or field parsing errors
    */
  @throws[CSVException]("in case of flawed CSV structure or field parsing errors")
  def process(stream: Stream[F, Char])(cb: Callback): F[Unit] = {
    val effect = evalCallback(cb)
    stream.through(parse).through(effect).compile.drain
  }

  /* Callback function wrapper to enclose it in effect F and let the stream evaluate it when run. */
  private def evalCallback(cb: Callback): Pipe[F, Record, Boolean] =
    _.evalMap(pr => Sync[F].delay(cb(pr))).takeWhile(_ == true)

  /** Provides access to asynchronous parsing method.
    *
    * @param F type class (monad) providing support for concurrency
    * @return helper class with asynchronous method
    */
  def async(implicit F: CEAsync[F]): CSVParser.Async[F] = new CSVParser.Async(this)
}

/** [[CSVParser]] companion object with types definitions and convenience methods to create parsers. */
object CSVParser {

  /** Callback function type. */
  type Callback = Record => Boolean

  /** Creates a [[CSVParser]] with default configuration, as defined in RFC 4180.
    *
    * @tparam F the effect type, with a type class providing support for suspended execution
    * (typically [[cats.effect.IO]]) and logging (provided internally by spata)
    * @return new parser
    */
  def apply[F[_]: Sync: Logger]: CSVParser[F] = new CSVParser(config)

  /** Provides default configuration, as defined in RFC 4180. */
  lazy val config: CSVConfig = CSVConfig()

  /** [[CSVParser]] helper with asynchronous parsing method.
    *
    * @param parser the regular parser
    * @tparam F the effect type, with type class providing support for concurrency (typically [[cats.effect.IO]])
    * and logging (provided internally by spata)
    */
  final class Async[F[_]: CEAsync: Logger] private[CSVParser] (parser: CSVParser[F]) {

    /** Processes each CSV record with provided callback functions to execute some side effects.
      * Stops processing input as soon as the callback function returns false, stream is exhausted or exception thrown.
      *
      * This function processes the callbacks asynchronously while retaining order of handled data.
      * The callbacks are run concurrently according to `maxConcurrent` parameter.
      *
      * Processing success or failure may be checked by examining by callback to method triggering effect evaluation
      * (e.g. [[cats.effect.IO.unsafeRunAsync]]) or handling error (e.g. with [[cats.effect.IO.handleErrorWith]]).
      *
      * @param stream the source stream containing CSV content
      * @param maxConcurrent maximum number of concurrently evaluated effects
      * @param cb the callback function to operate on each CSV record and produce some side effect;
      * it should return `true` to continue with next record or `false` to stop processing the source
      * @return unit effect, used as a handle to trigger evaluation
      */
    def process(stream: Stream[F, Char], maxConcurrent: Int = 1)(cb: Callback): F[Unit] = {
      val effect = evalCallback(maxConcurrent)(cb)
      stream.through(parser.parse).through(effect).compile.drain
    }

    /* Callback function wrapper to enclose it in effect F and let the stream evaluate it asynchronously when run. */
    private def evalCallback(maxConcurrent: Int)(cb: Callback): Pipe[F, Record, Boolean] =
      _.mapAsync(maxConcurrent) { pr =>
        CEAsync[F].async_[Boolean] { call =>
          val result = Try(cb(pr)).toEither
          call(result)
        }
      }.takeWhile(_ == true)
  }
}
