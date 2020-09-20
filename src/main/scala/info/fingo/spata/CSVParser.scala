/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import scala.util.Try
import cats.effect.{Concurrent, Sync}
import fs2.{Pipe, Pull, RaiseThrowable, Stream}
import info.fingo.spata.parser.{CharParser, FieldParser, ParsingErrorCode, RecordParser}
import info.fingo.spata.parser.RecordParser.RecordResult
import info.fingo.spata.CSVParser.CSVCallback

/** A utility for parsing comma-separated values (CSV) sources.
  * The source is assumed to be [[https://tools.ietf.org/html/rfc4180 RFC 4180]] conform,
  * although some aspects of its format are configurable.
  *
  * The parser may be created by providing full configuration with [[CSVConfig]]
  * or through a helper [[CSVParser.config]] function from companion object, e.g.:
  * {{{ val parser = CSVParser.config.fieldDelimiter(';').get[IO]() }}}
  *
  * Actual parsing is done through one of the 3 groups of methods:
  *  - [[parse]] to transform a stream of characters into records and process data in a functional way,
  *    which is the recommended approach,
  *  - [[get(stream:fs2\.Stream[F,Char])* get]] to fetch whole source data at once into a list,
  *  - [[process]] to deal with individual records through a callback function.
  *
  * This parser is normally used with stream fetching data from some external source,
  * so its computations are wrapped for deferred evaluation into an effect `F`, e.g. [[cats.effect.IO]].
  * Basic parsing does not impose any special requirements on `F`, except its support for raising and handling errors,
  * which requires implicit instance of [[fs2.RaiseThrowable]], meaning `ApplicativeError[F, Throwable]`
  * or `MonadError[F, Throwable]`.
  * Nevertheless, convenience methods for loading data into list or processing through callbacks
  * require presence of more complex type classes.
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
  * @tparam F the effect type, with a type class providing support for raising and handling errors
  * (typically [[cats.effect.IO]])
  */
class CSVParser[F[_]: RaiseThrowable](config: CSVConfig) {

  /** Transforms stream of characters representing CSV data into records.
    * This function is intended to be used with [[fs2.Stream.through]].
    * The transformed [[fs2.Stream]] allows further input processing in a very flexible, purely functional manner.
    *
    * Processing of data sources may be achieved by combining this function with [[io.reader]], e.g.:
    * {{{
    * val parser = CSVParser[IO]()
    * val stream: Stream[IO, CSVRecord] = Stream
    *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
    *   .flatMap(reader[IO].read)
    *   .through(parser.parse)
    * }}}
    *
    * Transformation may result in [[CSVStructureException]], to be handled with [[fs2.Stream.handleErrorWith]].
    * If not handled, the exception will be thrown.
    *
    * @see [[https://fs2.io/ FS2]] documentation for further guidance.
    *
    * @return a pipe to converter [[scala.Char]]s into [[CSVRecord]]s
    */
  def parse: Pipe[F, Char, CSVRecord] = (in: Stream[F, Char]) => {
    val cp = new CharParser[F](config.fieldDelimiter, config.recordDelimiter, config.quoteMark)
    val fp = new FieldParser[F](config.fieldSizeLimit)
    val rp = new RecordParser[F]()
    val stream = in.through(cp.toCharResults).through(fp.toFields).through(rp.toRecords)
    val pull = if (config.hasHeader) contentWithHeader(stream) else contentWithoutHeader(stream)
    pull.stream.rethrow.flatMap(_.toRecords)
  }

  /* Splits source data into header and actual content. */
  private def contentWithHeader(stream: Stream[F, RecordResult]) =
    stream.pull.uncons1.flatMap {
      case Some((h, t)) => Pull.output1(CSVContent(h, t, config.mapHeader))
      case None => Pull.raiseError[F](new CSVStructureException(ParsingErrorCode.MissingHeader, 1, 0))
    }

  /* Adds numeric header to source data - provides record size to construct it. */
  private def contentWithoutHeader(stream: Stream[F, RecordResult]) =
    stream.pull.peek1.flatMap {
      case Some((h, s)) => Pull.output1(CSVContent(h.fieldNum, s, config.mapHeader))
      case None => Pull.output1(CSVContent(0, Stream.empty[F], config.mapHeader))
    }

  /** Fetches whole source content into list of records.
    *
    * This function should be used only for small source data sets to avoid memory overflow.
    *
    * @param stream the source stream containing CSV content
    * @param F a type class (monad) providing suspended execution
    * @return the list of records
    * @throws CSVStructureException in case of flawed CSV structure
    */
  @throws[CSVStructureException]("in case of flawed CSV structure")
  def get(stream: Stream[F, Char])(implicit F: Sync[F]): F[List[CSVRecord]] = get(stream, None)

  /** Fetches requested number of CSV records into a list.
    *
    * This functions stops processing source data as soon as the limit is reached.
    * It mustn't be called twice for the same data source however - first call may consume more elements than requested,
    * leaving the source pointer at an unpredictable position.
    *
    * @param stream the source stream containing CSV content
    * @param limit the number of records to get
    * @param F a type class (monad) providing suspended execution
    * @return the list of records
    * @throws CSVStructureException in case of flawed CSV structure
    */
  @throws[CSVStructureException]("in case of flawed CSV structure")
  def get(stream: Stream[F, Char], limit: Long)(implicit F: Sync[F]): F[List[CSVRecord]] =
    get(stream, Some(limit))

  /* Loads all or provided number of records into a list. */
  private def get(stream: Stream[F, Char], limit: Option[Long])(implicit F: Sync[F]): F[List[CSVRecord]] = {
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
    * @param F a type class (monad) providing suspended execution
    * @return unit effect, used as a handle to trigger evaluation
    * @throws CSVException in case of flawed CSV structure or field parsing errors
    */
  @throws[CSVException]("in case of flawed CSV structure or field parsing errors")
  def process(stream: Stream[F, Char])(cb: CSVCallback)(implicit F: Sync[F]): F[Unit] = {
    val effect = evalCallback(cb)
    stream.through(parse).through(effect).compile.drain
  }

  /* Callback function wrapper to enclose it in effect F and let the stream evaluate it when run. */
  private def evalCallback(cb: CSVCallback)(implicit F: Sync[F]): Pipe[F, CSVRecord, Boolean] =
    _.evalMap(pr => F.delay(cb(pr))).takeWhile(_ == true)

  /** Provides access to asynchronous parsing method.
    *
    * @param F type class (monad) providing support for concurrency
    * @return helper class with asynchronous method
    */
  def async(implicit F: Concurrent[F]): CSVParser.Async[F] = new CSVParser.Async(this)
}

/** [[CSVParser]] companion object with types definitions and convenience methods to create parsers. */
object CSVParser {

  /** Callback function type. */
  type CSVCallback = CSVRecord => Boolean

  /** Creates a [[CSVParser]] with default configuration, as defined in RFC 4180.
    *
    * @tparam F the effect type, with a type class providing support for raising and handling errors
    * (typically [[cats.effect.IO]])
    * @return new parser
    */
  def apply[F[_]: RaiseThrowable](): CSVParser[F] = new CSVParser(config)

  /** Provides default configuration, as defined in RFC 4180. */
  lazy val config: CSVConfig = CSVConfig()

  /** [[CSVParser]] helper with asynchronous parsing method.
    *
    * @param parser the regular parser
    * @tparam F the effect type, with type class providing support for concurrency (typically [[cats.effect.IO]])
    */
  final class Async[F[_]: Concurrent] private[CSVParser] (parser: CSVParser[F]) {

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
    def process(stream: Stream[F, Char], maxConcurrent: Int = 1)(cb: CSVCallback): F[Unit] = {
      val effect = evalCallback(maxConcurrent)(cb)
      stream.through(parser.parse).through(effect).compile.drain
    }

    /* Callback function wrapper to enclose it in effect F and let the stream evaluate it asynchronously when run. */
    private def evalCallback(maxConcurrent: Int)(cb: CSVCallback): Pipe[F, CSVRecord, Boolean] =
      _.mapAsync(maxConcurrent) { pr =>
        Concurrent[F].async[Boolean] { call =>
          val result = Try(cb(pr)).toEither
          call(result)
        }
      }.takeWhile(_ == true)
  }
}
