/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import scala.util.Try
import cats.effect.{Concurrent, IO}
import fs2.{Pipe, Pull, Stream}
import info.fingo.spata.parser.{CharParser, FieldParser, ParsingErrorCode, RecordParser}
import info.fingo.spata.parser.RecordParser.ParsingResult
import info.fingo.spata.CSVParser.CSVCallback

/** A utility for parsing comma-separated values (CSV) sources.
  * The source is assumed to be [[https://tools.ietf.org/html/rfc4180 RFC 4180]] conform,
  * although some aspects of its format are configurable.
  *
  * The parser may be created by providing full configuration with [[CSVConfig]]
  * or through a helper [[CSVParser.config]] function from companion object, e.g.:
  * {{{ val parser = CSVParser.config.fieldDelimiter(';').get }}}
  *
  * Actual parsing is done through one of the 3 groups methods:
  *  - [[parse]] to transform a stream of characters into records and process data in a functional way,
  *    which is the recommended approach
  *  - [[get(stream:fs2\.Stream[cats\.effect\.IO,Char])* get]] to fetch whole source data at once into a list
  *  - [[process]] to deal with individual records through a callback function.
  *
  * This parser is normally used with stream fetching data from some external source,
  * so its computations are wrapped into [[cats.effect.IO]] for deferred evaluation.
  * To trigger evaluation one of the `unsafe` operations on `IO`
  * (e.g. [[cats.effect.IO.unsafeRunSync]]) has to be called.
  *
  * No method in this class does context shift and by default they execute synchronously on current thread.
  * Concurrency or asynchronous execution may be introduced through various [[fs2.Stream]] methods.
  * There is also supporting class [[CSVParser#Async]] available, which provides method for asynchronous callbacks.
  *
  * @constructor Creates parser with provided configuration.
  * @param config the configuration for CSV parsing (delimiters, header presence etc.)
  */
class CSVParser(config: CSVConfig) {

  /** Transforms into records stream of characters representing CSV data.
    * This function is intended to be used with [[fs2.Stream.through]].
    * The transformed [[fs2.Stream]] allows further input processing in a very flexible, purely functional manner.
    *
    * Processing of data sources may be achieved by combining this function with [[io.reader]], e.g.:
    * {{{
    * val parser = CSVParser()
    * val stream: Stream[IO, CSVRecord] = Stream
    *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
    *   .flatMap(reader(_))
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
  def parse: Pipe[IO, Char, CSVRecord] = (in: Stream[IO, Char]) => {
    val cp = new CharParser(config.fieldDelimiter, config.recordDelimiter, config.quoteMark)
    val fp = new FieldParser(config.fieldSizeLimit)
    val rp = new RecordParser()
    val stream = in.through(cp.toCharResults).through(fp.toFields).through(rp.toRecords)
    val pull = if (config.hasHeader) contentWithHeader(stream) else contentWithoutHeader(stream)
    pull.stream.rethrow.flatMap(_.toRecords)
  }

  /* Splits source data into header and actual content. */
  private def contentWithHeader(stream: Stream[IO, ParsingResult]) =
    stream.pull.uncons1.flatMap {
      case Some((h, t)) => Pull.output1(CSVContent(h, t, config.mapHeader))
      case None => Pull.raiseError[IO](new CSVStructureException(ParsingErrorCode.MissingHeader, 1, 0))
    }

  /* Adds numeric header to source data - provides record size to construct it. */
  private def contentWithoutHeader(stream: Stream[IO, ParsingResult]) =
    stream.pull.peek1.flatMap {
      case Some((h, s)) => Pull.output1(CSVContent(h.fieldNum, s, config.mapHeader))
      case None => Pull.output1(CSVContent(0, stream, config.mapHeader))
    }

  /** Fetches whole source content into list of records.
    *
    * This function should be used only for small amounts of source data to avoid memory overflow.
    *
    * @param stream the source stream containing CSV content
    * @return the list of records
    * @throws CSVStructureException in case of flawed CSV structure
    */
  @throws[CSVStructureException]("in case of flawed CSV structure")
  def get(stream: Stream[IO, Char]): IO[List[CSVRecord]] = get(stream, None)

  /** Fetches requested number of CSV records into a list.
    *
    * This functions stops processing source data as soon as the limit is reached.
    * It mustn't be called twice for the same data source however - first call may consume more elements than requested,
    * leaving the source pointer at an unpredictable position.
    *
    * @param stream the source stream containing CSV content
    * @param limit the number of records to get
    * @return the list of records
    * @throws CSVStructureException in case of flawed CSV structure
    */
  @throws[CSVStructureException]("in case of flawed CSV structure")
  def get(stream: Stream[IO, Char], limit: Long): IO[List[CSVRecord]] = get(stream, Some(limit))

  /* Loads all or provided number of records into a list. */
  private def get(stream: Stream[IO, Char], limit: Option[Long]): IO[List[CSVRecord]] = {
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
    * It should return `true` to continue the process with next record or `false` to stop processing the source.
    * @return
    * @throws CSVException in case of flawed CSV structure or field parsing errors
    */
  @throws[CSVException]("in case of flawed CSV structure or field parsing errors")
  def process(stream: Stream[IO, Char])(cb: CSVCallback): IO[Unit] = {
    val effect = evalCallback(cb)
    stream.through(parse).through(effect).compile.drain
  }

  /* Callback function wrapper to enclose it in IO effect and letting the stream to evaluate it when run. */
  private def evalCallback(cb: CSVCallback): Pipe[IO, CSVRecord, Boolean] =
    _.evalMap(pr => IO.delay(cb(pr))).takeWhile(_ == true)

  /** Provides access to asynchronous parsing method.
    *
    * @param cc implicit instance of type class to provide support for concurrency
    * @return helper class with asynchronous method
    */
  def async(implicit cc: Concurrent[IO]): CSVParser.Async = new CSVParser.Async(this)(cc)
}

/** [[CSVParser]] companion object with types definitions and convenience methods to create parsers. */
object CSVParser {

  /** Callback function type. */
  type CSVCallback = CSVRecord => Boolean

  /** Creates a [[CSVParser]] with default configuration, as defined in RFC 4180. */
  def apply(): CSVParser = new CSVParser(config)

  /** Provides default configuration, as defined in RFC 4180. */
  def config: CSVConfig = CSVConfig()

  /** [[CSVParser]] helper with asynchronous parsing method.
    *
    * @param parser the regular parser
    * @param cc implicit instance of type class to provide support for concurrency
    */
  final class Async private[CSVParser] (parser: CSVParser)(implicit cc: Concurrent[IO]) {

    /** Processes each CSV record with provided callback functions to execute some side effects.
      * Stops processing input as soon as the callback function returns false, stream is exhausted or exception thrown.
      *
      * This function process the callbacks asynchronously while retaining order of handled data.
      * The callback are run concurrently according to `maxConcurrent` parameter.
      *
      * Processing success or failure may be checked by examining by callback to [[cats.effect.IO.unsafeRunAsync]]
      * or handling error with [[cats.effect.IO.handleErrorWith]].
      *
      * @param stream the source stream containing CSV content
      * @param maxConcurrent maximum number of concurrently evaluated effects
      * @param cb the callback function to operate on each CSV record and produce some side effect.
      * It should return `true` to continue with next record or `false` to stop processing the source.
      * @return
      */
    def process(stream: Stream[IO, Char], maxConcurrent: Int = 1)(cb: CSVCallback): IO[Unit] = {
      val effect = evalCallback(maxConcurrent)(cb)
      stream.through(parser.parse).through(effect).compile.drain
    }

    /* Callback function wrapper to enclose it in IO effect and letting the stream to evaluate it asynchronously when run. */
    private def evalCallback(maxConcurrent: Int)(cb: CSVCallback): Pipe[IO, CSVRecord, Boolean] =
      _.mapAsync(maxConcurrent) { pr =>
        IO.async[Boolean] { call =>
          val result = Try(cb(pr)).toEither
          call(result)
        }
      }.takeWhile(_ == true)
  }
}
