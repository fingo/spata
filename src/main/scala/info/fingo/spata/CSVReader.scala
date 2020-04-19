package info.fingo.spata

import java.io.IOException

import scala.io.Source
import cats.effect.IO
import fs2.{Pipe, Pull, Stream}
import info.fingo.spata.parser.{CharParser, FieldParser, ParsingErrorCode, RecordParser}
import info.fingo.spata.parser.RecordParser.ParsingResult
import info.fingo.spata.CSVReader.CSVCallback

/** A utility for parsing comma-separated values (CSV) sources.
  * The source is assumed to be [[https://tools.ietf.org/html/rfc4180 RFC 4180]] conform,
  * although some aspects of its format are configurable.
  *
  * The reader may be created by providing full configuration with [[CSVConfig]]
  * or through a helper [[CSVReader.config]] function from companion object, e.g.:
  * {{{ val reader = CSVReader.config.fieldDelimiter(';').get }}}
  *
  * Actual parsing is done through one of the 3 groups methods:
  *  - [[parse]] or [[pipe]] to get a stream of records and process data in a functional way,
  *    which is the recommended approach
  *  - [[load(source:scala\.io\.Source)* load]] to load whole source data at once into a list
  *  - [[process]] to deal with individual records through a callback function
  *
  * @constructor Creates reader with provided configuration.
  * @param config the configuration for CSV parsing (delimiters, header presence etc.)
  */
class CSVReader(config: CSVConfig) {

  /** Parses a CSV source and returns a stream of records.
    * The returned [[fs2.Stream]] allows further input processing in a very flexible, purely functional manner.
    *
    * The I/O operations are wrapped in [[cats.effect.IO]] allowing deferred computation.
    *
    * The caller of this function is responsible for proper resource acquisition and release.
    * This is optimally done with [[fs2.Stream.bracket]], e.g.:
    * {{{
    * val reader = CSVReader()
    * val stream = Stream
    *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
    *   .flatMap(reader.parse)
    * }}}
    *
    * Processing errors (I/O - [[IOException]], source structure - [[CSVException]],
    * string parsing - [[text.DataParseException DataParseException]]) should be handled with
    * [[fs2.Stream.handleErrorWith]]. If not handled, they will propagate as exceptions.
    *
    * @see [[https://fs2.io/ FS2]] documentation for guidance how to use stream library.
    *
    * @param source the source containing CSV content
    * @return the stream of records
    */
  def parse(source: Source): Stream[IO, CSVRecord] = {
    val cp = new CharParser(config.fieldDelimiter, config.recordDelimiter, config.quoteMark)
    val fp = new FieldParser(config.fieldSizeLimit)
    val rp = new RecordParser()
    val stream =
      Stream.fromIterator[IO][Char](source).through(cp.toCharResults).through(fp.toFields).through(rp.toRecords)
    val pull = if (config.hasHeader) contentWithHeader(stream) else contentWithoutHeader(stream)
    pull.stream.rethrow.flatMap(_.toRecords)
  }

  /** Converts a CSV source into records.
    * This is a wrapper of [[parse]] to use this reader with [[fs2.Stream.through]]:
    * {{{
    * val reader = CSVReader()
    * val stream = Stream
    *   .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
    *   .through(reader.pipe)
    * }}}
    *
    * @see [[parse]] for more information.
    *
    * @return a pipe to convert [[scala.io.Source]] into [[CSVRecord]]s
    */
  def pipe: Pipe[IO, Source, CSVRecord] = (in: Stream[IO, Source]) => in.flatMap(s => parse(s))

  /* Splits source data into header and actual content. */
  private def contentWithHeader(stream: Stream[IO, ParsingResult]) =
    stream.pull.uncons1.flatMap {
      case Some((h, t)) => Pull.output1(CSVContent(h, t, config.mapHeader))
      case None =>
        val err = ParsingErrorCode.MissingHeader
        Pull.raiseError[IO](new CSVException(err.message, err.code, 1, 0))
    }

  /* Adds numeric header to source data - provides record size to construct it. */
  private def contentWithoutHeader(stream: Stream[IO, ParsingResult]) =
    stream.pull.peek1.flatMap {
      case Some((h, s)) => Pull.output1(CSVContent(h.fieldNum, s, config.mapHeader))
      case None => Pull.output1(CSVContent(0, stream, config.mapHeader))
    }

  /** Loads whole source content into list of records.
    *
    * This function should be used only for small amounts of source data to avoid memory overflow.
    *
    * @param source the source containing CSV content
    * @return the list of records
    * @throws IOException in case of any I/O error
    * @throws CSVException in case of flawed CSV structure
    */
  @throws[IOException]("in case of any I/O error")
  @throws[CSVException]("in case of flawed CSV structure")
  def load(source: Source): List[CSVRecord] = load(source, None)

  /** Loads requested number of CSV records from source into a list.
    *
    * This functions stops processing source data as soon as the limit is reached.
    * It mustn't be called twice on the same source however - first call may consume more elements from iterable source
    * than required and leave the pointer at unpredictable position in source structure.
    *
    * @param source the source containing CSV content
    * @param limit the number of records to load
    * @return the list of records
    * @throws IOException in case of any I/O error
    * @throws CSVException in case of flawed CSV structure
    */
  @throws[IOException]("in case of any I/O error")
  @throws[CSVException]("in case of flawed CSV structure")
  def load(source: Source, limit: Long): List[CSVRecord] = load(source, Some(limit))

  /* Loads all or provided number of records into a list. */
  private def load(source: Source, limit: Option[Long]): List[CSVRecord] = {
    val stream = parse(source)
    val limited = limit match {
      case Some(l) => stream.take(l)
      case _ => stream
    }
    limited.compile.toList.unsafeRunSync()
  }

  /** Processes each CSV record with provided callback functions to execute some side effects.
    * Stops processing input as soon as the callback function returns false or end of data is reached.
    *
    * In addition to exceptions documented below this function may throw any exceptions from callback function,
    * e.g. [[text.DataParseException DataParseException]] resulting from CSV record parsing.
    *
    * @param source the source containing CSV content
    * @param cb the callback function to operate on each CSV record and produce some side effect.
    * It should return `true` to continue the process with next record or `false` to stop processing the source.
    * @throws IOException in case of any I/O error
    * @throws CSVException in case of flawed CSV structure
    */
  @throws[IOException]("in case of any I/O error")
  @throws[CSVException]("in case of flawed CSV structure")
  def process(source: Source)(cb: CSVCallback): Unit = {
    val effect = evalCallback(cb)
    val stream = parse(source).through(effect)
    stream.compile.drain.unsafeRunSync()
  }

  /* Callback function wrapper to enclose it in IO effect and letting the stream to evaluate it when run. */
  private def evalCallback(cb: CSVCallback): Pipe[IO, CSVRecord, Boolean] =
    _.evalMap(pr => IO.delay(cb(pr))).takeWhile(_ == true)
}

/** [[CSVReader]] companion object with types definitions and convenience methods to create readers. */
object CSVReader {

  /** Callback function type. */
  type CSVCallback = CSVRecord => Boolean

  /** Creates a [[CSVReader]] with default configuration, as defined in RFC 4180. */
  def apply: CSVReader = new CSVReader(config)

  /** Provides default configuration, as defined in RFC 4180. */
  def config: CSVConfig = CSVConfig()
}
