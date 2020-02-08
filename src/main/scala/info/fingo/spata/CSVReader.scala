package info.fingo.spata

import java.io.IOException

import scala.io.Source
import cats.effect.IO
import fs2.{Pipe, Pull, Stream}
import info.fingo.spata.parser.{CharParser, FieldParser, ParsingErrorCode, ParsingResult, RecordParser}
import info.fingo.spata.CSVReader.{CSVCallback, CSVErrHandler, IOErrHandler}

class CSVReader(config: CSVConfig) {
  def parse(source: Source): Stream[IO, CSVRecord] = {
    val cp = new CharParser(config.fieldDelimiter, config.recordDelimiter, config.quoteMark)
    val fp = new FieldParser(config.fieldSizeLimit)
    val rp = new RecordParser()
    val stream =
      Stream.fromIterator[IO][Char](source).through(cp.toCharResults()).through(fp.toFields()).through(rp.toRecords)
    val pull = if (config.hasHeader) contentWithHeader(stream) else contentWithoutHeader(stream)
    pull.stream.rethrow.flatMap(_.toRecords)
  }
  private def contentWithHeader(stream: Stream[IO, ParsingResult]) = stream.pull.uncons1.flatMap {
    case Some((h, t)) => Pull.output1(CSVContent(h, t))
    case None =>
      val err = ParsingErrorCode.MissingHeader
      Pull.raiseError[IO](new CSVException(err.message, err.code, 1, 0))
  }
  private def contentWithoutHeader(stream: Stream[IO, ParsingResult]) = stream.pull.peek1.flatMap {
    case Some((h, s)) => Pull.output1(CSVContent(h.fieldNum, s))
    case None => Pull.output1(CSVContent(0, stream))
  }

  def load(source: Source): List[CSVRecord] = load(source, None)
  def load(source: Source, limit: Long): List[CSVRecord] = load(source, Some(limit))

  private def load(source: Source, limit: Option[Long]): List[CSVRecord] = {
    val stream = parse(source)
    val limited = limit match {
      case Some(l) => stream.take(l)
      case _ => stream
    }
    limited.compile.toList.unsafeRunSync()
  }

  def process(source: Source, cb: CSVCallback, ehCSV: CSVErrHandler, ehIO: IOErrHandler): Unit =
    process(source, cb, Some(ehCSV), Some(ehIO))
  def process(source: Source, cb: CSVCallback): Unit = process(source, cb, None, None)

  private def process(
    source: Source,
    cb: CSVCallback,
    ehoCSV: Option[CSVErrHandler],
    ehoIO: Option[IOErrHandler]
  ): Unit = {
    val effect = evalCallback(cb)
    val eh = errorHandler(ehoCSV, ehoIO)
    val stream = parse(source).through(effect).handleErrorWith(eh)
    stream.compile.drain.unsafeRunSync()
  }

  private def evalCallback(cb: CSVCallback): Pipe[IO, CSVRecord, Boolean] =
    _.evalMap(pr => IO.delay(cb(pr))).takeWhile(_ == true)

  private def errorHandler(ehoCSV: Option[CSVErrHandler], ehoIO: Option[IOErrHandler]): Throwable => Stream[IO, Unit] =
    ex => {
      val eho = ex match {
        case ex: CSVException => ehoCSV.map(eh => Stream.emit(eh(ex)))
        case ex: IOException => ehoIO.map(eh => Stream.emit(eh(ex)))
        case _ => None
      }
      eho.getOrElse(Stream.raiseError[IO](ex))
    }
}

object CSVReader {
  type CSVCallback = CSVRecord => Boolean
  type CSVErrHandler = CSVException => Unit
  type IOErrHandler = IOException => Unit

  def apply: CSVReader = new CSVReader(config)
  def config: CSVConfig = CSVConfig()
}
