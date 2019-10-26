package info.fingo.spata

import java.io.IOException

import cats.effect.IO
import info.fingo.spata.parser.{CharParser, FieldParser, ParsingErrorCode, ParsingResult, RecordParser}

import scala.io.Source
import fs2.{Pull, Stream}
import info.fingo.spata.CSVReader.{CSVCallback, CSVErrHandler, IOErrHandler}

class CSVReader(separator: Char, fieldSizeLimit: Option[Int] = None) {

  private val recordDelimiter: Char = '\n'
  private val quote: Char = '"'

  def read(source: Source, cb: CSVCallback, ehCSV: CSVErrHandler, ehIO: IOErrHandler): Unit =
    read(source, cb, Some(ehCSV), Some(ehIO))
  def read(source: Source, cb: CSVCallback): Unit = read(source, cb, None, None)

  private def read(source: Source, cb: CSVCallback, ehoCSV: Option[CSVErrHandler], ehoIO: Option[IOErrHandler]): Unit = {
    val stream = parse(source)
    val pull = stream.pull.uncons1.flatMap {
      case Some((h, t)) => Pull.output1(CSVContent(h, t))
      case None =>
        val err = ParsingErrorCode.MissingHeader
        Pull.raiseError[IO](new CSVException(err.message, err.code, 1, 0))
    }
    val eh = errorHandler(ehoCSV, ehoIO)
    pull.stream.rethrow.flatMap(_.process(cb)).handleErrorWith(eh).compile.drain.unsafeRunSync()
  }

  private def parse(source: Source): Stream[IO,ParsingResult] = {
    val cp = new CharParser(separator, recordDelimiter, quote)
    val fp = new FieldParser(fieldSizeLimit)
    val rp = new RecordParser()
    Stream.fromIterator[IO][Char](source).through(cp.toCharResults()).through(fp.toFields()).through(rp.toRecords)
  }

  private def errorHandler(ehoCSV: Option[CSVErrHandler], ehoIO: Option[IOErrHandler]): Throwable => Stream[IO,Unit] = ex => {
    val eho = ex match {
      case ex: CSVException => ehoCSV.map(eh => Stream.emit(eh(ex)))
      case ex: IOException => ehoIO.map(eh => Stream.emit(eh(ex)))
      case _ => None
    }
    eho.getOrElse(Stream.raiseError[IO](ex))
  }
}

object CSVReader {
  type CSVCallback = CSVRow => Boolean
  type CSVErrHandler = CSVException => Unit
  type IOErrHandler = IOException => Unit
}
