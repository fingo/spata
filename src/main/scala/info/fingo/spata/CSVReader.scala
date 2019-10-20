package info.fingo.spata

import java.io.IOException

import cats.effect.IO
import info.fingo.spata.parser.{CharParser, FieldParser, ParsingErrorCode, ParsingResult, RecordParser}

import scala.io.Source
import fs2.{Pull, Stream}
import info.fingo.spata.CSVReader.{CSVCallback, CSVErrorHandler, IOErrorHandler}

class CSVReader(separator: Char, fieldSizeLimit: Option[Int] = None) {

  private val recordDelimiter: Char = '\n'
  private val quote: Char = '"'

  def read(source: Source, cb: CSVCallback, ehCSV: CSVErrorHandler = throw _, ehIO: IOErrorHandler = throw _): Unit = {
    val stream = parse(source)
    val pull = stream.pull.uncons1.flatMap {
      case Some((h, t)) => Pull.output1(CSVContent(h, t))
      case None =>
        val err = ParsingErrorCode.MissingHeader
        throw new CSVException(err.message, err.code, 1, 0)
    }
    val eh = errorHandler(ehCSV, ehIO)
    pull.stream.flatMap(_.process(cb)).handleErrorWith(e => Stream.emit(eh(e))).compile.drain.unsafeRunSync()
  }

  private def parse(source: Source): Stream[IO,ParsingResult] = {
    val cp = new CharParser(separator, recordDelimiter, quote)
    val fp = new FieldParser(fieldSizeLimit)
    val rp = new RecordParser()
    Stream.fromIterator[IO][Char](source).through(cp.toCharResults()).through(fp.toFields()).through(rp.toRecords)
  }

  private def errorHandler(ehCSV: CSVErrorHandler, ehIO: IOErrorHandler): Throwable => Unit = {
    case ex: CSVException => ehCSV(ex)
    case ex: IOException => ehIO(ex)
    case ex => throw ex
  }
}

object CSVReader {
  type CSVCallback = CSVRow => Boolean
  type CSVErrorHandler = CSVException => Unit
  type IOErrorHandler = IOException => Unit
}
