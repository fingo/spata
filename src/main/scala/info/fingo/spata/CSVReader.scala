package info.fingo.spata

import cats.effect.IO
import info.fingo.spata.parser.{CharParser, FieldParser, ParsingResult, RecordParser}

import scala.io.Source
import fs2.{Pull, Stream}
import info.fingo.spata.CSVReader.CSVCallback

class CSVReader(separator: Char, fieldSizeLimit: Option[Int] = None) {

  private val recordDelimiter: Char = '\n'
  private val quote: Char = '"'

  def read(source: Source, cb: CSVCallback): Unit = {
    val stream = parse(source)
    val pull = stream.pull.uncons1.flatMap {
      case Some((h, t)) => Pull.output1(CSVContent(h, t))
      case None => throw new RuntimeException // TODO: handle this case
    }
    pull.stream.flatMap(_.process(cb)).compile.drain.unsafeRunSync()
  }

  private def parse(source: Source): Stream[IO,ParsingResult] = {
    val cp = new CharParser(separator, recordDelimiter, quote)
    val fp = new FieldParser(fieldSizeLimit)
    val rp = new RecordParser()
    Stream.fromIterator[IO][Char](source).through(cp.toCharResults()).through(fp.toFields()).through(rp.toRecords)
  }
}

object CSVReader {
  type CSVCallback = CSVRow => Boolean
}
