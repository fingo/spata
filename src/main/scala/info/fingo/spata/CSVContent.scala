package info.fingo.spata

import cats.effect.IO
import fs2.Stream
import info.fingo.spata.CSVReader.CSVCallback
import info.fingo.spata.parser.{ParsingFailure, ParsingResult, RawRecord}

private[spata] case class CSVContent(header: ParsingResult, data: Stream[IO,ParsingResult]) {

  private val index = buildHeaderIndex(header)
  private val rIndex = buildReverseHeaderIndex(index)

  def process(cb: CSVCallback): Stream[IO, Boolean] = {
    data.map(wrapRow).evalMap(pr => IO.delay(cb(pr))).takeWhile(_ == true)
  }

  private def buildHeaderIndex(pr: ParsingResult): Map[String,Int] = pr match {
    case RawRecord(captions, _, _) => captions.zipWithIndex.toMap
    case ParsingFailure(code, _, _, _) => throw new CSVException(code.message, code.toString) // TODO: add better info
  }

  private def buildReverseHeaderIndex(hi: Map[String,Int]): Map[Int,String] = hi.map(x => x._2 -> x._1)

  private def wrapRow(pr: ParsingResult): CSVRow = pr match {
    case RawRecord(fields, location, recordNum) =>
      new CSVRow(fields, location.line, recordNum-1)(index)   // -1 because of header
    case ParsingFailure(code, counters, recordNum, fieldNum) =>
      throw new CSVException(
        code.message,
        code.toString,
        Some(counters.line),
        Some(counters.position),
        Some(recordNum-1),   // -1 because of header
        rIndex.get(fieldNum - 1)
      )
  }
}
