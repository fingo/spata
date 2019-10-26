package info.fingo.spata

import cats.effect.IO
import fs2.Stream
import info.fingo.spata.CSVReader.CSVCallback
import info.fingo.spata.parser.{ParsingFailure, ParsingResult, RawRecord}

private[spata] class CSVContent private (data: Stream[IO,ParsingResult], index: Map[String,Int]) {

  private val rIndex = buildReverseHeaderIndex(index)

  def process(cb: CSVCallback): Stream[IO, Boolean] = {
    data.map(wrapRow).rethrow.evalMap(pr => IO.delay(cb(pr))).takeWhile(_ == true)
  }

  private def buildReverseHeaderIndex(hi: Map[String,Int]): Map[Int,String] = hi.map(x => x._2 -> x._1)

  private def wrapRow(pr: ParsingResult): Either[CSVException,CSVRow] = pr match {
    case RawRecord(fields, location, recordNum) =>
      CSVRow(fields, location.line, recordNum-1)(index)   // -1 because of header
    case ParsingFailure(code, location, recordNum, fieldNum) =>
      Left(new CSVException(
        code.message,
        code.toString,
        location.line,
        recordNum-1,   // -1 because of header
        location.position,
        rIndex.get(fieldNum - 1)
      ))
  }
}

object CSVContent {
  def apply(header: ParsingResult, data: Stream[IO,ParsingResult]): Either[CSVException,CSVContent] =
    buildHeaderIndex(header) match {
      case Right(index) => Right(new CSVContent(data, index))
      case Left(e) => Left(e)
  }

  private def buildHeaderIndex(pr: ParsingResult): Either[CSVException,Map[String,Int]] = pr match {
    case RawRecord(captions, _, _) => Right(captions.zipWithIndex.toMap)
    case ParsingFailure(code, location, _, _) =>
      Left(new CSVException(code.message, code.toString, location.line, 0, location.position, None))
  }
}
