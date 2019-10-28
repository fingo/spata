package info.fingo.spata

import cats.effect.IO
import fs2.Stream
import info.fingo.spata.parser.{ParsingFailure, ParsingResult, RawRecord}

private[spata] class CSVContent private (data: Stream[IO,ParsingResult], index: Map[String,Int]) {

  private val rIndex = buildReverseHeaderIndex(index)

  def toRecords: Stream[IO, CSVRecord] = data.map(wrapRecord).rethrow

  private def buildReverseHeaderIndex(hi: Map[String,Int]): Map[Int,String] = hi.map(x => x._2 -> x._1)

  private def wrapRecord(pr: ParsingResult): Either[CSVException,CSVRecord] = pr match {
    case RawRecord(fields, location, recordNum) =>
      CSVRecord(fields, location.line, recordNum-1)(index)   // -1 because of header
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
