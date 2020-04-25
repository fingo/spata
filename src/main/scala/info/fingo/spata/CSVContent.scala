/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import cats.effect.IO
import fs2.Stream
import info.fingo.spata.parser.RecordParser._

/* Intermediate entity used to converter raw records into key-values indexed by header.
 * It converts additionally CSV parsing failures into stream error by raising CSVStructureException.
 */
private[spata] class CSVContent private (
  data: Stream[IO, ParsingResult],
  index: Map[String, Int],
  hasHeader: Boolean = true
) {
  private val reverseIndex = index.map(x => x._2 -> x._1)

  /* Converts RawRecord into CSVRecord and raise ParsingFailure as CSVStructureException */
  def toRecords: Stream[IO, CSVRecord] = data.map(wrapRecord).rethrow

  private def wrapRecord(pr: ParsingResult): Either[CSVStructureException, CSVRecord] = pr match {
    case RawRecord(fields, location, recordNum) =>
      CSVRecord(fields, location.line, recordNum - dataOffset)(index)
    case ParsingFailure(code, location, recordNum, fieldNum) =>
      Left(
        new CSVStructureException(
          code,
          location.line,
          recordNum - dataOffset,
          Some(location.position),
          reverseIndex.get(fieldNum - 1)
        )
      )
  }

  /* First data record should be always at row 1, so record num has to be adjusted if header is present. */
  private def dataOffset: Int = if (hasHeader) 1 else 0
}

/* CSVContent helper object. Used to create content for header and header-less data. */
private[spata] object CSVContent {

  /* Creates CSVContent for data with header. May return CSVStructureException if no header is available (means empty source). */
  def apply(
    header: ParsingResult,
    data: Stream[IO, ParsingResult],
    headerMap: S2S
  ): Either[CSVStructureException, CSVContent] =
    buildHeaderIndex(header, headerMap) match {
      case Right(index) => Right(new CSVContent(data, index))
      case Left(e) => Left(e)
    }

  /* Creates CSVContent for data without header - builds a numeric header. */
  def apply(
    headerSize: Int,
    data: Stream[IO, ParsingResult],
    headerMap: S2S
  ): Either[CSVStructureException, CSVContent] =
    Right(new CSVContent(data, buildNumHeader(headerSize, headerMap), false))

  /* Build index for header with remapping selected header values. */
  private def buildHeaderIndex(pr: ParsingResult, headerMap: S2S): Either[CSVStructureException, Map[String, Int]] =
    pr match {
      case RawRecord(captions, _, _) => Right(captions.map(hRemap(_, headerMap)).zipWithIndex.toMap)
      case ParsingFailure(code, location, _, _) =>
        Left(new CSVStructureException(code, location.line, 0, Some(location.position), None))
    }

  /* Tuple-style header: _1, _2, _3 etc. (if not remapped). */
  private def buildNumHeader(size: Int, headerMap: S2S) =
    (0 until size).map(i => hRemap(s"_${i + 1}", headerMap) -> i).toMap

  /* Remap provided header values, leave intact the rest. */
  private def hRemap(header: String, f: S2S): String = if (f.isDefinedAt(header)) f(header) else header
}
