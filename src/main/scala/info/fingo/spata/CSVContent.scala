/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import fs2.{RaiseThrowable, Stream}
import info.fingo.spata.error.StructureException
import info.fingo.spata.parser.RecordParser._

/* Intermediate entity used to converter raw records into key-values indexed by header.
 * It converts additionally CSV parsing failures into stream error by raising StructureException.
 */
private[spata] class CSVContent[F[_]: RaiseThrowable] private (
  data: Stream[F, RecordResult],
  header: CSVHeader,
  hasHeaderRecord: Boolean = true
) {
  /* Converts RawRecord into CSVRecord and raise RecordFailure as StructureException */
  def toRecords: Stream[F, CSVRecord] = data.map(wrapRecord).rethrow

  private def wrapRecord(rr: RecordResult): Either[StructureException, CSVRecord] = rr match {
    case RawRecord(fields, location, recordNum) =>
      CSVRecord(fields, location.line, recordNum - dataOffset)(header)
    case RecordFailure(code, location, recordNum, fieldNum) =>
      Left(
        new StructureException(
          code,
          location.line,
          recordNum - dataOffset,
          Some(location.position),
          header.get(fieldNum - 1)
        )
      )
  }

  /* First data record should be always at row 1, so record num has to be adjusted if header record is present. */
  private def dataOffset: Int = if (hasHeaderRecord) 1 else 0
}

/* CSVContent helper object. Used to create content for header and header-less data. */
private[spata] object CSVContent {

  /* Creates CSVContent for data with header. May return StructureException if no header is available (means empty source). */
  def apply[F[_]: RaiseThrowable](
    headerRecord: RecordResult,
    data: Stream[F, RecordResult],
    headerMap: S2S
  ): Either[StructureException, CSVContent[F]] =
    createHeader(headerRecord, headerMap) match {
      case Right(header) => Right(new CSVContent(data, header))
      case Left(e) => Left(e)
    }

  /* Creates CSVContent for data without header record - builds a numeric header. */
  def apply[F[_]: RaiseThrowable](
    headerSize: Int,
    data: Stream[F, RecordResult],
    headerMap: S2S
  ): Either[StructureException, CSVContent[F]] =
    Right(new CSVContent(data, CSVHeader(headerSize, headerMap), false))

  /* Build header for based on header record, remapping selected header values. */
  private def createHeader(rr: RecordResult, headerMap: S2S): Either[StructureException, CSVHeader] =
    rr match {
      case RawRecord(captions, _, _) => Right(CSVHeader(captions, headerMap))
      case RecordFailure(code, location, _, _) =>
        Left(new StructureException(code, location.line, 0, Some(location.position), None))
    }
}
