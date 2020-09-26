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
private[spata] class Content[F[_]: RaiseThrowable] private (
  data: Stream[F, RecordResult],
  header: Header,
  hasHeaderRecord: Boolean = true
) {
  /* Converts RawRecord into Record and raise RecordFailure as StructureException */
  def toRecords: Stream[F, Record] = data.map(wrapRecord).rethrow

  private def wrapRecord(rr: RecordResult): Either[StructureException, Record] = rr match {
    case RawRecord(fields, location, recordNum) =>
      Record(fields, location.line, recordNum - dataOffset)(header)
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

/* Content helper object. Used to create content for header and header-less data. */
private[spata] object Content {

  /* Creates Content for data with header. May return StructureException if no header is available (means empty source). */
  def apply[F[_]: RaiseThrowable](
    headerRecord: RecordResult,
    data: Stream[F, RecordResult],
    headerMap: S2S
  ): Either[StructureException, Content[F]] =
    createHeader(headerRecord, headerMap) match {
      case Right(header) => Right(new Content(data, header))
      case Left(e) => Left(e)
    }

  /* Creates Content for data without header record - builds a numeric header. */
  def apply[F[_]: RaiseThrowable](
    headerSize: Int,
    data: Stream[F, RecordResult],
    headerMap: S2S
  ): Either[StructureException, Content[F]] =
    Right(new Content(data, Header(headerSize, headerMap), false))

  /* Build header for based on header record, remapping selected header values. */
  private def createHeader(rr: RecordResult, headerMap: S2S): Either[StructureException, Header] =
    rr match {
      case RawRecord(captions, _, _) => Right(Header(captions, headerMap))
      case RecordFailure(code, location, _, _) =>
        Left(new StructureException(code, location.line, 0, Some(location.position), None))
    }
}