/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import java.util.NoSuchElementException
import cats.effect.Sync
import fs2.{Chunk, Pipe, Stream}
import info.fingo.spata.error.HeaderError
import info.fingo.spata.util.Logger

/** A utility for rendering data to CSV representation.
  *
  * @param config the configuration for CSV rendering (delimiters, header presence etc.)
  * @tparam F the effect type, with a type class providing support for suspended execution
  * (typically [[cats.effect.IO]]) and logging (provided internally by spata)
  */
class CSVRenderer[F[_]: Sync: Logger](config: CSVConfig) {

  private val sfd = config.fieldDelimiter.toString
  private val sq = config.quoteMark.toString

  /** Transforms stream of records into stream of characters representing CSV data.
    *
    * @param header header to be potentially written into target stream and used to select required data from records
    * @return a pipe to converter [[Record]]s into [[scala.Char]]s
    */
  def render(header: Header): Pipe[F, Record, Char] = (in: Stream[F, Record]) => {
    val hs = Stream.emit(Right(header.names.map(escape).mkString(sfd)))
    val cs = in.map(makeLine(_, header))
    val stream = if (config.hasHeader) hs ++ cs else cs
    stream.rethrow
      .intersperse(config.recordDelimiter.toString)
      .map(s => Chunk.chars(s.toCharArray))
      .flatMap(Stream.chunk)
  }

  private def makeLine(record: Record, header: Header): Either[HeaderError, String] =
    header.names.map { name =>
      // FIXME: correct HeaderError params / change error type
      record(name).map(escape).toRight(new HeaderError(0, 0, name, new NoSuchElementException()))
    }.foldRight[Either[HeaderError, List[String]]](Right(Nil))((elm, seq) => elm.flatMap(s => seq.map(s :: _)))
      .map(_.mkString(sfd))

  private def escape(s: String): String = if (s.contains(sq)) s.replace(sq, sq * 2).mkString(sq, "", sq) else s
}
