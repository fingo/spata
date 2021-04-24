/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

/** Representation of location of record in source data.
  *
  * `rowNum` is record counter. It start with `1` for data, with header row having number `0`.
  * It differs from `lineNum` for sources with header or fields containing line breaks.
  *
  * `lineNum` is the last line in source data which content is used to parse a record
  * - in other words it is the number of lines consumed so far to load a record.
  * It starts with `1`, including header line - first data record has typically line number `2`.
  * There may be many lines per record when some fields contain line breaks.
  * New line is interpreted independently from CSV record separator, as the standard platform `EOL` character sequence.
  *
  * @param row the row number
  * @param line the line number
  */
case class Position(row: Int, line: Int)

private[spata] object Position {
  def some(row: Int, line: Int): Option[Position] = Some(Position(row, line))
  def none: Option[Position] = None
}
