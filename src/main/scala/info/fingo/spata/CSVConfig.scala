/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import fs2.RaiseThrowable

/** CSV configuration used for creating [[CSVParser]].
  *
  * This config may be used as a builder to create a parser:
  * {{{ val parser = CSVConfig.fieldSizeLimit(1000).noHeader().get[IO]() }}}
  *
  * Field delimiter is `','` by default.
  *
  * Record delimiter is `'\n'` by default. When the delimiter is line feed (`'\n'`, ASCII 10)
  * and it is preceded by carriage return (`'\r'`, ASCII 13), they are treated as a single character.
  *
  * Quotation mark is `'"'` by default.
  * It is required to wrap special characters in quotes - field and record delimiters.
  * Quotation mark in content may appear only inside quotation marks.
  * It has to be doubled to be interpreted as part of actual data, not a control character.
  * If a field starts or end with white character it has to be wrapped in quotation marks.
  * In another case the white characters are stripped.
  *
  * If the source has a header, which is the default, it is used as an keys to actual values and not included in data.
  * If there is no header, a number-based keys are created (starting from `"_1"`).
  *
  * If CSV records are converted to case classes, header values are used as class fields and may require remapping.
  * This can be achieved through [[mapHeader(* mapHeader]]:
  * {{{config.mapHeader(Map("first name" -> "firstName", "last name" -> "lastName")))}}}
  * or if there is no header line:
  * {{{config.mapHeader(Map("_1" -> "firstName", "_2" -> "lastName")))}}}
  *
  * Field size limit is used to stop processing input when it is significantly larger then expected
  * and avoid `OutOfMemoryError`.
  * This might happen if the source structure is invalid, e.g. the closing quotation mark is missing.
  * There is no limit by default.
  *
  * @param fieldDelimiter field (cell) separator
  * @param recordDelimiter record (row) separator
  * @param quoteMark character used to wrap (quote) field content
  * @param hasHeader set if data starts with header row
  * @param mapHeader partial function to remap selected header values
  * @param fieldSizeLimit maximal size of a field
  */
case class CSVConfig private[spata] (
  fieldDelimiter: Char = ',',
  recordDelimiter: Char = '\n',
  quoteMark: Char = '"',
  hasHeader: Boolean = true,
  setHeader: I2S = PartialFunction.empty,
  mapHeader: S2S = PartialFunction.empty,
  fieldSizeLimit: Option[Int] = None
) {

  /** Gets new config from this one by replacing field delimiter with provided one. */
  def fieldDelimiter(fd: Char): CSVConfig = this.copy(fieldDelimiter = fd)

  /** Gets new config from this one by replacing record delimiter with provided one. */
  def recordDelimiter(rd: Char): CSVConfig = this.copy(recordDelimiter = rd)

  /** Gets new config from this one by replacing quotation mark with provided one. */
  def quoteMark(qm: Char): CSVConfig = this.copy(quoteMark = qm)

  /** Gets new config from this one by switching off header presence. */
  def noHeader(): CSVConfig = this.copy(hasHeader = false)

  /** Gets new config from this one by replacing field size limit with provided one. */
  def fieldSizeLimit(fsl: Int): CSVConfig = this.copy(fieldSizeLimit = Some(fsl))

  /** Remap selected fields names. */
  def mapHeader(mh: S2S): CSVConfig = this.copy(mapHeader = mh)

  /** Set selected fields names. */
  def setHeader(sh: I2S): CSVConfig = this.copy(setHeader = sh)

  /** Creates [[CSVParser]] from this config.
    *
    * @tparam F the effect type, with a type class providing support for raising and handling errors
    * (typically [[cats.effect.IO]])
    * @return parser configured according to provided settings
    */
  def get[F[_]: RaiseThrowable](): CSVParser[F] = new CSVParser(this)
}
