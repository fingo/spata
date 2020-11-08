/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import cats.effect.Sync
import info.fingo.spata.util.Logger

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
  * Header mapping may be also position-based, which is especially handy when there are duplicates in header
  * and name-based remapping does not solve it (because it remaps all occurrences):
  * {{{config.mapHeader(Map(0 -> "firstName", 1 -> "lastName")))}}}
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
  * @param headerMap definition of header remapping, by name or index
  * @param fieldSizeLimit maximal size of a field
  */
case class CSVConfig private[spata] (
  fieldDelimiter: Char = ',',
  recordDelimiter: Char = '\n',
  quoteMark: Char = '"',
  hasHeader: Boolean = true,
  headerMap: HeaderMap = NoHeaderMap,
  trim: Boolean = false,
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

  /** Remap selected fields names. */
  def mapHeader(hm: HeaderMap): CSVConfig = this.copy(headerMap = hm)

  /** Gets new config from this one by switching on whitespace stripping around fields */
  def trimSpaces(): CSVConfig = this.copy(trim = true)

  /** Gets new config from this one by replacing field size limit with provided one. */
  def fieldSizeLimit(fsl: Int): CSVConfig = this.copy(fieldSizeLimit = Some(fsl))

  /** Creates [[CSVParser]] from this config.
    *
    * @tparam F the effect type, with a type class providing support for suspended execution
    * (typically [[cats.effect.IO]]) and logging (provided internally by spata)
    * @return parser configured according to provided settings
    */
  def get[F[_]: Sync: Logger](): CSVParser[F] = new CSVParser(this)

  /** Provides configuration description
    *
    * @return short textual information about configuration
    */
  override def toString: String = {
    def printWhite(c: Char) = c match {
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case ' ' => " "
      case c if c.isWhitespace => '␣'
      case _ => c
    }
    val fd = printWhite(fieldDelimiter)
    val rd = printWhite(recordDelimiter)
    val qm = printWhite(quoteMark)
    val hdr = if (hasHeader) "header" else "no header"
    val hm = if (headerMap == NoHeaderMap) "no mapping" else "header mapping"
    val st = if (trim) "space trimming" else "no trimming"
    val fsl = fieldSizeLimit.map(size => s", $size").getOrElse("")
    s"CSVConfig('$fd', '$rd', '$qm', $hdr, $hm, $st$fsl)"
  }
}
