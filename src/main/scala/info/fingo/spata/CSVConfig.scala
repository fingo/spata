/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import cats.effect.Sync
import info.fingo.spata.util.Logger

/** CSV configuration used for creating [[CSVParser]] or [[CSVRenderer]].
  *
  * This config may be used as a builder to create a parser :
  * {{{ val parser = CSVConfig().fieldSizeLimit(1000).noHeader().parser[IO]() }}}
  * or renderer:
  * {{{ val parser = CSVConfig().escapeSpaces().noHeader().renderer[IO]() }}}
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
  *
  * While parsing, the header setting defines if the source has a header, which is true by default.
  * Header is used as an keys to actual values and not included in data.
  * If there is no header, a number-based keys are created (starting from `"_1"`).
  * While rendering, the header setting defines if header row should be added to output.
  * If no header is explicitly defined, a number-based is used, like for parsing.
  *
  * If CSV records are converted to case classes, header values are used as class fields and may require remapping.
  * This can be achieved through [[mapHeader(* mapHeader]]:
  * {{{config.mapHeader(Map("first name" -> "firstName", "last name" -> "lastName")))}}}
  * or if there is no header line:
  * {{{config.mapHeader(Map("_1" -> "firstName", "_2" -> "lastName")))}}}
  * Header mapping may be also position-based, which is especially handy when there are duplicates in header
  * and name-based remapping does not solve it (because it remaps all occurrences):
  * {{{config.mapHeader(Map(0 -> "firstName", 1 -> "lastName")))}}}
  * Remapping may be used for renderer as well, allowing customized header while converting data from case classes.
  *
  * Unescaped fields with leading or trailing spaces may be automatically trimmed while parsing
  * when `trimSpaces` is set to `true`.
  * This setting is `false` by default and white spaces are preserved, even for unescaped fields.
  *
  * Field size limit is used to stop processing input when it is significantly larger then expected
  * and avoid `OutOfMemoryError`.
  * This might happen if the source structure is invalid, e.g. the closing quotation mark is missing.
  * There is no limit by default.
  *
  * While rendering CSV content, different quoting polices may be used, which is controlled by `escapeMode` setting.
  * By default only field that have to are put into quotes -
  * this means fields which contain field delimiter, record delimiter or quotation mark.
  * When set to `EscapeSpaces` quotes are put additionally around field with leading or trailing spaces.
  * `EscapeAll` results in putting quotes around all fields.
  *
  * @param fieldDelimiter field (cell) separator, `','` by default
  * @param recordDelimiter record (row) separator, `'\n'` by default
  * @param quoteMark character used to wrap (quote) field content, `'"'` by default
  * @param hasHeader set if data starts with header row, `true` by default
  * @param headerMap definition of header remapping, by name or index, empty by default
  * @param trimSpaces flag to strip spaces, `false` by default, valid only for parsing
  * @param fieldSizeLimit maximal size of a field, `None` by default, valid only for parsing
  * @param escapeMode method of escaping fields, `EscapeRequired` by default, valid only for rendering
  */
case class CSVConfig private[spata] (
  fieldDelimiter: Char = ',',
  recordDelimiter: Char = '\n',
  quoteMark: Char = '"',
  hasHeader: Boolean = true,
  headerMap: HeaderMap = NoHeaderMap,
  trimSpaces: Boolean = false,
  fieldSizeLimit: Option[Int] = None,
  escapeMode: CSVConfig.EscapeMode = CSVConfig.EscapeRequired
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

  /** Gets new config from this one by switching on stripping of unquoted, leading and trailing whitespaces.
    *
    * @note This setting is used only by parser and ignored by renderer.
    */
  def stripSpaces(): CSVConfig = this.copy(trimSpaces = true)

  /** Gets new config from this one by replacing field size limit with provided one.
    *
    * @note This setting is used only by parser and ignored by renderer.
    */
  def fieldSizeLimit(fsl: Int): CSVConfig = this.copy(fieldSizeLimit = Some(fsl))

  /**Gets new config from this one by changing escape mode to escaping fields with leading or trailing spaces.
    *
    * @note This setting is used only by renderer and ignored by parser.
    */
  def escapeSpaces(): CSVConfig = this.copy(escapeMode = CSVConfig.EscapeSpaces)

  /**Gets new config from this one by changing escape mode to escaping all fields.
    *
    * @note This setting is used only by renderer and ignored by parser.
    */
  def escapeAll(): CSVConfig = this.copy(escapeMode = CSVConfig.EscapeAll)

  /** Creates [[CSVParser]] from this config.
    *
    * @tparam F the effect type, with a type class providing support for suspended execution
    * (typically [[cats.effect.IO]]) and logging (provided internally by spata)
    * @return parser configured according to provided settings
    */
  def parser[F[_]: Sync: Logger](): CSVParser[F] = new CSVParser(this)

  /** Creates [[CSVRenderer]] from this config.
    *
    * @tparam F the effect type, with a type class providing support for suspended execution
    * (typically [[cats.effect.IO]]) and logging (provided internally by spata)
    * @return renderer configured according to provided settings
    */
  def renderer[F[_]: Sync: Logger](): CSVRenderer[F] = new CSVRenderer(this)

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
    val st = if (trimSpaces) "space trimming" else "no trimming"
    val fsl = fieldSizeLimit.map(size => s", $size").getOrElse("")
    s"CSVConfig('$fd', '$rd', '$qm', $hdr, $hm, $st$fsl)"
  }
}

/** CSVConfig companion object with escape mode definitions. */
object CSVConfig {

  /** Method of escaping fields while rendering CSV. */
  sealed trait EscapeMode

  /** Escape fields only when required - when they contain one of the delimiter or escape character. */
  case object EscapeRequired extends EscapeMode

  /** Escape fields only when required and and when field has leading or trailing white spaces. */
  case object EscapeSpaces extends EscapeMode

  /** Escape all fields regardless of their content. */
  case object EscapeAll extends EscapeMode
}
