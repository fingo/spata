package info.fingo.spata

/** CSV configuration used for creating [[CSVReader]]
  *
  * The way to create a reader is to use config as a builder:
  * {{{ val reader = CSVConfig.fieldSizeLimit(1000).noHeader().get }}}
  *
  * Field delimiter is `','` by default.
  *
  * Record delimiter is `'\n'` by default. When the delimiter is line feed (`'\n'`, ASCII 10)
  * and it is preceding by carriage return (`'\r'`, ASCII 13), they are treated as single character.
  *
  * Quotation mark is `'"'` by default. It is required to wrap special characters - field and record delimiters.
  * Quotation mark quote may appear only inside quote marks in source file.
  * It has to be doubled to be interpreted as part of actual data, not control character
  * If a field starts or end with white character it has to be wrapped in quote mark.
  * In another case the white characters are stripped.
  *
  * If the source has header, which is the default, it is used as index for actual data and not included in it.
  * If there is no header, a number-base one is created (starting from `"0"`).
  *
  * Field size limit is used to stop processing the input when it is significantly larger then expected and avoid
  * `OutOfMemoryError`. This might happen if the source structure is invalid, e.g. the closing quotation mark is missing.
  * By default there is no limit.
  *
  * @param fieldDelimiter field (cell) separator
  * @param recordDelimiter record (row, line) separator
  * @param quoteMark character used to wrap (quote) field content
  * @param hasHeader set if data starts with header row
  * @param fieldSizeLimit maximal size of a field
  */
case class CSVConfig private[spata] (
  fieldDelimiter: Char = ',',
  recordDelimiter: Char = '\n',
  quoteMark: Char = '"',
  hasHeader: Boolean = true,
  fieldSizeLimit: Option[Int] = None
) {

  /** Get new config from this one by replacing field delimiter with provided one */
  def fieldDelimiter(fd: Char): CSVConfig = this.copy(fieldDelimiter = fd)

  /** Get new config from this one by replacing record delimiter with provided one */
  def recordDelimiter(rd: Char): CSVConfig = this.copy(recordDelimiter = rd)

  /** Get new config from this one by replacing quotation mark with provided one */
  def quoteMark(qm: Char): CSVConfig = this.copy(quoteMark = qm)

  /** Get new config from this one by switching off header presence */
  def noHeader(): CSVConfig = this.copy(hasHeader = false)

  /** Get new config from this one by replacing field size limit with provided one */
  def fieldSizeLimit(fsl: Int): CSVConfig = this.copy(fieldSizeLimit = Some(fsl))

  /** Create [[CSVReader]] from this config */
  def get: CSVReader = new CSVReader(this)
}
