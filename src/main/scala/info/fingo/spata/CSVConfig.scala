package info.fingo.spata

case class CSVConfig private[spata] (
  fieldDelimiter: Char = ',',
  recordDelimiter: Char = '\n',
  quoteMark: Char = '"',
  hasHeader: Boolean = true,
  fieldSizeLimit: Option[Int] = None
) {
  def fieldDelimiter(fd: Char): CSVConfig = this.copy(fieldDelimiter = fd)
  def recordDelimiter(rd: Char): CSVConfig = this.copy(recordDelimiter = rd)
  def quoteMark(qm: Char): CSVConfig = this.copy(quoteMark = qm)
  def noHeader(): CSVConfig = this.copy(hasHeader = false)
  def fieldSizeLimit(fsl: Int): CSVConfig = this.copy(fieldSizeLimit = Some(fsl))

  def get: CSVReader = new CSVReader(this)
}
