package info.fingo.spata

import info.fingo.spata.parser.ParsingErrorCode

class CSVRecord private (val row: IndexedSeq[String], val lineNum: Int, val rowNum: Int)(implicit header: Map[String, Int]) {

  def getString(key: String): String = {
    val pos = header(key)
    row(pos)
  }

  override def toString: String = row.mkString(",")
}

object CSVRecord {
  def apply(row: IndexedSeq[String], lineNum: Int, rowNum: Int)(implicit header: Map[String, Int]): Either[CSVException,CSVRecord] =
    if(row.size == header.size)
      Right(new CSVRecord(row, lineNum, rowNum)(header))
    else {
      val err = ParsingErrorCode.FieldsHeaderImbalance
      Left(new CSVException(err.message, err.code, lineNum, rowNum))
    }
}
