package info.fingo.spata

class CSVRow(val row: IndexedSeq[String], val lineNum: Int, val rowNum: Int)(implicit header: Map[String, Int]) {

  if(row.length != header.size)
    throw new CSVException("Bad format: wrong number of values","valuesNumber", lineNum, rowNum)

  def getString(key: String): String = {
    val pos = header(key)
    row(pos)
  }

  override def toString: String = row.mkString(",")
}
