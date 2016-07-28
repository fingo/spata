package info.fingo.csv

class CSVRow(row: IndexedSeq[String])(implicit header: Map[String,Int]) {

  def getString(key: String): String = {
    val pos = header(key)
    row(pos)
  }
}
