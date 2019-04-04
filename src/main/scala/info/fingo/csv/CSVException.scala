package info.fingo.csv

class CSVException(val message: String,
                   val messageCode: String,
                   val line: Option[Int],
                   val col: Option[Int],
                   val row: Option[Int],
                   val field: Option[String],
                   val cause: Throwable = null)
  extends RuntimeException(message,cause) {

  def this(message: String, messageCode: String, cause: Throwable) = this(message,messageCode,None,None,None,None,cause)
  def this(message: String, messageCode: String, line: Int, row: Int) = this(message,messageCode,Some(line),None,Some(row),None,null)
  def this(message: String, messageCode: String, line: Int, col: Int, row: Int) = this(message,messageCode,Some(line),Some(col),Some(row),None,null)
  def this(message: String, messageCode: String) = this(message,messageCode,None,None,None,None,null)
}