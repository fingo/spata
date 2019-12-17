package info.fingo.spata

class CSVException(
  val message: String,
  val messageCode: String,
  val line: Option[Int],
  val row: Option[Int],
  val col: Option[Int],
  val field: Option[String]
) extends Exception(message) {

  def this(message: String, messageCode: String) =
    this(message, messageCode, None, None, None, None)

  def this(message: String, messageCode: String, line: Int, row: Int) =
    this(message, messageCode, Some(line), Some(row), None, None)

  def this(message: String, messageCode: String, line: Int, row: Int, col: Int, field: Option[String]) =
    this(message, messageCode, Some(line), Some(row), Some(col), field)
}
