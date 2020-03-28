package info.fingo.spata.parser

/* Location of read source data (character), used mainly to report place of failure.
 * position is the number of character at given line. It starts with 0, meaning "before first character".
 * line numbers starts with 1.
 */
private[spata] case class Location(position: Int, line: Int = 1) {
  def add(position: Int, line: Int = 0): Location = Location(this.position + position, this.line + line)
  def nextPosition: Location = add(1)
  def nextLine: Location = Location(0, this.line + 1)
}
