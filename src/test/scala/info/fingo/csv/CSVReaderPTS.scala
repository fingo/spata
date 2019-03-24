package info.fingo.csv

import org.scalatest.FunSuite

import scala.io.Source

class CSVReaderPTS extends FunSuite {
  val amount = 100000
  test("Reader should handle large data streams") {
    val separator = ','
    val source = new TestSource(separator)
    val reader = new CSVReader(source, separator)
    var count = 0
    while(reader.iterator.hasNext) {
      count += 1
      reader.iterator.next
    }
    assert(count == amount)
  }

  class TestSource(separator: Char) extends Source {
    def csvStream(sep: Char, lines: Int): Stream[Char] = {
      val cols = 10
      val header = ((1 to cols).mkString(""+sep) + "\n").toStream
      val rows = Stream.fill(lines)(s"lorem ipsum$sep" * (cols-1) + "lorem ipsum\n").flatMap(_.toCharArray)
      header #::: rows
    }
    override val iter: Iterator[Char] = csvStream(separator, amount).iterator
  }
}
