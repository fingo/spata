/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import info.fingo.spata.io.reader

import scala.io.Source
import org.scalatest.funsuite.AnyFunSuite

class CSVParserPTS extends AnyFunSuite {
  val amount = 5_000
  test("reader should handle large data streams") {
    val separator = ','
    val parser = CSVParser.config.fieldDelimiter(separator).get
    val data = reader(new TestSource(separator))
    var count = 0
    parser
      .process(data) { _ =>
        count += 1
        true
      }
      .unsafeRunSync()
    assert(count == amount)
  }

  class TestSource(separator: Char) extends Source {
    def csvStream(sep: Char, lines: Int): LazyList[Char] = {
      val cols = 10
      val header = ((1 to cols).mkString("" + sep) + "\n").to(LazyList)
      val rows = LazyList.fill(lines)(s"lorem ipsum$sep" * (cols - 1) + "lorem ipsum\n").flatMap(_.toCharArray)
      header #::: rows
    }
    override val iter: Iterator[Char] = csvStream(separator, amount).iterator
  }
}
