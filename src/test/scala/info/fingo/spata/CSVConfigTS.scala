package info.fingo.spata

import org.scalatest.funsuite.AnyFunSuite

class CSVConfigTS extends AnyFunSuite {
  test("Config should be build correctly") {
    val config = CSVConfig().fieldDelimiter(';').noHeader().fieldSizeLimit(100)
    val expected = CSVConfig(';', '\n', '"', hasHeader = false, Some(100))
    assert(config == expected)
  }
}
