/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import java.nio.file.{Path, Paths}
import java.time.LocalDate
import scala.io.Source
import cats.effect.IO

/* Helper classes and variables for performance tests */
object PerformanceTH {

  val separator: Char = ','
  val path: Path = Paths.get(getClass.getClassLoader.getResource("mars-weather.csv").toURI)
  val parser: CSVParser[IO] = CSVParser.config.fieldDelimiter(separator).stripSpaces().parser[IO]()

  case class MarsWeather(
    id: Int,
    terrestrial_date: LocalDate,
    sol: Int,
    ls: Int,
    month: String,
    min_temp: Double,
    max_temp: Double,
    pressure: Double,
    wind_speed: Double,
    atmo_opacity: String
  )

  class TestSource(separator: Char, amount: Int) extends Source {
    def csvStream(sep: Char, lines: Int): LazyList[Char] = {
      val cols = 10
      val header = ((1 to cols).mkString("_", s"${sep}_", "") + "\n").to(LazyList)
      val rows =
        LazyList.fill(lines)(s"123.45$sep" * (cols - 1) + "lorem ipsum\n").flatMap(_.toCharArray)
      header #::: rows
    }
    override val iter: Iterator[Char] = csvStream(separator, amount).iterator
  }
}
