/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import java.io.File
import java.nio.file.{Path, Paths}
import java.time.LocalDate
import scala.io.Source
import cats.effect.IO
import fs2.Stream

/* Helper classes and variables for performance tests */
object PerformanceTH:

  val separator: Char = ','
  val input: Path = Paths.get(getClass.getClassLoader.getResource("mars-weather.csv").toURI)
  val output: Path =
    val temp = File.createTempFile("spata_perf_", ".csv")
    temp.deleteOnExit()
    temp.toPath

  val parser: CSVParser[IO] = CSVParser.config.fieldDelimiter(separator).stripSpaces.parser[IO]
  val renderer: CSVRenderer[IO] = CSVRenderer.config.fieldDelimiter(separator).renderer[IO]

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
  val mwHeader: Header = Header(
    "id",
    "terrestrial_date",
    "sol",
    "ls",
    "month",
    "min_temp",
    "max_temp",
    "pressure",
    "wind_speed",
    "atmo_opacity"
  )

  class TestSource(amount: Int) extends Source:
    private val cols = 10
    private val row = s"123.45$separator" * (cols - 1) + "lorem ipsum\n"
    private def csvStream: LazyList[Char] =
      val header = ((1 to cols).mkString("_", s"${separator}_", "") + "\n").to(LazyList)
      val rows = LazyList.fill(amount)(row).flatMap(_.toCharArray)
      header #::: rows
    override val iter: Iterator[Char] = csvStream.iterator

  def testSource(amount: Int): Stream[IO, Char] =
    Stream.fromIterator[IO](new TestSource(amount), 1_000)

  def testRecords(amount: Int): Stream[IO, Record] =
    val cols = 10
    val header = Header((1 to cols).map(i => s"_$i"): _*)
    val record = Record((1 to cols).map(i => s"value$i"): _*)(header)
    val rows: LazyList[Record] = LazyList.fill(amount)(record)
    Stream(rows*).covary[IO]

  def testMarsWeather(amount: Int): Stream[IO, MarsWeather] =
    val mw = MarsWeather(1, LocalDate.of(2018, 2, 18), 1968, 133, "Month 5", -76, -19, 732, Double.NaN, "Sunny")
    val rows = LazyList.fill(amount)(mw)
    Stream(rows*).covary[IO]
