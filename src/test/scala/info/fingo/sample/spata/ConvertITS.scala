/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.sample.spata

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import info.fingo.spata.CSVConfig
import info.fingo.spata.io.Reader
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate

/* Sample which converts CSV records to case classes. */
class ConvertITS extends AnyFunSuite {

  test("spata allows manipulate data and converter it to case classes using stream functionality") {
    // class to converter data to - class fields have to match CSV header fields
    case class DayTemp(date: LocalDate, minTemp: Double, maxTemp: Double)
    val mh = Map("terrestrial_date" -> "date", "min_temp" -> "minTemp", "max_temp" -> "maxTemp")
    val parser = CSVConfig().mapHeader(mh).stripSpaces.parser[IO] // parser with IO effect
    val stream = Stream
      .bracket(IO { SampleTH.sourceFromResource(SampleTH.dataFile) })(source => IO { source.close() }) // ensure resource cleanup
      .through(Reader.plain[IO].by)
      .through(parser.parse) // get stream of CSV records
      .map(_.to[DayTemp]) // converter records to DayTemps
      .rethrow // get data out of Either and let stream fail on error
      .filter(_.date.getYear == 2016) // filter data for specific year
      .handleErrorWith(ex => fail(ex.getMessage)) // fail test on any stream error
    val result = stream.compile.toList.unsafeRunSync()
    assert(result.length > 300 && result.length < 400)
    assert(result.forall(_.date.getYear == 2016))
  }
}
