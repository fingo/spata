/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.sample

import java.time.LocalDate
import cats.effect.IO
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import info.fingo.spata.CSVParser
import info.fingo.spata.io.reader

/* Samples which converter CSV records to case classes. */
class ConvertITS extends AnyFunSuite {

  test("spata allows manipulate data and converter it to case classes using stream functionality") {
    // class to converter data to - class fields have to match CSV header fields
    case class DayTemp(date: LocalDate, minTemp: Double, maxTemp: Double)
    val mh = Map("terrestrial_date" -> "date", "min_temp" -> "minTemp", "max_temp" -> "maxTemp")
    val parser = CSVParser.config.mapHeader(mh).trimSpaces().get[IO]() // parser with IO effect
    val stream = Stream
      .bracket(IO { SampleTH.sourceFromResource(SampleTH.dataFile) })(source => IO { source.close() }) // ensure resource cleanup
      .through(reader[IO]().by)
      .through(parser.parse) // get stream of CSV records
      .map(_.to[DayTemp]()) // converter records to DayTemps
      .rethrow // get data out of Either and let stream fail on error
      .filter(_.date.getYear == 2016) // filter data for specific year
      .handleErrorWith(ex => fail(ex.getMessage)) // fail test on any stream error
    val result = stream.compile.toList.unsafeRunSync()
    assert(result.length > 300 && result.length < 400)
    assert(result.forall(_.date.getYear == 2016))
  }
}
