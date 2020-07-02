/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.sample

import java.time.LocalDate
import scala.util.control.NonFatal
import cats.effect.IO
import cats.implicits._
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import info.fingo.spata.CSVParser
import info.fingo.spata.io.reader

/* Samples which use console to output CSV processing results */
class ConsoleITS extends AnyFunSuite {

  private def println(s: String): String = s // do nothing, don't pollute test output

  test("spata allows manipulate data using stream functionality") {
    case class YT(year: Int, temp: Double) // class to converter data to
    val parser = CSVParser[IO]() // parser with default configuration and IO effect
    // get stream of CSV records while ensuring source cleanup
    val records = Stream
      .bracket(IO { SampleTH.sourceFromResource(SampleTH.dataFile) })(source => IO { source.close() })
      .through(reader[IO].by)
      .through(parser.parse)
    // converter and aggregate data, get stream of YTs
    val aggregates = records.filter { record =>
      record("max_temp") != "NaN"
    }.map { record =>
      val year = record.get[LocalDate]("terrestrial_date").getYear
      val temp = record.get[Double]("max_temp")
      YT(year, temp)
    }.filter { yt =>
      yt.year > 2012 && yt.year < 2018
    }.groupAdjacentBy(_.year) // we assume here that data is sorted by date
      .map(_._2)
      .map { chunk =>
        val year = chunk(0).year
        val temp = chunk.map(_.temp).foldLeft(0.0)(_ + _) / chunk.size
        YT(year, temp)
      }
      .handleErrorWith { ex =>
        println(s"An error has been detected while processing ${SampleTH.dataFile}: ${ex.getMessage}")
        Stream.empty[IO]
      }
    // get the IO effect with it final action
    val io = aggregates.compile.toList.map { l =>
      l.foreach(yt => println(f"${yt.year}: ${yt.temp}%5.1f"))
      assert(l.size == 5)
      assert(l.head.year == 2017)
    }
    // evaluate effect - trigger all stream operations
    io.unsafeRunSync()
  }

  test("spata allows executing simple side effects through callbacks") {
    val parser = CSVParser.config.get[IO] // parser with default configuration and IO effect
    try {
      SampleTH.withResource(SampleTH.sourceFromResource(SampleTH.dataFile)) { source =>
        parser
          .process(reader[IO].read(source)) { record =>
            if (record.get[Double]("max_temp") > 0) {
              println(s"Maximum daily temperature over 0 degree found on ${record("terrestrial_date")}")
              false
            } else {
              assert(record.rowNum < 500)
              true
            }
          }
          .unsafeRunSync()
      }
    } catch {
      case NonFatal(ex) =>
        println(ex.getMessage)
        fail()
    }
  }

  test("spata allow processing csv data as list") {
    val parser = CSVParser[IO]() // parser with default configuration and IO effect
    try {
      SampleTH.withResource(SampleTH.sourceFromResource(SampleTH.dataFile)) { source =>
        // get 500 first records
        val records = parser.get(reader[IO].read(source), 500).unsafeRunSync()
        val over0 = records.find(_.get[Double]("max_temp") > 0)
        assert(over0.isDefined)
        for (r <- over0)
          println(s"Over 0 temperature found on ${r("terrestrial_date")}")
      }
    } catch {
      case NonFatal(ex) =>
        println(ex.getMessage)
        fail()
    }
  }
}
