/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.sample.spata

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.*
import fs2.Stream
import info.fingo.spata.io.Reader
import info.fingo.spata.{CSVParser, Record}
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate
import scala.util.{Failure, Success, Try}

/* Samples which use console to output CSV processing results */
class ConsoleITS extends AnyFunSuite {

  private def println(s: String): String = s // do nothing, don't pollute test output

  test("spata allows manipulate data using stream functionality") {
    case class YT(year: Int, temp: Double) // class to converter data to

    def ytFromRecord(record: Record) =
      for {
        date <- record.get[LocalDate]("terrestrial_date")
        temp <- record.get[Double]("max_temp")
      } yield YT(date.getYear, temp)

    // get stream of CSV records while ensuring source cleanup
    val records = Stream
      .bracket(IO { SampleTH.sourceFromResource(SampleTH.dataFile) })(source => IO { source.close() })
      .through(Reader[IO].by)
      .through(CSVParser[IO].parse) // parser with default configuration and IO effect
    // converter and aggregate data, get stream of YTs
    val aggregates = records
      .map(ytFromRecord)
      .filter {
        _.exists(yt => yt.year > 2012 && yt.year < 2018) // filter only correct (Right) values in certain range
      }
      .rethrow // rethrow to get rid of Either, which is safe because of above filter
      .groupAdjacentBy(_.year) // we assume here that data is sorted by date
      .map {
        case (_, chunk) =>
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
    Try {
      SampleTH.withResource(SampleTH.sourceFromResource(SampleTH.dataFile)) { source =>
        CSVParser[IO] // parser with default configuration and IO effect
          .process(Reader[IO].read(source)) { record =>
            if (record.get[Double]("max_temp").exists(_ > 0)) {
              println(s"Maximum daily temperature over 0 degree found on ${record("terrestrial_date")}")
              false
            } else {
              assert(record.rowNum < 500)
              true
            }
          }
          .unsafeRunSync()
      }
    } match {
      case Success(_) => // do nothing
      case Failure(ex) =>
        println(ex.getMessage)
        fail()
    }
  }

  test("spata allow processing csv data as list") {
    Try {
      SampleTH.withResource(SampleTH.sourceFromResource(SampleTH.dataFile)) { source =>
        // get 500 first records
        val records = CSVParser[IO].get(Reader[IO].read(source), 500).unsafeRunSync()
        val over0 = records.find(_.get[Double]("max_temp").exists(_ > 0))
        assert(over0.isDefined)
        val info = over0.map(r => s"Over 0 temperature found on ${r("terrestrial_date")}")
        info.getOrElse("Over 0 temperature not found")
      }
    } match {
      case Success(info) => println(info)
      case Failure(ex) =>
        println(ex.getMessage)
        fail()
    }
  }
}
