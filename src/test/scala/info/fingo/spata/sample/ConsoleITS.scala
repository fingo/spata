package info.fingo.spata.sample

import java.time.LocalDate

import scala.io.Source
import org.scalatest.funsuite.AnyFunSuite
import cats.effect.IO
import cats.implicits._
import fs2.Stream
import info.fingo.spata.CSVReader

/* Samples which use console to output CSV processing results */
class ConsoleITS extends AnyFunSuite {
  val dataFile = "mars-weather.csv"

  test("spata allows manipulate data using stream functionality") {
    case class YT(year: Int, temp: Double) // class to convert data to
    val reader = CSVReader.config.get // reader with default configuration
    // get stream of CSV records while ensuring source cleanup
    val records = Stream
      .bracket(IO { Source.fromResource(dataFile) })(source => IO { source.close() })
      .flatMap(reader.parse)
    // convert and aggregate data, get stream of YTs
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
    // get the IO effect with it final action
    val io = aggregates.compile.toList.map { l =>
      assert(l.size == 5)
      assert(l.head.year == 2017)
      l.foreach(yt => println(f"${yt.year}: ${yt.temp}%5.1f"))
    }
    // evaluate effect - trigger all stream operations
    io.unsafeRunSync()
  }

  test("spata allows executing simple side effects") {
    val reader = CSVReader.config.get // reader with default configuration
    val source = Source.fromResource(dataFile)
    try {
      reader.process(
        source,
        record =>
          if (record.get[Double]("max_temp") > 0) {
            println(s"Maximum daily temperature over 0 degree found")
            false
          } else {
            assert(record.rowNum < 500)
            true
          }
      )
    } finally {
      source.close()
    }
  }
}
