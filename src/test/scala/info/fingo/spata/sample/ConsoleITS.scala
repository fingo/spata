package info.fingo.spata.sample

import java.io.{Closeable, FileNotFoundException}
import java.time.LocalDate

import scala.io.Source
import org.scalatest.funsuite.AnyFunSuite
import cats.effect.IO
import cats.implicits._
import fs2.Stream
import info.fingo.spata.CSVReader

import scala.util.control.NonFatal

/* Samples which use console to output CSV processing results */
class ConsoleITS extends AnyFunSuite {
  val dataFile = "mars-weather.csv"
  private def println(s: String): String = s // do nothing, don't pollute test output

  test("spata allows manipulate data using stream functionality") {
    case class YT(year: Int, temp: Double) // class to convert data to
    val reader = CSVReader.config.get // reader with default configuration
    // get stream of CSV records while ensuring source cleanup
    val records = Stream
      .bracket(IO { sourceFromResource(dataFile) })(source => IO { source.close() })
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
      .handleErrorWith { ex =>
        println(s"An error has been detected while processing $dataFile: ${ex.getMessage}")
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
    val reader = CSVReader.config.get // reader with default configuration
    try {
      withResource(sourceFromResource(dataFile)) { source =>
        reader.process(
          source,
          record =>
            if (record.get[Double]("max_temp") > 0) {
              println(s"Maximum daily temperature over 0 degree found on ${record("terrestrial_date")}")
              false
            } else {
              assert(record.rowNum < 500)
              true
            }
        )
      }
    } catch {
      case NonFatal(ex) =>
        println(ex.getMessage)
        fail
    }
  }

  test("spata allow processing csv data as list") {
    val reader = CSVReader.config.get // reader with default configuration
    try {
      withResource(sourceFromResource(dataFile)) { source =>
        val records = reader.load(source, 500) // load 500 first records
        val over0 = records.find(_.get[Double]("max_temp") > 0)
        assert(over0.isDefined)
        for (r <- over0)
          println(s"Over 0 temperature found on ${r("terrestrial_date")}")
      }
    } catch {
      case NonFatal(ex) =>
        println(ex.getMessage)
        fail
    }
  }

  /* Source.fromResource throws NullPointerException on access, instead one of IOExceptions,
   * like fromFile or fromURL, so we convert it to be conform with typical usage scenarios.
   */
  def sourceFromResource(name: String): Source = {
    val source = Source.fromResource(name)
    try {
      source.hasNext
    } catch {
      case _: NullPointerException => throw new FileNotFoundException(s"Cannot find resource $name")
    }
    source
  }

  /* This is very simplistic approach - don't use it in production code.
   *  Look for better implementation of loan pattern or user ARM library
   */
  private def withResource[A <: Closeable, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      resource.close()
    }
}
