/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.sample

import java.time.{LocalDate, Month}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.LongAdder
import scala.concurrent.ExecutionContext
import cats.effect.{Blocker, ContextShift, IO}
import fs2.Stream
import info.fingo.spata.CSVParser
import info.fingo.spata.CSVParser.CSVCallback
import info.fingo.spata.io.reader
import org.scalatest.funsuite.AnyFunSuite

/* Samples which process the data asynchronously or using blocking context */
class AsyncITS extends AnyFunSuite {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private def println(s: String): String = s // do nothing, don't pollute test output

  test("spata allows asynchronous source processing") {
    val parser = CSVParser.config.get
    val sum = new LongAdder()
    val count = new LongAdder()

    val cb: CSVCallback = row => {
      count.increment()
      sum.add(row.get[Long]("max_temp") - row.get[Long]("min_temp"))
      row("month") == "Month 5"
    }
    val cdl = new CountDownLatch(1)
    val result: Either[Throwable, Unit] => Unit = {
      case Right(_) =>
        val avg = sum.doubleValue() / count.doubleValue()
        println(s"Average temperature during Month 5 was $avg")
        cdl.countDown()
      case _ =>
        println(s"Error occurred while processing data")
        cdl.countDown()
    }
    SampleTH.withResource(SampleTH.sourceFromResource(SampleTH.dataFile)) { source =>
      val data = reader.withBlocker.read(source)
      parser.async.process(data)(cb).unsafeRunAsync(result)
      assert(sum.intValue() < 1000)
      cdl.await(3, TimeUnit.SECONDS)
      assert(sum.intValue() > 1000)
    }
  }

  test("spata source reading blocking operations may be shifted to blocking execution context") {
    // class to converter data to - class fields have to match CSV header fields
    case class DayTemp(date: LocalDate, minTemp: Double, maxTemp: Double)
    val mh = Map("terrestrial_date" -> "date", "min_temp" -> "minTemp", "max_temp" -> "maxTemp")
    val parser = CSVParser.config.mapHeader(mh).get // parser with default configuration
    val records = for {
      blocker <- Stream.resource(Blocker[IO]) // ensure creation and cleanup of blocking execution context
      // ensure resource allocation and  cleanup
      source <- Stream.bracket(IO { SampleTH.sourceFromResource(SampleTH.dataFile) })(source => IO { source.close() })
      record <- reader.withBlocker(blocker).read(source).through(parser.parse) // get stream of CSV records
    } yield record
    val dayTemps = records
      .map(_.to[DayTemp]()) // converter records to DayTemps
      .rethrow // get data out of Either and let stream fail on error
      .filter(dt => dt.date.getYear == 2018 && dt.date.getMonth == Month.JANUARY) // filter data for specific month
      .take(30)
      .handleErrorWith(ex => fail(ex.getMessage)) // fail test on any stream error
    val result = dayTemps.compile.toList.unsafeRunSync()
    assert(result.length == 30)
    assert(result.forall { dt =>
      dt.date.isAfter(LocalDate.of(2017, 12, 31)) &&
      dt.date.isBefore(LocalDate.of(2018, 2, 1))
    })
  }
}
