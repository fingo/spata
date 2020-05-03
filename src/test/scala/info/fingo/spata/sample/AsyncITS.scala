/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.sample

import java.time.LocalDate
import java.util.concurrent.atomic.{AtomicInteger, LongAdder}

import scala.concurrent.ExecutionContext
import cats.effect.{Blocker, ContextShift, IO}
import fs2.Stream
import info.fingo.spata.CSVReader
import info.fingo.spata.CSVReader.CSVCallback
import org.scalatest.funsuite.AnyFunSuite

class AsyncITS extends AnyFunSuite {
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private def println(s: String): String = s // do nothing, don't pollute test output

  test("spata source reading blocking operations may be shifted to blocking execution context") {
    // class to converter data to - class fields have to match CSV header fields
    case class DayTemp(date: LocalDate, minTemp: Double, maxTemp: Double)
    val mh = Map("terrestrial_date" -> "date", "min_temp" -> "minTemp", "max_temp" -> "maxTemp")
    val reader = CSVReader.config.mapHeader(mh).get // reader with default configuration
    val records = for {
      blocker <- Stream.resource(Blocker[IO]) // ensure creation and cleanup of blocking execution context
      // ensure resource allocation and  cleanup
      source <- Stream.bracket(IO { SampleTH.sourceFromResource(SampleTH.dataFile) })(source => IO { source.close() })
      recs <- reader.shifting(blocker).parse(source) // get stream of CSV records
    } yield recs
    val dayTemps = records
      .map(_.to[DayTemp]()) // converter records to DayTemps
      .rethrow // get data out of Either and let stream fail on error
      .filter(_.date.getYear == 2016) // filter data for specific year
      .handleErrorWith(ex => fail(ex.getMessage)) // fail test on any stream error
    val result = dayTemps.compile.toList.unsafeRunSync()
    assert(result.length > 300 && result.length < 400)
    assert(result.forall(_.date.getYear == 2016))
  }

  test("spata allows asynchronous source processing") {
    val reader = CSVReader.config.get
    val sum = new LongAdder()
    val count = new LongAdder()
    val cb: CSVCallback = row => {
      count.increment()
      sum.add(row.get[Long]("max_temp") - row.get[Long]("min_temp"))
      row("month") == "Month 5"
    }
    val waiting = new AtomicInteger(100)
    val result: Either[Throwable, Unit] => Unit = {
      case Right(_) =>
        val avg = sum.doubleValue() / count.doubleValue()
        println(s"Average temperature during Month 5 was $avg")
        waiting.set(0)
      case _ =>
        waiting.set(0)
        fail()
    }
    SampleTH.withResource(SampleTH.sourceFromResource(SampleTH.dataFile)) { source =>
      reader.shifting.processAsync(source, result)(cb) // use internal blocker
      assert(sum.intValue() < 1000)
      while (waiting.decrementAndGet() > 0) Thread.sleep(100)
      assert(sum.intValue() > 1000)
    }
  }
}
