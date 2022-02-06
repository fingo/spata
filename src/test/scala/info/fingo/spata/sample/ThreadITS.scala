/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.sample

import java.time.{LocalDate, Month}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.LongAdder
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import info.fingo.spata.{CSVConfig, CSVParser}
import info.fingo.spata.CSVParser.Callback
import info.fingo.spata.io.Reader

/* Samples which process the data asynchronously or using blocking context */
class ThreadITS extends AnyFunSuite {

  private def println(s: String): String = s // do nothing, don't pollute test output

  test("spata allows asynchronous source processing") {
    val sum = new LongAdder()
    val count = new LongAdder()

    val cb: Callback = row => {
      count.increment()
      val diff = for {
        max <- row.get[Long]("max_temp")
        min <- row.get[Long]("min_temp")
      } yield max - min
      sum.add(diff.getOrElse(0))
      row("month").contains("Month 5")
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
      val data = Reader.shifting[IO].read(source)
      CSVParser[IO].async.process(data)(cb).unsafeRunAsync(result)
      assert(sum.intValue() < 1000)
      cdl.await(3, TimeUnit.SECONDS)
      assert(sum.intValue() > 1000)
    }
  }

  test("spata source reading blocking operations may be shifted to blocking execution context") {
    // class to converter data to - class fields have to match CSV header fields
    case class DayTemp(date: LocalDate, minTemp: Double, maxTemp: Double)
    val mh = Map("terrestrial_date" -> "date", "min_temp" -> "minTemp", "max_temp" -> "maxTemp")
    val parser = CSVConfig().mapHeader(mh).parser[IO] // parser with IO effect
    val records = for {
      // ensure resource allocation and  cleanup
      source <- Stream.bracket(IO { SampleTH.sourceFromResource(SampleTH.dataFile) })(source => IO { source.close() })
      record <- Reader.shifting[IO].read(source).through(parser.parse) // get stream of CSV records
    } yield record
    val dayTemps = records
      .map(_.to[DayTemp]) // converter records to DayTemps
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
