/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.sample

import cats.effect.IO
import fs2.Stream
import org.slf4j.LoggerFactory
import info.fingo.spata.CSVParser
import info.fingo.spata.io.reader
import info.fingo.spata.util.{Logger, SLF4JLogger}
import org.scalatest.funsuite.AnyFunSuite

/* Sample which show logging configuration and usage */
class LoggingITS extends AnyFunSuite {

  private val logger = LoggerFactory.getLogger(this.getClass)
  // turn on spata logging
  implicit private val spataLogger: Logger[IO] = new SLF4JLogger[IO](LoggerFactory.getLogger("spata"))

  test("spata allows manipulate data using stream functionality") {
    val parser = CSVParser[IO]() // parser with default configuration and IO effect
    // get stream of CSV records while ensuring source cleanup
    val records = Stream
      .bracket(IO { SampleTH.sourceFromResource(SampleTH.dataFile) })(source => IO { source.close() })
      .through(reader[IO]().by)
      .through(parser.parse)
    // find maximum temperature
    val maximum = records
      .map(_.get[Double]("max_temp"))
      .filter(_.exists(!_.isNaN))
      .rethrow // rethrow to get rid of Either
      .fold(-273.15)(Math.max)
      .handleErrorWith { ex =>
        logger.error(s"An error occurred while processing ${SampleTH.dataFile}: ${ex.getMessage}", ex)
        Stream.empty[IO]
      }
    // get the IO effect with it final result
    val io = maximum.compile.toList.map { l =>
      val t = l.headOption.getOrElse(fail())
      assert(t > 0)
      logger.debug(f"Maximum recorded temperature is $t%.1f° C")
      t
    }
    // evaluate effect - trigger all stream operations
    logger.debug("CSV parsing with logging - start")
    val maxTemp = io.unsafeRunSync()
    logger.debug("CSV parsing with logging - finish")
    maxTemp
  }

}
