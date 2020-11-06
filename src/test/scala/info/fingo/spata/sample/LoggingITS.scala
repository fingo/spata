/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.sample

import java.nio.file.{Files, Paths}
import cats.effect.IO
import fs2.Stream
import org.slf4j.LoggerFactory
import org.scalatest.funsuite.AnyFunSuite
import info.fingo.spata.CSVParser
import info.fingo.spata.io.reader
import info.fingo.spata.util.Logger

/* Sample which show logging configuration and usage */
class LoggingITS extends AnyFunSuite {

  val logFile = "./target/spata.log" // has to match org.slf4j.simpleLogger.logFile property
  private val logger = LoggerFactory.getLogger(this.getClass) // regular, impure logger
  // turn on spata logging (logging operations are suspended in IO)
  implicit private val spataLogger: Logger[IO] = new Logger[IO](LoggerFactory.getLogger("spata"))

  test("spata allows logging basic operations") {
    val parser = CSVParser.config.mapHeader(Map("max_temp" -> "temp")).get[IO]() // parser with IO effect
    // get stream of CSV records while ensuring source cleanup
    val records = Stream
      .bracket(IO { SampleTH.sourceFromResource(SampleTH.dataFile) })(source => IO { source.close() })
      .through(reader[IO]().by)
      .through(parser.parse)
    // find maximum temperature
    val maximum = records
      .map(_.get[Double]("temp"))
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
      logger.debug(f"Maximum recorded temperature is $t%.1f° C")
      assert(t > 0)
      t
    }
    logger.debug("CSV parsing with logging - start")
    assert(!Files.readString(Paths.get(logFile)).contains("spata"))
    // evaluate effect - trigger all stream operations
    val maxTemp = io.unsafeRunSync()
    logger.debug("CSV parsing with logging - finish")

    val log = Files.readString(Paths.get(logFile))
    assert(log.contains("INFO spata - Parsing CSV"))
    assert(log.contains("DEBUG spata - CSV parsing finished"))
    maxTemp
  }

}
