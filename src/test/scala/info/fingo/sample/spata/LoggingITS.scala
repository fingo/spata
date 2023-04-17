/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.sample.spata

import java.nio.file.{Files, Paths}
import java.time.LocalDate
import org.slf4j.LoggerFactory
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import info.fingo.spata.io.Reader
import info.fingo.spata.schema.CSVSchema
import info.fingo.spata.schema.validator.FiniteValidator
import info.fingo.spata.util.Logger
import info.fingo.spata.{CSVConfig, CSVParser}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

/* Sample which show logging configuration and usage */
class LoggingITS extends AnyFunSuite with BeforeAndAfter {

  val logFile = "./target/spata.log" // has to match org.slf4j.simpleLogger.logFile property
  private val logFilePath = Paths.get(logFile)
  private val logger = LoggerFactory.getLogger(this.getClass) // regular, impure logger
  // turn on spata logging (logging operations are suspended in IO)
  private given spataLogger: Logger[IO] = new Logger[IO](LoggerFactory.getLogger("spata"))

  private def getSource() = SampleTH.sourceFromResource(SampleTH.dataFile)

  before {
    Files.writeString(logFilePath, "")
  }

  test("spata allows logging basic operations") {
    val parser = CSVConfig().mapHeader(Map("max_temp" -> "temp")).stripSpaces.parser[IO] // parser with IO effect
    // get stream of CSV records while ensuring source cleanup
    val records = Stream
      .bracket(IO(getSource()))(source => IO(source.close()))
      .through(Reader.plain[IO].by)
      .through(parser.parse)
    // find maximum temperature
    val maximum = records
      .map(_.get[Double]("temp"))
      .filter(_.exists(!_.isNaN))
      .rethrow // rethrow to get rid of Either
      .fold(-273.15)(Math.max)
      .handleErrorWith(ex =>
        logger.error(s"An error occurred while processing ${SampleTH.dataFile}: ${ex.getMessage}", ex)
        Stream.empty[IO]
      )
    // get the IO effect with it final result
    val io = maximum.compile.toList.map(l =>
      val t = l.headOption.getOrElse(fail())
      logger.debug(f"Maximum recorded temperature is $t%.1f° C")  // logged while evalueting IO
      assert(t > 0)
      t
    )
    logger.debug("CSV parsing with logging - start")  // side effect - logged outside of IO
    assert(!Files.readString(logFilePath).contains("spata -"))
    // evaluate effect - trigger all stream operations
    val maxTemp = io.unsafeRunSync()
    logger.debug("CSV parsing with logging - finish")  // side effect - logged outside of IO

    val log = Files.readString(logFilePath)
    assert(log.contains("INFO spata - Parsing CSV"))
    assert(log.contains("DEBUG spata - CSV parsing finished"))
    maxTemp
  }

  test("spata allows logging validation information") {
    val parser = CSVParser.config.stripSpaces.parser[IO] // parser with IO effect
    val schema = CSVSchema()
      .add[LocalDate]("terrestrial_date")
      .add[Double]("min_temp", FiniteValidator()) // NaN is not accepted
      .add[Double]("max_temp", FiniteValidator())
    val stream = Stream
      .bracket(IO(getSource()))(source => IO(source.close())) // ensure resource cleanup
      .through(Reader.plain[IO].by)
      .through(parser.parse) // get stream of CSV records
      .through(schema.validate) // validate against schema, get stream of Validated
      .map(
        _.bimap( // map over invalid and valid part of Validated
          invalidRecord => logger.warn(invalidRecord.toString),
          typedRecord => (typedRecord("terrestrial_date"), typedRecord("max_temp") - typedRecord("min_temp"))
        )
      )
      .handleErrorWith(ex => fail(ex.getMessage)) // fail test on any stream error
    assert(!Files.readString(logFilePath).contains("spata -"))
    stream.compile.toList.unsafeRunSync()
    val log = Files.readString(logFilePath)
    assert(log.indexOf("DEBUG spata - Validating CSV") > log.indexOf("INFO spata - Parsing CSV"))
  }
}
