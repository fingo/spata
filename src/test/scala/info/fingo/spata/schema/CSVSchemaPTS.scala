/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import java.time.LocalDate
import cats.effect.IO
import org.scalameter.{Bench, Gen}
import org.scalameter.Key.exec
import info.fingo.spata.io.Reader
import info.fingo.spata.schema.validator.RangeValidator
import info.fingo.spata.PerformanceTH.{input, parser, MarsWeather, TestSource}

/* Check performance of schema validation.
 * It would be good to have regression for it but ScalaMeter somehow refused to work in this mode.
 */
class CSVSchemaPTS extends Bench.LocalTime {

  private val schemaGen = CSVSchema()
    .add[Double]("_1", RangeValidator(100.0, 200.0))
    .add[Double]("_2")
    .add[Double]("_3", RangeValidator(100.0, 200.0))
    .add[Double]("_4")
    .add[Double]("_5", RangeValidator(100.0, 200.0))
    .add[Double]("_6")
    .add[Double]("_7", RangeValidator(100.0, 200.0))
    .add[Double]("_8")
    .add[Double]("_9", RangeValidator(100.0, 200.0))
    .add[String]("_10")

  private val schemaFile = CSVSchema()
    .add[Int]("id")
    .add[LocalDate]("terrestrial_date")
    .add[Int]("sol")
    .add[Int]("ls")
    .add[String]("month")
    .add[Double]("min_temp")
    .add[Double]("max_temp")
    .add[Double]("pressure")
    .add[Double]("wind_speed")
    .add[String]("atmo_opacity")

  performance.of("schema").config(exec.maxWarmupRuns := 1, exec.benchRuns := 3) in {
    measure.method("validate_gen") in {
      using(amounts) in { amount =>
        Reader[IO]
          .read(new TestSource(amount))
          .through(parser.parse)
          .through(schemaGen.validate)
          .compile
          .drain
          .unsafeRunSync()
      }
    }
    measure.method("validate_and_convert_file") in {
      using(Gen.unit("file")) in { _ =>
        Reader[IO]
          .read(input)
          .through(parser.parse)
          .through(schemaFile.validate)
          .map(_.map(_.to[MarsWeather]()))
          .compile
          .drain
          .unsafeRunSync()
      }
    }
  }

  private lazy val amounts = Gen.exponential("amount")(1_000, 25_000, 5)
}
