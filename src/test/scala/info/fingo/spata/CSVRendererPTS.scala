/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import cats.effect.unsafe.implicits.global
import org.scalameter.{Bench, Gen}
import org.scalameter.Key.exec
import info.fingo.spata.Record.ProductOps
import info.fingo.spata.PerformanceTH.{mwHeader, renderer, testMarsWeather, testRecords}

/* Check performance of parser. */
class CSVRendererPTS extends Bench.LocalTime {

  performance.of("renderer").config(exec.maxWarmupRuns := 1, exec.benchRuns := 3) in {
    measure.method("render_gen") in {
      using(amounts) in { amount =>
        testRecords(amount).through(renderer.render).compile.drain.unsafeRunSync()
      }
    }
    measure.method("convert_and_render_gen") in {
      using(amounts) in { amount =>
        testMarsWeather(amount).map(_.toRecord).through(renderer.render).compile.drain.unsafeRunSync()
      }
    }
    measure.method("convert_and_render_with_header_gen") in {
      using(amounts) in { amount =>
        testMarsWeather(amount).map(_.toRecord).through(renderer.render(mwHeader)).compile.drain.unsafeRunSync()
      }
    }
  }

  private lazy val amounts = Gen.exponential("amount")(1_000, 25_000, 5)
}
