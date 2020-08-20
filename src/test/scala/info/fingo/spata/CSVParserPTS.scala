/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import java.nio.file.Paths
import java.time.LocalDate
import scala.io.Source
import cats.effect.IO
import info.fingo.spata.io.reader
import org.scalameter.{Bench, Gen}
import org.scalameter.Key.exec

/* Check performance of parser.
 * It would be good to have regression for it but ScalaMeter somehow refused to work in this mode.
 */
object CSVParserPTS extends Bench.LocalTime {

  private val separator = ','
  private val path = Paths.get(getClass.getClassLoader.getResource("mars-weather.csv").toURI)
  private val parser = CSVParser.config.fieldDelimiter(separator).get[IO]()

  performance.of("parser").config(exec.maxWarmupRuns -> 1, exec.benchRuns -> 3) in {
    measure.method("parse_gen") in {
      using(amounts) in { amount =>
        reader[IO]().read(new TestSource(separator, amount)).through(parser.parse).compile.drain.unsafeRunSync()
      }
    }
    measure.method("parse_and_convert_gen") in {
      using(amounts) in { amount =>
        reader[IO]()
          .read(new TestSource(separator, amount))
          .through(parser.parse)
          .map(_.to[(Double, Double, Double, Double, Double, Double, Double, Double, Double, String)])
          .compile
          .drain
          .unsafeRunSync()
      }
    }
    measure.method("parse_and_convert_file") in {
      using(Gen.unit("file")) in { _ =>
        reader[IO]()
          .read(path)
          .through(parser.parse)
          .map(_.to[(Int, LocalDate, Int, Int, String, String, String, String, String, String)])
          .compile
          .drain
          .unsafeRunSync()
      }
    }
  }

  private lazy val amounts = Gen.exponential("amount")(1_000, 25_000, 5)

  private class TestSource(separator: Char, amount: Int) extends Source {
    def csvStream(sep: Char, lines: Int): LazyList[Char] = {
      val cols = 10
      val header = ((1 to cols).mkString("_", s"${sep}_", "") + "\n").to(LazyList)
      val rows =
        LazyList.fill(lines)(s"123.45$sep" * (cols - 1) + "lorem ipsum\n").flatMap(_.toCharArray)
      header #::: rows
    }
    override val iter: Iterator[Char] = csvStream(separator, amount).iterator
  }
}
