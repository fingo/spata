/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.io

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import scala.io.Source
import scala.concurrent.ExecutionContext
import cats.effect.{ContextShift, IO}
import fs2.Stream
import info.fingo.spata.CSVParser
import org.scalameter.{Bench, Gen}
import org.scalameter.Key.exec
import org.scalameter.picklers.noPickler._

/* Check performance of reader using different implementations.
 * It would be good to have regression for it but ScalaMeter somehow refused to work in this mode.
 */
object readerPTS extends Bench.LocalTime {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private val path = Paths.get(getClass.getClassLoader.getResource("mars-weather.csv").toURI)
  private val parser = CSVParser.config.get[IO] // parser with default configuration

  case class ReadMethod(info: String, method: Path => Stream[IO, Char]) {
    def apply(path: Path): Stream[IO, Char] = method(path)
    override def toString: String = info
  }

  performance.of("reader").config(exec.maxWarmupRuns -> 3, exec.benchRuns -> 3) in {
    measure.method("read") in {
      using(methods) in { method =>
        method(path).compile.drain.unsafeRunSync()
      }
    }
    measure.method("read_and_parse") in {
      using(methods) in { method =>
        method(path).through(parser.parse).compile.drain.unsafeRunSync()
      }
    }
  }

  private lazy val methods = Gen.enumeration("method")(
    ReadMethod("source", (path: Path) => bracket(source(path)).through(reader[IO]().by)),
    ReadMethod("source-fs2io", (path: Path) => bracket(source(path)).through(reader.shifting[IO]().by)),
    ReadMethod("inputstream", (path: Path) => bracket(inputStream(path)).through(reader[IO]().by)),
    ReadMethod("inputstream-fs2io", (path: Path) => bracket(inputStream(path)).through(reader.shifting[IO]().by)),
    ReadMethod("path", (path: Path) => reader[IO]().read(path)),
    ReadMethod("path-fs2io", (path: Path) => reader.shifting[IO]().read(path))
  )

  private def inputStream(path: Path) = Files.newInputStream(path, StandardOpenOption.READ)
  private def source(path: Path) = Source.fromFile(path.toFile)
  private def bracket[A <: AutoCloseable](resource: A) =
    Stream.bracket(IO(resource))(resource => IO { resource.close() })
}
