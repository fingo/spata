/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.io

import java.nio.file.{Files, Path, StandardOpenOption}
import scala.io.Source
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import org.scalameter.{Bench, Gen}
import org.scalameter.Key.exec
import org.scalameter.picklers.noPickler._
import info.fingo.spata.PerformanceTH.{input, parser}

/* Check performance of Reader using different implementations.
 * It would be good to have regression for it but ScalaMeter somehow refused to work in this mode.
 */
object ReaderPTS extends Bench.LocalTime {

  case class ReadMethod(info: String, method: Path => Stream[IO, Char]) {
    def apply(path: Path): Stream[IO, Char] = method(path)
    override def toString: String = info
  }

  performance.of("reader").config(exec.maxWarmupRuns := 3, exec.benchRuns := 3) in {
    measure.method("read") in {
      using(methods) in { method =>
        method(input).compile.drain.unsafeRunSync()
      }
    }
    measure.method("read_and_parse") in {
      using(methods) in { method =>
        method(input).through(parser.parse).compile.drain.unsafeRunSync()
      }
    }
  }

  private lazy val methods = Gen.enumeration("method")(
    ReadMethod("source", (path: Path) => bracket(source(path)).through(Reader.plain[IO].by)),
    ReadMethod("source-fs2io", (path: Path) => bracket(source(path)).through(Reader.shifting[IO].by)),
    ReadMethod("inputstream", (path: Path) => bracket(inputStream(path)).through(Reader.plain[IO].by)),
    ReadMethod("inputstream-fs2io", (path: Path) => bracket(inputStream(path)).through(Reader.shifting[IO].by)),
    ReadMethod("path", (path: Path) => Reader.plain[IO].read(path)),
    ReadMethod("path-fs2io", (path: Path) => Reader.shifting[IO].read(path))
  )

  private def inputStream(path: Path) = Files.newInputStream(path, StandardOpenOption.READ)
  private def source(path: Path) = Source.fromFile(path.toFile)
  private def bracket[A <: AutoCloseable](resource: A) =
    Stream.bracket(IO(resource))(resource => IO { resource.close() })
}
