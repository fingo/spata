/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.io

import java.nio.file.{Files, Path, StandardOpenOption}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import org.scalameter.Key.exec
import org.scalameter.picklers.noPickler.*
import org.scalameter.{Bench, Gen}
import info.fingo.spata.PerformanceTH.{output, renderer, testRecords, testSource}

/* Check performance of Reader using different implementations. */
object WriterPTS extends Bench.LocalTime:

  val amount = 1_000

  case class WriteMethod(info: String, method: (Path, Stream[IO, Char]) => Stream[IO, Unit]):
    def apply(path: Path, source: Stream[IO, Char]): Stream[IO, Unit] = method(path, source)
    override def toString: String = info

  performance.of("writer").config(exec.maxWarmupRuns := 3, exec.benchRuns := 3) in {
    measure.method("write") in {
      using(methods) in { method =>
        method(output, testSource(amount)).compile.drain.unsafeRunSync()
      }
    }
    measure.method("render_and_write") in {
      using(methods) in { method =>
        testRecords(amount).through(renderer.render).through(src => method(output, src)).compile.drain.unsafeRunSync()
      }
    }
  }

  private lazy val methods = Gen.enumeration("method")(
    WriteMethod(
      "outputstream",
      (path: Path, source: Stream[IO, Char]) =>
        bracket(outputStream(path)).flatMap(os => source.through(Writer.plain[IO].write(os)))
    ),
    WriteMethod(
      "outputstream-fs2io",
      (path: Path, source: Stream[IO, Char]) =>
        bracket(outputStream(path)).flatMap(os => source.through(Writer.shifting[IO].write(os)))
    ),
    WriteMethod("path", (path: Path, source: Stream[IO, Char]) => source.through(Writer.plain[IO].write(path))),
    WriteMethod(
      "path-fs2io",
      (path: Path, source: Stream[IO, Char]) => source.through(Writer.shifting[IO].write(path))
    )
  )

  private def outputStream(path: Path) =
    Files.newOutputStream(
      path,
      StandardOpenOption.WRITE,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
    )
  private def bracket[A <: AutoCloseable](resource: A) =
    Stream.bracket(IO(resource))(resource => IO(resource.close()))
