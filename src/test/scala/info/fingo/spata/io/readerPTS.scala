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
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class readerPTS extends AnyFunSuite with TableDrivenPropertyChecks {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private val path = Paths.get(getClass.getClassLoader.getResource("mars-weather.csv").toURI)
  private val parser = CSVParser.config.get // parser with default configuration

  test("reader should perform well in simple char-reading scenarios") {
    forAll(testCases) { (testCase: String, f: Path => Stream[IO, Char]) =>
      val (_, time) = elapsed(f(path).take(25_000).compile.drain.unsafeRunSync())
      println(s"$testCase: $time")
      assert(time < 1000)
    }
  }

  test("reader should perform well in parsing scenarios") {
    forAll(testCases) { (testCase: String, f: Path => Stream[IO, Char]) =>
      val (_, time) = elapsed(f(path).through(parser.parse).take(500).compile.drain.unsafeRunSync())
      println(s"$testCase: $time")
      assert(time < 1000)
    }
  }

  private def elapsed[A](code: => A): (A, Long) = {
    code // warm-up, fill caches
    val start = System.nanoTime()
    val ret = code
    val end = System.nanoTime()
    (ret, (end - start) / 1_000_000)
  }

  private def inputStream(path: Path) = Files.newInputStream(path, StandardOpenOption.READ)
  private def source(path: Path) = Source.fromFile(path.toFile)
  private def bracket[A <: AutoCloseable](resource: A) =
    Stream.bracket(IO(resource))(resource => IO { resource.close() })

  private lazy val testCases = Table(
    ("testCase", "function"),
    ("source", (path: Path) => bracket(source(path)).through(reader.by)),
    ("source-fs2io", (path: Path) => bracket(source(path)).through(reader.withBlocker.by)),
    ("is", (path: Path) => bracket(inputStream(path)).through(reader.by)),
    ("is-fs2io", (path: Path) => bracket(inputStream(path)).through(reader.withBlocker.by)),
    ("path", (path: Path) => reader.read(path)),
    ("path-fs2io", (path: Path) => reader.withBlocker.read(path))
  )
}
