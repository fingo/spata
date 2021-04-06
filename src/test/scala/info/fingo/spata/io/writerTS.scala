/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.io

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.concurrent.ExecutionContext
import fs2.{Chunk, Stream}
import cats.effect.{ContextShift, IO}
import info.fingo.spata.io.writer.Writer
import info.fingo.spata.sample.SampleTH
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class writerTS extends AnyFunSuite with TableDrivenPropertyChecks {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  test("writer should properly write to OutputSteam wrapped in effect") {
    forAll(testCases) { (_: String, data: String) =>
      forAll(writers) { (_: String, wrt: Writer[IO]) =>
        val os = new ByteArrayOutputStream()
        val fos = IO[OutputStream](os)
        val input = source(data)
        val output = input.through(wrt.write(fos))
        output.compile.drain.unsafeRunSync()
        assert(os.toByteArray.sameElements(data.getBytes(StandardCharsets.UTF_8)))
      }
    }
  }

  test("writer should properly write to plain OutputSteam") {
    forAll(testCases) { (_: String, data: String) =>
      forAll(writers) { (_: String, wrt: Writer[IO]) =>
        val os = new ByteArrayOutputStream()
        val input = source(data)
        val output = input.through(wrt.write(os))
        output.compile.drain.unsafeRunSync()
        assert(os.toByteArray.sameElements(data.getBytes(StandardCharsets.UTF_8)))
      }
    }
  }

  test("writer should properly write to path") {
    forAll(testCases) { (_: String, data: String) =>
      forAll(writers) { (_: String, wrt: Writer[IO]) =>
        val path = SampleTH.getTempFile.toPath
        val input = source(data)
        val output = input.through(wrt.write(path))
        output.compile.drain.unsafeRunSync()
        assert(data == Files.readString(path))
        Files.delete(path) // deleteOnExit sometimes does not work
      }
    }
  }

  private def source(data: String): Stream[IO, Char] =
    Stream(data).map(s => Chunk.chars(s.toCharArray)).flatMap(Stream.chunk).covary[IO]

  private lazy val writers = Table(
    ("name", "writer"),
    ("plain", writer[IO]()),
    ("shifting", writer.shifting[IO]())
  )

  private lazy val testCases = Table(
    ("testCase", "data"),
    ("simple", "some simple source"),
    ("specialChars", s"source with special characters:\n---\t\r${8.toChar}łoś & 片仮名"),
    ("long", "(very long string)" * 100),
    ("empty", "")
  )
}
