/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.io

import java.io.{ByteArrayInputStream, IOException}
import java.nio.charset.StandardCharsets
import scala.concurrent.ExecutionContext
import scala.io.{BufferedSource, Source}
import cats.effect.{ContextShift, IO}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class readerTS extends AnyFunSuite with TableDrivenPropertyChecks {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  test("reader should properly load characters from source") {
    forAll(testCases) { (_: String, data: String) =>
      def stream = reader(Source.fromString(data))
      stream.zip(reader.read(Source.fromString(data))).map(p => assert(p._1 == p._2)).compile.drain.unsafeRunSync()
      assert(stream.compile.toList.unsafeRunSync() == data.toList)
    }
  }

  test("reader should allow handling exception with MonadError") {
    val source = new BufferedSource(() => throw new IOException("message"))
    val stream = reader(source)
    val eh = (ex: Throwable) => Stream.emit(ex.isInstanceOf[IOException])
    val result = stream.map(_ => false).handleErrorWith(eh).compile.toList.unsafeRunSync()
    assert(result.length == 1)
    assert(result.head)
  }

  test("reader should properly load characters from source while shifting IO operations to blocking context") {
    forAll(testCases) { (_: String, data: String) =>
      def stream = reader.withBlocker.read(Source.fromString(data))
      stream.zip(reader(Source.fromString(data))).map(p => assert(p._1 == p._2)).compile.drain.unsafeRunSync()
      assert(stream.compile.toList.unsafeRunSync() == data.toList)
    }
  }

  test("reader should properly read from InputSteam") {
    forAll(testCases) { (_: String, data: String) =>
      val input = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))
      val stream = reader.read(input)
      assert(stream.compile.toList.unsafeRunSync() == data.toList)
    }
  }

  test("reader should properly read from InputSteam on blocking context") {
    forAll(testCases) { (_: String, data: String) =>
      val input = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))
      val stream = reader.withBlocker.read(input)
      assert(stream.compile.toList.unsafeRunSync() == data.toList)
    }
  }

  private lazy val testCases = Table(
    ("testCase", "data"),
    ("simple", "some simple source"),
    ("specialChars", s"source with special characters:\n---\t\r${8.toChar}łoś & 片仮名"),
    ("long", "(very long string)" * 100),
    ("empty", "")
  )
}
