/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.io

import java.io.{ByteArrayInputStream, IOException}
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.ExecutionContext
import scala.io.{BufferedSource, Codec, Source}
import cats.effect.{Blocker, ContextShift, IO}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class ReaderTS extends AnyFunSuite with TableDrivenPropertyChecks {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  // use smaller chunk size than the default one is some tests to avoid having all data in single chunk
  private val chunkSize = 16

  test("reader should properly load characters from source") {
    forAll(testCases) { (_: String, data: String) =>
      forAll(readers) { (_: String, rdr: Reader[IO]) =>
        def stream = rdr.read(Source.fromString(data))
        stream
          .zip(Reader[IO]().read(Source.fromString(data)))
          .map { case (chunkedChar, notChunkedChar) => assert(chunkedChar == notChunkedChar) }
          .compile
          .drain
          .unsafeRunSync()
        assert(stream.compile.toList.unsafeRunSync() == data.toList)
      }
    }
  }

  test("reader should allow handling exception with MonadError") {
    val source = new BufferedSource(() => throw new IOException("message"))
    forAll(readers) { (_: String, rdr: Reader[IO]) =>
      val stream = rdr.read(source)
      val eh = (ex: Throwable) => Stream.emit(ex.isInstanceOf[IOException])
      val result = stream.map(_ => false).handleErrorWith(eh).compile.toList.unsafeRunSync()
      assert(result.length == 1)
      assert(result.head)
    }
  }

  test("reader should properly read from InputSteam wrapped in effect") {
    forAll(testCases) { (_: String, data: String) =>
      forAll(readers) { (_: String, rdr: Reader[IO]) =>
        val input = IO(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)))
        val stream = rdr.read(input)
        assert(stream.compile.toList.unsafeRunSync() == data.toList)
      }
    }
  }

  test("reader should properly read from plain InputSteam") {
    forAll(testCases) { (_: String, data: String) =>
      forAll(readers) { (_: String, rdr: Reader[IO]) =>
        val input = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))
        val stream = rdr.read(input)
        assert(stream.compile.toList.unsafeRunSync() == data.toList)
      }
    }
  }

  test("reader should properly read from path") {
    val path = Paths.get(getClass.getClassLoader.getResource("sample.csv").toURI)
    forAll(readers) { (_: String, rdr: Reader[IO]) =>
      val stream = rdr.read(path)
      val content = stream.compile.toList.unsafeRunSync()
      assert(content.mkString == Files.readString(path))
    }
  }

  test("reader should properly read UTF-8 files with BOM") {
    val localChar = 'ł'
    val path = Paths.get(getClass.getClassLoader.getResource("bom.csv").toURI)
    forAll(readers) { (_: String, rdr: Reader[IO]) =>
      val stream = rdr(path)
      val content = stream.compile.toList.unsafeRunSync()
      assert(content.startsWith("author"))
      assert(content.contains(localChar))
    }
  }

  test("reader should properly read files with non-UTF encoding") {
    val localChar = 'ł'
    implicit val codec: Codec = new Codec(Charset.forName("windows-1250"))
    val path = Paths.get(getClass.getClassLoader.getResource("windows1250.csv").toURI)
    forAll(readers) { (_: String, rdr: Reader[IO]) =>
      val stream = rdr.read(path)
      val content = stream.compile.toList.unsafeRunSync()
      assert(content.startsWith("author"))
      assert(content.contains(localChar))
    }
  }

  test("reader should properly handle charset conversion errors while using blocking context") {
    val CAN = 0x18.toChar
    implicit val codec: Codec = new Codec(StandardCharsets.UTF_8)
    val path = Paths.get(getClass.getClassLoader.getResource("windows1250.csv").toURI)
    val fis = IO(Files.newInputStream(path, StandardOpenOption.READ))
    val stream = Stream.bracket(fis)(resource => IO { resource.close() }).through(Reader.shifting[IO](chunkSize).by)
    val content = stream.handleErrorWith(_ => Stream.emit(CAN)).compile.toList.unsafeRunSync()
    assert(content == List(CAN))
  }

  private lazy val readers = Table(
    ("name", "reader"),
    ("plain", Reader[IO]()),
    ("plain custom", Reader[IO](chunkSize)),
    ("shifting", Reader.shifting[IO]()),
    ("shifting custom", Reader.shifting[IO](chunkSize)),
    ("blocker", Reader.shifting[IO](Blocker.liftExecutionContext(ExecutionContext.global))),
    ("blocker custom", Reader.shifting[IO](Blocker.liftExecutionContext(ExecutionContext.global), chunkSize))
  )

  private lazy val testCases = Table(
    ("testCase", "data"),
    ("simple", "some simple source"),
    ("specialChars", s"source with special characters:\n---\t\r${8.toChar}łoś & 片仮名"),
    ("long", "(very long string)" * 100),
    ("empty", "")
  )
}
