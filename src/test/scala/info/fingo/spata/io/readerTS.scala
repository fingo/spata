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
import cats.effect.{ContextShift, IO}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class readerTS extends AnyFunSuite with TableDrivenPropertyChecks {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  // use smaller chunk size in tests than the default one to avoid having all data in single chunk
  private val chunkSize = 16

  test("reader should properly load characters from source") {
    forAll(testCases) { (_: String, data: String) =>
      def stream = reader[IO](chunkSize).read(Source.fromString(data))
      stream
        .zip(reader[IO]().read(Source.fromString(data)))
        .map(p => assert(p._1 == p._2))
        .compile
        .drain
        .unsafeRunSync()
      assert(stream.compile.toList.unsafeRunSync() == data.toList)
    }
  }

  test("reader should allow handling exception with MonadError") {
    val source = new BufferedSource(() => throw new IOException("message"))
    val stream = reader[IO]().read(source)
    val eh = (ex: Throwable) => Stream.emit(ex.isInstanceOf[IOException])
    val result = stream.map(_ => false).handleErrorWith(eh).compile.toList.unsafeRunSync()
    assert(result.length == 1)
    assert(result.head)
  }

  test("reader should properly load characters from source while shifting IO operations to blocking context") {
    forAll(testCases) { (_: String, data: String) =>
      def stream = reader.shifting[IO](chunkSize).read(Source.fromString(data))
      stream
        .zip(reader[IO]().read(Source.fromString(data)))
        .map(p => assert(p._1 == p._2))
        .compile
        .drain
        .unsafeRunSync()
      assert(stream.compile.toList.unsafeRunSync() == data.toList)
    }
  }

  test("reader should properly read from InputSteam") {
    forAll(testCases) { (_: String, data: String) =>
      val input = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))
      val stream = reader[IO](chunkSize).read(input)
      assert(stream.compile.toList.unsafeRunSync() == data.toList)
    }
  }

  test("reader should properly read from InputSteam on blocking context") {
    forAll(testCases) { (_: String, data: String) =>
      val input = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))
      val stream = reader.shifting[IO](chunkSize).read(input)
      assert(stream.compile.toList.unsafeRunSync() == data.toList)
    }
  }

  test("reader should properly read from path") {
    val path = Paths.get(getClass.getClassLoader.getResource("sample.csv").toURI)
    val stream = reader[IO](chunkSize).read(path)
    val content = stream.compile.toList.unsafeRunSync()
    assert(content.mkString == Files.readString(path))
  }

  test("reader should properly read from path using blocking context") {
    val path = Paths.get(getClass.getClassLoader.getResource("sample.csv").toURI)
    val stream = reader.shifting[IO](chunkSize).read(path)
    val content = stream.compile.toList.unsafeRunSync()
    assert(content.mkString == Files.readString(path))
  }

  test("reader should properly read UTF-8 files with BOM") {
    val localChar = 'ł'
    val path = Paths.get(getClass.getClassLoader.getResource("bom.csv").toURI)
    val stream = reader[IO](chunkSize).apply(path)
    val content = stream.compile.toList.unsafeRunSync()
    assert(content.startsWith("author"))
    assert(content.contains(localChar))
  }

  test("reader should properly read files with non-UTF encoding") {
    val localChar = 'ł'
    implicit val codec: Codec = new Codec(Charset.forName("windows-1250"))
    val path = Paths.get(getClass.getClassLoader.getResource("windows1250.csv").toURI)
    val stream = reader[IO](chunkSize).read(path)
    val content = stream.compile.toList.unsafeRunSync()
    assert(content.startsWith("author"))
    assert(content.contains(localChar))
  }

  test("reader should properly read files with non-UTF encoding using blocking context") {
    val localChar = 'ł'
    implicit val codec: Codec = new Codec(Charset.forName("windows-1250"))
    val path = Paths.get(getClass.getClassLoader.getResource("windows1250.csv").toURI)
    val is = Files.newInputStream(path, StandardOpenOption.READ)
    val stream = Stream.bracket(IO(is))(resource => IO { resource.close() }).through(reader.shifting[IO](chunkSize).by)
    val content = stream.compile.toList.unsafeRunSync()
    assert(content.startsWith("author"))
    assert(content.contains(localChar))
  }

  test("reader should properly handle charset conversion errors while using blocking context") {
    val CAN = 0x18.toChar
    implicit val codec: Codec = new Codec(StandardCharsets.UTF_8)
    val path = Paths.get(getClass.getClassLoader.getResource("windows1250.csv").toURI)
    val is = Files.newInputStream(path, StandardOpenOption.READ)
    val stream = Stream.bracket(IO(is))(resource => IO { resource.close() }).through(reader.shifting[IO](chunkSize).by)
    val content = stream.handleErrorWith(_ => Stream.emit(CAN)).compile.toList.unsafeRunSync()
    assert(content == List(CAN))
  }

  private lazy val testCases = Table(
    ("testCase", "data"),
    ("simple", "some simple source"),
    ("specialChars", s"source with special characters:\n---\t\r${8.toChar}łoś & 片仮名"),
    ("long", "(very long string)" * 100),
    ("empty", "")
  )
}
