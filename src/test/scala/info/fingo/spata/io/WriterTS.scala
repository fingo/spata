/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.io

import java.io.{ByteArrayOutputStream, File, IOException, OutputStream}
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Path}
import scala.io.Codec
import fs2.{Chunk, Stream}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class WriterTS extends AnyFunSuite with TableDrivenPropertyChecks:

  private val defaultCharset = StandardCharsets.UTF_8

  test("writer should properly write to OutputSteam wrapped in effect") {
    forAll(testCases)((_: String, data: String) =>
      forAll(writers)((_: String, writer: Writer[IO]) =>
        val os = new ByteArrayOutputStream()
        val fos = IO[OutputStream](os)
        val input = source(data)
        val output = input.through(writer(fos))
        output.compile.drain.unsafeRunSync()
        assert(os.toByteArray.sameElements(data.getBytes(defaultCharset)))
      )
    )
  }

  test("writer should properly write to plain OutputSteam") {
    forAll(testCases)((_: String, data: String) =>
      forAll(writers)((_: String, writer: Writer[IO]) =>
        val os = new ByteArrayOutputStream()
        val input = source(data)
        val output = input.through(writer(os))
        output.compile.drain.unsafeRunSync()
        assert(os.toByteArray.sameElements(data.getBytes(defaultCharset)))
      )
    )
  }

  test("writer should properly write to path") {
    forAll(testCases)((_: String, data: String) =>
      forAll(writers)((_: String, writer: Writer[IO]) =>
        val path = getTempPath
        val input = source(data)
        val output = input.through(writer(path))
        output.compile.drain.unsafeRunSync()
        assert(data == Files.readString(path))
        Files.delete(path) // deleteOnExit sometimes does not work
      )
    )
  }

  test("writer should properly write to OutputSteam with non-UTF encoding") {
    val charset = Charset.forName("windows-1250")
    given codec: Codec(charset)
    forAll(testCases)((testCase: String, data: String) =>
      if testCase != "extended chars" then // not supported by encoding
        forAll(writers)((_: String, writer: Writer[IO]) =>
          val os = new ByteArrayOutputStream()
          val fos = IO[OutputStream](os)
          val input = source(data)
          val output = input.through(writer(fos))
          output.compile.drain.unsafeRunSync()
          assert(os.toByteArray.sameElements(data.getBytes(charset)))
        )
    )
  }

  test("writer should properly write to path with non-UTF encoding") {
    val charset = Charset.forName("windows-1250")
    implicit val codec: Codec = new Codec(charset)
    forAll(testCases)((testCase: String, data: String) =>
      if testCase != "extended chars" then // not supported by encoding
        forAll(writers)((_: String, writer: Writer[IO]) =>
          val path = getTempPath
          val input = source(data)
          val output = input.through(writer(path))
          output.compile.drain.unsafeRunSync()
          assert(data == Files.readString(path, charset))
          Files.delete(path) // deleteOnExit sometimes does not work
        )
    )
  }

  test("writer should allow handling exception with MonadError") {
    val target = new OutputStream:
      override def write(b: Int): Unit = throw new IOException("message")
    forAll(writers)((_: String, writer: Writer[IO]) =>
      val stream = Stream('a', 'n', 'y').lift[IO].through(writer(target)).map(_ => false)
      val eh = (ex: Throwable) => Stream.emit(ex.isInstanceOf[IOException])
      val result = stream.handleErrorWith(eh).compile.toList.unsafeRunSync()
      assert(result.length == 1)
      assert(result.head)
    )
  }

  private def source(data: String): Stream[IO, Char] =
    Stream(data).map(s => Chunk.array[Char](s.toCharArray)).flatMap(Stream.chunk).covary[IO]

  /* Create temporary file with random name, removes it after closing. */
  def getTempPath: Path =
    val temp = File.createTempFile("spata_", ".csv")
    temp.deleteOnExit()
    temp.toPath

  private lazy val writers = Table(
    ("name", "writer"),
    ("plain", Writer.plain[IO]),
    ("shifting", Writer.shifting[IO])
  )

  private lazy val testCases = Table(
    ("testCase", "data"),
    ("simple", "some simple source"),
    ("local chars", s"source with local characters: lękliwy łoś"),
    ("extended chars", s"source with special characters:\n---\t\r${8.toChar} & 片仮名"),
    ("long", "(very long string)" * 100),
    ("empty", "")
  )
