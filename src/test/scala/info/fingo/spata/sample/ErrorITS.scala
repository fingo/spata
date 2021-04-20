/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.sample

import java.io.IOException
import cats.effect.IO
import fs2.{Pipe, Stream}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import info.fingo.spata.io.Reader
import info.fingo.spata.{CSVParser, CSVRenderer, Header, Record, RecordBuilder}
import info.fingo.spata.error.CSVException

class ErrorITS extends AnyFunSuite with TableDrivenPropertyChecks {

  case class Book(author: String, title: String, year: Int)

  test("spata allows consistently handling parsing errors") {
    forAll(files) { (testCase: String, file: String, row: Int) =>
      val parser = CSVParser[IO]()
      val stream = Stream
        .bracket(IO { SampleTH.sourceFromResource(file) })(source => IO { source.close() })
        .flatMap(Reader.plain[IO]().read)
        .through(parser.parse)
        .map(_.to[Book]())
        .handleErrorWith(ex => Stream.eval(IO(Left(ex)))) // converter global (I/O, CSV structure) errors to Either
      val result = stream.compile.toList.unsafeRunSync()
      assert(result.exists(_.isLeft))
      val errors = result.filter(_.isLeft).map(_.swap.getOrElse(new RuntimeException))
      errors.foreach {
        case e: CSVException => assert(e.getMessage.startsWith(s"Error occurred at row $row (line ${row + 1})"))
        case _: IOException => assert(testCase == "wrongFile")
        case _ => fail()
      }
    }
  }

  test("spata allows handling rendering errors") {
    val source = (1 to 10).map(i => (i, s"text$i"))
    def result(record: (Int, String) => Record)(render: Pipe[IO, Record, Char]): List[Either[Throwable, Char]] = {
      // stream with some regular records and the last one which differs
      val records = Stream(source: _*).covary[IO].map { case (id, text) => record(id, text) } ++
        Stream(Record.fromPairs("id" -> "0")) // add invalid record, with fewer fields
      // error handler
      val eh = (ex: Throwable) => Stream.eval(IO(Left(ex)))
      //  render and execute
      records.through(render).map(Right.apply).handleErrorWith(eh).compile.toList.unsafeRunSync()
    }
    val renderer = CSVRenderer[IO]()
    // explicit header
    val header = Header("id", "text")
    val es = result { (i, t) =>
      Record(i.toString, t)(header)
    }(renderer.render(header))
    assert(es.init.forall(_.isRight))
    assert(es.last.isLeft)
    // implicit header
    val is = result { (i, t) =>
      RecordBuilder().add("id", i).add("text", t).result()
    }(renderer.render)
    assert(is.init.forall(_.isRight))
    assert(is.head.isRight)
  }

  private lazy val files = Table(
    ("testCase", "fileName", "errorRow"),
    ("wrongFile", "missing.csv", 0),
    ("wrongFormat", "malformed.csv", 3),
    ("wrongType", "wrong-type.csv", 4),
    ("emptyFile", "empty.csv", 0)
  )
}
