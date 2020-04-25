/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.sample

import java.io.IOException
import cats.effect.IO
import fs2.Stream
import info.fingo.spata.{CSVException, CSVReader}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class ErrorITS extends AnyFunSuite with TableDrivenPropertyChecks {

  case class Book(author: String, title: String, year: Int)

  test("spata allows consistently handling errors") {
    forAll(files) { (testCase: String, file: String, row: Int) =>
      val reader = CSVReader.config.get
      val stream = Stream
        .bracket(IO { SampleTH.sourceFromResource(file) })(source => IO { source.close() })
        .through(reader.pipe)
        .map(_.to[Book]())
        .handleErrorWith(ex => Stream.eval(IO(Left(ex)))) // converter global (I/O, CSV structure) errors to Either
      val result = stream.compile.toList.unsafeRunSync()
      assert(result.exists(_.isLeft))
      val errors = result.filter(_.isLeft).map(_.swap.getOrElse(new RuntimeException))
      errors.foreach {
        case e: CSVException => assert(e.getMessage.startsWith(s"Error occurred at row $row (line ${row + 1})"))
        case _: IOException => assert(testCase == "wrongFile")
        case _ => fail
      }
    }
  }

  private lazy val files = Table(
    ("testCase", "fileName", "errorRow"),
    ("wrongFile", "missing.csv", 0),
    ("wrongFormat", "malformed.csv", 3),
    ("wrongType", "wrong-type.csv", 4),
    ("emptyFile", "empty.csv", 0)
  )
}
