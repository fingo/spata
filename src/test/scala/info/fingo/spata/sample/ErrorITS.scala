package info.fingo.spata.sample

import cats.effect.IO
import fs2.Stream
import info.fingo.spata.CSVReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class ErrorITS extends AnyFunSuite with TableDrivenPropertyChecks {

  case class Book(author: String, title: String, year: Int)

  test("spata allows consistently handling errors") {
    forAll(files) { (_: String, file: String) =>
      val reader = CSVReader.config.get
      val stream = Stream
        .bracket(IO { SampleTH.sourceFromResource(file) })(source => IO { source.close() })
        .through(reader.pipe)
        .map(_.to[Book]())
        .handleErrorWith(ex => Stream.eval(IO(Left(ex)))) // converter global (I/O, CSV structure) errors to Either
      val result = stream.compile.toList.unsafeRunSync()
      assert(result.exists(_.isLeft))
    }
  }

  private lazy val files = Table(
    ("testCase", "fileName"),
    ("wrongFile", "missing.csv"),
    ("wrongFormat", "malformed.csv"),
    ("wrongType", "wrong-type.csv"),
    ("emptyFile", "empty.csv")
  )
}
