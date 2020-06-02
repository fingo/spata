spata
=====

[![Build Status](https://travis-ci.org/fingo/spata.svg?branch=master)](https://travis-ci.org/fingo/spata)

**spata** is a functional Scala parser for tabular data (`CSV`).
The library is based on [FS2 - Functional Streams for Scala](https://github.com/functional-streams-for-scala/fs2).

Main goal of the library is to provide handy, functional API and precise information about errors in source data
(their location) while keeping good performance.

The source data format is assumed to conform to [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt).
It is possible however to configure the parser to accept separator and quote symbols - see CSVConfig for details.

spata requires Java 11.

Usage
-----
Basic usage:
```scala
import scala.io.Source
import cats.effect.IO
import fs2.Stream
import info.fingo.spata.CSVParser
import info.fingo.spata.io.reader

case class Data(item: String, value: Double)
val parser = CSVParser.config.get // parser with default configuration
val records = Stream
  // get stream of CSV records while ensuring source cleanup
  .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
  .flatMap(reader.read) // produce stream of chars from source
  .through(parser.parse)  // parse csv file and get csv records 
  .filter(_.get[Double]("value") > 1000)  // do some operations using Stream API
  .map(_.to[Data]()) // converter records to case class
  .handleErrorWith(ex => Stream.eval(IO(Left(ex)))) // converter global (I/O, CSV structure) errors to Either
val result = records.compile.toList.unsafeRunSync // run everything while converting result to list
```

Another example may be taken from [[https://fs2.io/ FS2 readme]] assuming,
that the data is stored in CSV format with two fields, `date` and `temp`:
```scala
import java.nio.file.Paths
import scala.io.Codec
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import cats.implicits._
import fs2.Stream
import fs2.io
import fs2.text
import info.fingo.spata.CSVParser
import info.fingo.spata.io.reader

object Converter extends IOApp {

  val converter: Stream[IO, Unit] = Stream.resource(Blocker[IO]).flatMap {
    blocker =>
      implicit val codec: Codec = Codec.UTF8
      val parser: CSVParser = CSVParser.config.get
      def fahrenheitToCelsius(f: Double): Double =
        (f - 32.0) * (5.0 / 9.0)

      reader
        .withBlocker(blocker)
        .read(Paths.get("testdata/fahrenheit.txt"))
        .through(parser.parse)
        .filter(r => !r("temp").isEmpty)
        .map { r =>
          val date = r("date")
          val temp = fahrenheitToCelsius(r.get[Double]("temp"))
          s"$date,$temp"
        }
        .intersperse("\n")
        .through(text.utf8Encode)
        .through(io.file.writeAll(Paths.get("testdata/celsius.txt"), blocker))
  }

  def run(args: List[String]): IO[ExitCode] =
    converter.compile.drain.as(ExitCode.Success)
}
```

More examples how to use the library may be found in `src/test/scala/sample`.

Credits
-------

**spata** makes use of following tools, languages, frameworks, libraries and data sets (in alphabetical order):
* [Cats Effect](https://typelevel.org/cats-effect/) licensed under [Apache 2.0](https://github.com/typelevel/cats-effect/blob/master/LICENSE.txt) /R
* [FS2](https://fs2.io/) licensed under [MIT](https://github.com/functional-streams-for-scala/fs2/blob/master/LICENSE) /R
* [Git](https://git-scm.com/) licensed under [GPL-2.0](https://git-scm.com/about/free-and-open-source) /D
* [http4s](https://http4s.org/) licensed under [Apache 2.0](https://github.com/http4s/http4s#license) /S
* [IntelliJ IDEA CE](https://www.jetbrains.com/idea/) licensed under [Apache 2.0](https://www.jetbrains.com/idea/download/) /D
* [Mars weather data](https://github.com/the-pudding/data/tree/master/mars-weather) made publicly available by [NASA](https://pds.nasa.gov/) and [CAB](https://cab.inta-csic.es/rems/en) /T
* [OpenJDK](https://adoptopenjdk.net/) licensed under [GPL-2.0 with CE](https://openjdk.java.net/legal/gplv2+ce.html) /R
* [sbt](https://www.scala-sbt.org/) licensed under [BSD-2-Clause](https://www.lightbend.com/legal/licenses) /D
* [sbt-api-mappings](https://github.com/ThoughtWorksInc/sbt-api-mappings) licensed under [Apache 2.0](https://github.com/ThoughtWorksInc/sbt-api-mappings/blob/3.0.x/LICENSE) /D
* [sbt-header](https://github.com/sbt/sbt-header) licensed under [Apache-2.0](https://github.com/sbt/sbt-header/blob/master/LICENSE) /D
* [sbt-scoverage](https://github.com/scoverage/sbt-scoverage) licensed under [Apache 2.0](https://github.com/scoverage/sbt-scoverage#license) /D
* [Scala](https://www.scala-lang.org/download/) licensed under [Apache 2.0](https://www.scala-lang.org/license/) /R
* [Scalafmt](https://scalameta.org/scalafmt/docs/installation.html#sbt) licensed under [Apache 2.0](https://github.com/scalameta/scalafmt/blob/master/LICENCE.md) /D
* [ScalaMeter](https://scalameter.github.io/) licensed under [BSD-3-Clause](https://scalameter.github.io/home/license/) /T
* [ScalaTest](http://www.scalatest.org/) licensed under [Apache 2.0](http://www.scalatest.org/about) /T
* [shapeless](https://github.com/milessabin/shapeless) licensed under [Apache 2.0](https://github.com/milessabin/shapeless/blob/master/LICENSE) /R

**/R** means runtime dependency, **/T** means test dependency, **/S** means source code derivative and **/D** means development tool.
For libraries, only direct dependencies are presented on above list.
