spata
=====

[![Build Status](https://travis-ci.org/fingo/spata.svg?branch=master)](https://travis-ci.org/fingo/spata)
[![Code Coverage](https://codecov.io/gh/fingo/spata/branch/master/graph/badge.svg)](https://codecov.io/gh/fingo/spata)
[![Maven Central](https://img.shields.io/maven-central/v/info.fingo/spata_2.13.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22info.fingo%22%20AND%20a:%22spata_2.13%22)
[![Scala Doc](https://javadoc.io/badge2/info.fingo/spata_2.13/javadoc.svg)](https://javadoc.io/doc/info.fingo/spata_2.13)

**spata** is a functional Scala parser for tabular data (`CSV`).
The library is based on [FS2 - Functional Streams for Scala](https://github.com/functional-streams-for-scala/fs2).

Main goal of the library is to provide handy, functional, stream-based API with easy conversion to case classes and
precise information about flaws in source data (their location) while keeping good performance.
Providing the location of the cause of a parsing error has been the main motivation to develop the library.
It is typically not that hard to parse well-formatted CSV file,
but it could be a nightmare to locate the source of problem in case of any distortion in large data file.   

The source data format is assumed basically to conform to [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt)
but allows some variations - see `CSVConfig` for details.

Getting started
---------------

spata is available for Scala 2.13 and requires at least Java 11.

To use spata you have to add this single dependency to your `build.sbt`:
```sbt
libraryDependencies += "info.fingo" %% "spata" % "<version>"
``` 
The latest version may be found on the badge above.

Link to the current API version is available through the badge as well. 

Basic usage
-----------
The whole parsing process in a simple case may look like this:
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

Alternatives
------------

For those, who needs different characteristic of a CSV library, there are a few alternatives available for Scala:
* [Itto-CSV](https://github.com/gekomad/itto-csv) - CSV handling library based on FS2 and Cats with support for case class conversion.
* [fs2  data](https://github.com/satabin/fs2-data) - collection of FS2 based parsers, including CSV.
* [kantan.csv](https://github.com/nrinaudo/kantan.csv) - well documented CSV parser/serializer with support for different parsing engines.
* [scala-csv](https://github.com/tototoshi/scala-csv) - easy to use CSV reader/writer.

Credits
-------

**spata** makes use of following tools, languages, frameworks, libraries and data sets (in alphabetical order):
* [Cats Effect](https://typelevel.org/cats-effect/) licensed under [Apache 2.0](https://github.com/typelevel/cats-effect/blob/master/LICENSE.txt) /C
* [Codecov](https://codecov.io/) available under following [Terms of Use](https://codecov.io/terms) /D
* [FS2](https://fs2.io/) licensed under [MIT](https://github.com/functional-streams-for-scala/fs2/blob/master/LICENSE) /C
* [Git](https://git-scm.com/) licensed under [GPL-2.0](https://git-scm.com/about/free-and-open-source) /D
* [GitHub](https://github.com/) available under following [Terms of Service](https://help.github.com/en/github/site-policy/github-terms-of-service) /D
* [Gitter](https://gitter.im/) available under following [Terms of Use](https://about.gitlab.com/terms/) /D
* [http4s](https://http4s.org/) licensed under [Apache 2.0](https://github.com/http4s/http4s#license) /S
* [IntelliJ IDEA CE](https://www.jetbrains.com/idea/) licensed under [Apache 2.0](https://www.jetbrains.com/idea/download/) /D
* [javadoc.io](https://www.javadoc.io/) licensed under [apache  2.0](https://github.com/maxcellent/javadoc.io/blob/master/LICENSE) /D
* [Mars weather data](https://github.com/the-pudding/data/tree/master/mars-weather) made publicly available by [NASA](https://pds.nasa.gov/) and [CAB](https://cab.inta-csic.es/rems/en) /T
* [OpenJDK](https://adoptopenjdk.net/) licensed under [GPL-2.0 with CE](https://openjdk.java.net/legal/gplv2+ce.html) /C
* [sbt](https://www.scala-sbt.org/) licensed under [BSD-2-Clause](https://www.lightbend.com/legal/licenses) /D
* [sbt-api-mappings](https://github.com/ThoughtWorksInc/sbt-api-mappings) licensed under [Apache 2.0](https://github.com/ThoughtWorksInc/sbt-api-mappings/blob/3.0.x/LICENSE) /D
* [sbt-dynver](https://github.com/dwijnand/sbt-dynver) licensed under [Apache 2.0](https://github.com/dwijnand/sbt-dynver/blob/master/LICENSE) /D
* [sbt-header](https://github.com/sbt/sbt-header) licensed under [Apache-2.0](https://github.com/sbt/sbt-header/blob/master/LICENSE) /D
* [sbt-pgp](https://github.com/sbt/sbt-pgp) licensed under [BSD-3-Clause](https://github.com/sbt/sbt-pgp/blob/master/LICENSE) /D
* [sbt-scoverage](https://github.com/scoverage/sbt-scoverage) licensed under [Apache 2.0](https://github.com/scoverage/sbt-scoverage#license) /D
* [sbt-sonatype](https://github.com/xerial/sbt-sonatype) licensed under [Apache 2.0](https://github.com/xerial/sbt-sonatype/blob/master/LICENSE.txt) /D
* [Scala](https://www.scala-lang.org/download/) licensed under [Apache 2.0](https://www.scala-lang.org/license/) /C
* [Scalafmt](https://scalameta.org/scalafmt/docs/installation.html#sbt) licensed under [Apache 2.0](https://github.com/scalameta/scalafmt/blob/master/LICENCE.md) /D
* [ScalaMeter](https://scalameter.github.io/) licensed under [BSD-3-Clause](https://scalameter.github.io/home/license/) /T
* [ScalaTest](http://www.scalatest.org/) licensed under [Apache 2.0](http://www.scalatest.org/about) /T
* [shapeless](https://github.com/milessabin/shapeless) licensed under [Apache 2.0](https://github.com/milessabin/shapeless/blob/master/LICENSE) /C
* [sonatype OSSRH](https://central.sonatype.org/) available under following [Terms of Service](https://central.sonatype.org/pages/central-repository-producer-terms.html) /D
* [Travis CI](https://travis-ci.org/) available under following [Terms of Service](https://docs.travis-ci.com/legal/terms-of-service/) /D

**/C** means compile/runtime dependency, **/T** means test dependency, **/S** means source code derivative and **/D** means development tool.
Only direct dependencies are presented on above list.
