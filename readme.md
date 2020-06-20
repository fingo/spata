spata
=====

[![Build Status](https://travis-ci.org/fingo/spata.svg?branch=master)](https://travis-ci.org/fingo/spata)
[![Code Coverage](https://codecov.io/gh/fingo/spata/branch/master/graph/badge.svg)](https://codecov.io/gh/fingo/spata)
[![Maven Central](https://img.shields.io/maven-central/v/info.fingo/spata_2.13.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22info.fingo%22%20AND%20a:%22spata_2.13%22)
[![Scala Doc](https://javadoc.io/badge2/info.fingo/spata_2.13/javadoc.svg)](https://javadoc.io/doc/info.fingo/spata_2.13)
[![Gitter](https://badges.gitter.im/fingo-spata/community.svg)](https://gitter.im/fingo-spata/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

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

Another example may be taken from [FS2 readme](https://fs2.io/) assuming,
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

More examples how to use the library may be found in `src/test/scala/info/fingo/spata/sample`.

Tutorial
--------

### Parsing

Core spata operation is a transformation from a stream of characters into a stream of `CSVRecords`.
This is available through `CSVParser.parse` function (supplying FS2 `Pipe`)
and is probably the best way to include CSV parsing into any FS2 stream processing pipeline:
```scala
val input: Stream[IO, Char] = ???
val parser: CSVParser = CSVParser()
val output: Stream[IO, CSVRecord] = input.through(parser.parse)
```
Like in case of any other FS2 processing, spata consumes only as much of the source stream as required. 

Field and record delimiters are required to be single characters.
There are however no other assumptions about them - particularly the record delimiter does not have to be a line break
and spata does not assume line break presence in the source data - it does not read the data by lines.

If newline (`LF`, `\n`, `0x0A`) is however used as the record delimiter,
carriage return character (`CR`, `\r`, `0x0D`) is automatically skipped if not escaped, to support `CRLF` line breaks.     

Fields containing delimiters (field or record) or quotes have to be wrapped in quotation marks.
As defined in RFC 4180, quotation marks in content have to be escaped through double quotation marks.

In addition to `parse`, `CSVParser` provides other methods to read CSV data:
* `get` to load data into `List[CSVRecord]`, which may be handy for small data sets,
* `process` to deal with record by record through a callback function,
* `async` to process data through a callback function in asynchronous way.

The three above functions return the result (`List` or `Unit`) wrapped in an effect (`IO`)
and require calling one of the unsafe functions (`unsafeRunSync` or `unsafeRunAsync`) to trigger computation.
```scala
val stream: Stream[IO, Char] = ???
val parser: CSVParser = CSVParser()
val list: List[CSVRecord] = parser.get(stream).unsafeRunSync()
```
Alternatively, instead of calling an unsafe function,
whole processing may run through [IOApp](https://typelevel.org/cats-effect/datatypes/ioapp.html).

If we have to work with a stream of `String` (e.g. from FS2 `text.utf8Decode`)
we may easily convert it to a character stream:
```scala
val ss: Stream[IO, String] = ???
val sc: Stream[IO, Char] = ss.map(s => Chunk.chars(s.toCharArray)).flatMap(Stream.chunk)
```

See [Reading source data](#reading-source-data) for helper methods to get stream of characters from various sources.

### Configuration

`CSVParser` is configured through `CSVConfig` being parameter to its constructor.
A more convenient way may be a builder-like method, which takes the defaults provided by `CSVParser` object
and allows altering selected parameters:
```scala
val parser = CSVReader.config.fieldDelimiter(';').noHeader().get
```

Individual configuration parameters are described in
`CSVConfig`'s [Scaladoc](https://javadoc.io/doc/info.fingo/spata_2.13/latest/info/fingo/spata/CSVConfig.html).

A specific setting is the header mapping, available through `CSVConfig.mapHeader`.
It allows replacement of original header values with more convenient ones or even defining header if no one is present.
The new values are then used in any operations referencing individual fields,
including automatic conversion to case classes or tuples.
Mapping may be defined only for a subset of fields, leaving the rest in original form.
```csv
date,max temparature,min temparature
2020-02-02,13.7,-2.2
```
```scala
val stream: Stream[IO, Char] = ???
val parser: CSVParser = CSVParser.config.mapHeader(Map("max temparature" -> "tempMax", "min temparature" -> "tempMin")).get
val frosty: Stream[IO, Char] = stream.through(parser.parse).filter(_.get[Double]("minTemp") < 0)
```
It may be defined as well for more fields than present in any particular data source,
which allows using single parser for multiple data sets, with different headers.

FS2 takes care of limiting the amount of processed data and consumed memory to the required level.
This works well to restrict number of records, however each record has to be fully loaded into memory,
no matter how large it is.
This is not a  problem if everything goes well - individual records are not that large.
A record can however grow uncontrollably in case of incorrect configuration (e.g. wrong record delimiter)
or malformed structure (e.g. unclosed quotation).
To prevent `OutOfMemoryError` in such situations,
spata can be configured to limit maximum size of single field using `fieldSizeLimit`.
If this limit is exceeded during parsing, the processing stops with error. 
There is no limit by default.

### Reading source data

As mentioned earlier, `CSVParser` requires stream of characters as its input.
To simplify working with common data sources, like files or sockets, spata provides a few convenience method,
available through its `io.reader` object.

There are two groups of `read` methods in `reader`:
* basic ones, placed directly in `reader` object, where reading is done synchronously on the current thread,
* with support for [thread shifting](https://typelevel.org/cats-effect/datatypes/io.html#thread-shifting),
accessible through `reader.withBlocker`.

It is recommended to use the thread shifting version, especially for long reading operation,
for better thread pools utilization.
See [a post from Daniel Spiewak](https://gist.github.com/djspiewak/46b543800958cf61af6efa8e072bfd5c)
about thread pools configuration.
Caution has to be taken however in case of thread shifting version of `read` method using Scala `Source`.
Due to the `Source` being character-based iterator, the context switch in made for each character,
which can be an order of magnitude slower than its regular, non-shifting counterpart - at least for common data sources.

More information about threading may be found in
[Cats Concurrency Basics](https://typelevel.org/cats-effect/concurrency/basics.html).

The simplest way to read a file is:
```scala
val stream: Stream[IO, Char] = reader.read(Path.of("data.csv"))
```
The thread shifting reader provides similar method, by requires implicit `ContextShift`:
```scala
implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
val stream: Stream[IO, Char] = reader.withBlocker.read(Path.of("data.csv"))
```
The `ExecutionContext` provided to `ContextShift` is used to switch the context back to the CPU-bound one,
used for regular, non-blocking operation, after the blocking IO operation finishes.
The `Blocker`, which provides thread pool for blocking I/O,
may be passed to `withBlocker` or will be created internally. 

Except of `Source`, which is already a character-based, other data sources require an implicit `Codec`
to convert bytes into characters:
```scala
implicit val codec: Codec = Codec.UTF8
```

The caller to a `read` function which takes a resource as parameter (`Source` or `InputStream`)
is responsible for resource cleanup. This may be achieved through FS2 `Stream.bracket`:
```scala
val stream: Stream[IO, Char] = for {
   source <- Stream.bracket(IO { Source.fromFile("data.csv") })(source => IO { source.close() })
   char <- reader.withBlocker.read(source)
} yield char
```
Other methods of resource acquisition and releasing are described in
[Cats Effect tutorial](https://typelevel.org/cats-effect/tutorial/tutorial.html#acquiring-and-releasing-resources).

In addition to the `read` function, `reader` provides a `by` function, to be used with `Stream.through`.
The above example may be rewritten using it to:
```scala
val stream: Stream[IO, Char] = Stream
  .bracket(IO { Source.fromFile("data.csv") })(source => IO { source.close() })
  .through(reader.withBlocker.by)
```

### Getting actual data

Sole CSV parsing operation produces a stream of `CSVRecord`s.
Each record may be seen as a map from `String` to `String`, where the keys are shared among all records.
The basic method to obtain individual values is through the `apply` function, by key (taken from header):
```scala
val record: CSVRecord = ???
val value: String = record("key")
```
or index:
```scala
val record: CSVRecord = ???
val value: String = record(0)
```

`CSVRecord` supports retrieval of typed values.
In simple cases, when the value is serialized in its canonical form,
which does not require additional format information, like ISO format for dates,
this may be done with single-parameter `get` or `seek` functions:
```scala
val record: CSVRecord = ???
val num: Double = record.get[Double]("123.45")
val numM: Maybe[Double] = record.seek[Double]("123.45")
```
`seek` is a safe version of `get` - it returns the result wrapped in `Maybe[A]`,
which is an alias for `Either[Throwable, A]`.
`get` may throw `CSVDataException`.
Both functions require a `text.StringParser[A]`, which is described in [next chapter](#text-parsing).

Both, `get` and `seek` have overloaded versions which support formatting-aware parser:
```scala
val record: CSVRecord = ???
val df = new DecimalFormat("#,###")
val num: Double = record.get[Double]("123,45", df)
val numM: Maybe[Double] = record.seek[Double]("123,45", df)
```
Both functions require a `text.FormattedStringParser[A, B]`, which is described in [next chapter](#text-parsing).
(They use an intermediary classes `Field` and `SafeField` to provide nice syntax,
this should be however transparent in most cases).

In addition to retrieval of single fields, `CSVRecord` may be converted to a case class or tuple.
Assuming CSV data in following form:
```csv
element,symnol,melting,boiling
hydrogen,H,13.99,20.271
helium,He,0.95,4.222
lithium,Li,453.65,1603
```
the data can be converted from a record directly to a case class:
```scala
val record: CSVRecord = ???
case class Element(symbol: String, melting: Double, boiling: Double)
val element: Maybe[Element] = record.to[Element]()
```
Notice that not all source fields have to be used for conversion.
The conversion is name-based - header strings have to match exactly case class field names, including case.
We can use header mapping, described in [Configuration](#configuration), if they do not match.

For tuples, the header has to match tuple field names (`_1`, `_2`, etc.)
and is automatically generated in this form for data without header:
```csv
hydrogen,H,13.99,20.271
helium,He,0.95,4.222
lithium,Li,453.65,1603
```
```scala
val record: CSVRecord = ???
type Element = (String, String, Double, Double)
val element: Maybe[Element] = record.to[Element]()
```
Notice that in this case the first column has been included in conversion to ensure header and tuple field matching.

Both forms of conversion require implicit `StringParser`.
Parsers for common types and their default formats are provided through `StringParser` object
and are automatically brought in scope.
Because it is not possible to explicitly provide custom formatter while converting record to a case class,
an implicit `StringParser` has to be defined in case of specific formats or types:
```csv
element,symnol,melting,boiling
hydrogen,H,"13,99","20,271"
helium,He,"0,95","4,222"
lithium,Li,"453,65","1603"
```
```scala
val record: CSVRecord = ???
case class Element(symbol: String, melting: Double, boiling: Double)
val nf = NumberFormat.getInstance(new Locale("pl", "PL"))
implicit val nsp: StringParser[Double] = (str: String) => nf.parse(str).doubleValue()
val element: Maybe[Element] = record.to[Element]()
```

### Text parsing

CSV data is parsed as `String`s.
We often need typed values, e.g. numbers or dates, for further processing.
There is no standard, uniform interface available for Scala nor Java to parse strings to different types.
Numbers may be parsed using `java.text.NumberFormat`.
Dates and times through `parse` methods in `java.time.LocalDate` or `LocalTime`, providing format as parameter.
This is awkward when providing single interface for various types, like `CSVRecord` does.
This is the place where spata's `text.StringParser` comes in handy.

`StringParser` object provides methods for parsing strings with default format:
```scala
val num: Double = StringParser.parse[Double]("123.45")
val numM: Maybe[Double] = StringParser.parseSafe[Double]("123.45")
```
`parse` may throw `DataParseException` while `parseSafe` wraps the result in `Either[Throwable, A]`

When specific format has to be provided, overloaded version of above methods are available: 
```scala
val df = new DecimalFormat("#,###")
val num: Double = StringParser.parse[Double]("123,45", df)
val numM: Maybe[Double] = StringParser.parseSafe[Double]("123,45", df)
```
(They use intermediary classes `Pattern` and `SafePattern` to provide nice syntax,
this should be however transparent in most cases).

These functions require implicit `StringParser` or `FormattedStringParser` respectively.
Implicits for a few basic types are already available - see Scaladoc for `StringParser`.
When additional parsers are required,
they may be easily provided by implementing `StringParser` or `FormattedStringParser` traits.

Let's take `java.sql.Date` as an example. Having implemented `StringParser[Date]`:
```scala
import java.sql.Date
import info.fingo.spata.text.StringParser
implicit val sdf: StringParser[Date] = (s: String) => Date.valueOf(s)
```
we can use it as follows:
```scala
val date = StringParser.parse[Date]("2020-02-02")
val dateM = StringParser.parseSafe[Date]("2020-02-02")
```

Defining parser with support for custom formatting requires implementation of `FormattedStringParser`:
```scala
import java.sql.Date
import java.text.DateFormat
import info.fingo.spata.text.FormattedStringParser

implicit val sdf: FormattedStringParser[Date, DateFormat] =
  new FormattedStringParser[Date, DateFormat] {
      override def parse(str: String): Date = Date.valueOf(str.strip)
      override def parse(str: String, fmt: DateFormat): Date =  new Date(fmt.parse(str.strip).getTime)
  }
```
and can be used as follows:
```scala
import info.fingo.spata.text.StringParser
import java.util.Locale

val df = DateFormat.getDateInstance(DateFormat.SHORT, new Locale("pl", "PL"))
val date = StringParser.parse[Date]("02.02.2020", df)
val dateM = StringParser.parseSafe[Date]("02.02.2020", df)
```
Please notice that this sample implementation accepts partial string parsing,
e.g. `"02.02.2020xyz"` will successfully parse to `2020-02-02`.
This is different from the built-in parsing behaviour for `LocalDate`,
where the entire string has to conform to the format. 

Parsing implementation are expected to throw specific runtime exceptions when parsing fails.
This is converted to `DataParseException` in `StringParser` object's `parse` method,
while keeping original exception in `cause` field.

### Error handling

There are three types of errors which may arise while parsing CSV:
* Various I/O errors, including but not limited to `IOException`.
They are not directly related to parsing logic but CSV is typically read from some external, unreliable source.
They may be raised by `reader` operations.
* Errors caused by malformed CSV structure, reported as `CSVStructureException`.
They may be caused by `CSVParser`'s methods.
* Errors caused by unexpected / incorrect data in record fields, reported as `CSVDataException`.
They may result from interaction with `CVSRecord`.

The two first error categories are unrecoverable and stop stream processing.
For the `CSVStructureException` errors we are able to precisely identify the place which caused the problem.
See Scaladoc for `CSVException` for further information about error location.

The last category in reported on the record level and allows for different handling policies.
Please notice however, that if the error is not handled locally (using safe functions returning `Maybe`)
and propagates through the stream, further processing of input data is stopped, like for above error categories.  

Errors are raised and should be handled with [FS2 error handling](https://fs2.io/guide.html#error-handling) mechanism.
FS2 captures exceptions reported explicitly with `raiseError` or thrown
and in both cases is able to handle them with `handleErrorWith`.

The converter example presented in [Basic usage](#basic-usage) may be enriched with explicit error handling:
```scala
import java.nio.file.Paths
import scala.io.Codec
import scala.util.Try
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import fs2.Stream
import fs2.io
import fs2.text
import info.fingo.spata.CSVParser
import info.fingo.spata.io.reader

object Converter extends IOApp {

  val converter: Stream[IO, ExitCode] = Stream.resource(Blocker[IO]).flatMap {
    blocker =>
      def fahrenheitToCelsius(f: Double): Double =
        (f - 32.0) * (5.0 / 9.0)
      val parser: CSVParser = CSVParser.config.get
      implicit val codec: Codec = Codec.UTF8
      val src = Paths.get("testdata/fahrenheit.txt")
      val dst = Paths.get("testdata/celsius.txt")

      reader
        .withBlocker(blocker)
        .read(src)
        .through(parser.parse)
        .filter(r => !r("temp").isEmpty)
        .map { r =>
          val date = r("date")
          val temp = fahrenheitToCelsius(r.get[Double]("temp"))
          s"$date,$temp"
        }
        .intersperse("\n")
        .through(text.utf8Encode)
        .through(io.file.writeAll(dst, blocker))
        .fold(ExitCode.Success)((z, _) => z)
        .handleErrorWith(ex => {
          println(ex)
          Try(dst.toFile.delete())
          Stream.eval(IO(ExitCode.Error))
        })
  }

  def run(args: List[String]): IO[ExitCode] =
    converter.compile.lastOrError
}
```

If some operations return `Either` (e.g. when `r.seek` would be used instead of `r.get` in above code)
and we would like to handle errors wrapped in `Left` together with raised ones, we may call `rethrow` on the stream. 

Sometimes we would like to convert stream to a collection.
We should wrap result in `Either` in such situations to distinguish successful processing from erroneous one.
See first code snippet in [Basic usage](#basic-usage) for sample.

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
