spata
=====

[![Build Status](https://travis-ci.com/fingo/spata.svg?branch=master)](https://travis-ci.com/fingo/spata)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/21674eb21b7645edb9b149dfcbcb628d)](https://www.codacy.com/gh/fingo/spata/dashboard)
[![Code Coverage](https://codecov.io/gh/fingo/spata/branch/master/graph/badge.svg)](https://codecov.io/gh/fingo/spata)
[![Maven Central](https://img.shields.io/maven-central/v/info.fingo/spata_2.13.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22info.fingo%22%20AND%20a:%22spata_2.13%22)
[![Scala Doc](https://javadoc.io/badge2/info.fingo/spata_2.13/javadoc.svg)](https://javadoc.io/doc/info.fingo/spata_2.13)
[![Gitter](https://badges.gitter.im/fingo-spata/community.svg)](https://gitter.im/fingo-spata/community)

**spata** is a functional Scala parser for tabular data (`CSV`).
The library is backed by [FS2 - Functional Streams for Scala](https://github.com/functional-streams-for-scala/fs2).

The main goal of the library is to provide handy, functional, stream-based API
with easy conversion of records to case classes and precise information about possible flaws
and their location in source data while maintaining good performance.
Providing the location of the cause of a parsing error has been the main motivation to develop the library.
It is typically not that hard to parse a well-formatted CSV file,
but it could be a nightmare to locate the source of a problem in case of any distortions in a large data file.   

The source data format is assumed to basically conform to [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt),
but allows some variations - see `CSVConfig` for details.

*   [Getting started](#getting-started)
*   [Basic usage](#basic-usage)
*   [Tutorial](#tutorial)
*   [Alternatives](#alternatives)
*   [Credits](#credits)

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
val parser = CSVParser[IO]() // parser with default configuration
val records = Stream
  // get stream of CSV records while ensuring source cleanup
  .bracket(IO { Source.fromFile("input.csv") })(source => IO { source.close() })
  .through(reader[IO]().by) // produce stream of chars from source
  .through(parser.parse)  // parse CSV file and get CSV records 
  .filter(_.get[Double]("value").exists(_ > 1000))  // do some operations using Stream API
  .map(_.to[Data]()) // convert records to case class
  .handleErrorWith(ex => Stream.eval(IO(Left(ex)))) // convert global (I/O, CSV structure) errors to Either
val result = records.compile.toList.unsafeRunSync // run everything while converting result to list
```

Another example may be taken from [FS2 readme](https://fs2.io/),
assuming that the data is stored in CSV format with two fields, `date` and `temp`:
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
      val parser: CSVParser[IO] = CSVParser.config.get[IO]()
      def fahrenheitToCelsius(f: Double): Double =
        (f - 32.0) * (5.0 / 9.0)

      reader
        .shifting[IO](blocker)
        .read(Paths.get("testdata/fahrenheit.txt"))
        .through(parser.parse)
        .filter(r => r("temp").exists(!_.isBlank)
        .map { r =>
          val date = r.unsafe("date")
          val temp = fahrenheitToCelsius(r.unsafe.get[Double]("temp"))
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
(This example uses exception throwing methods for brevity and to keep it closer to original snippet.
A modified version with safe access to record data may be found in [error handling](#error-handling) part of tutorial.)

More examples of how to use the library may be found in `src/test/scala/info/fingo/spata/sample`.

Tutorial
--------

*   [Parsing](#parsing)
*   [Configuration](#configuration)
*   [Reading source data](#reading-source-data)
*   [Getting actual data](#getting-actual-data)
*   [Text parsing](#text-parsing)
*   [Schema validation](#schema-validation)
*   [Error handling](#error-handling)
*   [Logging](#logging)

### Parsing

Core spata operation is a transformation from a stream of characters into a stream of `Record`s.
This is available through `CSVParser.parse` function (supplying FS2 `Pipe`)
and is probably the best way to include CSV parsing into any FS2 stream processing pipeline:
```scala
val input: Stream[IO, Char] = ???
val parser: CSVParser[IO] = CSVParser[IO]()
val output: Stream[IO, Record] = input.through(parser.parse)
```
In accordance with FS2, spata is polymorphic in the effect type and may be used with different effect implementations
(Cats [IO](https://typelevel.org/cats-effect/datatypes/io.html),
Monix [Task](https://monix.io/docs/3x/eval/task.html)
or ZIO [ZIO](https://zio.dev/docs/datatypes/datatypes_io)).
Please note however, that Cats Effect `IO` is the only effect implementation used for testing and documentation purposes. 
Type class dependencies are defined in terms of [Cats Effect](https://typelevel.org/cats-effect/typeclasses/) class hierarchy.
To support effect suspension, spata requires in general `cats.effect.Sync` type class implementation for its effect type.
Some methods need enhanced type classes to support asynchronous or concurrent computation.

Like in case of any other FS2 processing, spata consumes only as much of the source stream as required,
give or take a chunk size. 

Field and record delimiters are required to be single characters.
There are however no other assumptions about them - particularly the record delimiter does not have to be a line break
and spata does not assume line break presence in the source data - it does not read the data by lines.

If newline (`LF`, `\n`, `0x0A`) is however used as the record delimiter,
carriage return character (`CR`, `\r`, `0x0D`) is automatically skipped if not escaped, to support `CRLF` line breaks.     

Fields containing delimiters (field or record) or quotes have to be wrapped in quotation marks.
As defined in RFC 4180, quotation marks in content have to be escaped through double quotation marks.

By default, in accordance with standard, whitespace characters are considered part of the field and are not ignored.
Nonetheless, it is possible to turn on trimming of leading and trailing whitespaces with a configuration option.
This differs from stripping whitespaces from resulting field content,
because it distinguishes between quoted and unquoted spaces. For example, having following input:
```csv
X,Y,Z
xxx," yyy ",zzz
xxx, yyy ,zzz
```
without trimming the content of `Y` field will be `" yyy "` for both records.
With trimming on, we get `" yyy "` for the first record and `"yyy"` for the second.

Please also note, that following content: 
```csv
X,Y,Z
xxx, " yyy " ,zzz
```
is correct with trimming on (and produces `" yyy "` for field `Y`), but will cause an error without it,
as spaces are considered regular characters in this case and quote has to be put around the whole field.

Not all invisible characters (notably non-breaking space, `'\u00A0'`) are whitespaces.
See Java `Char.isWhitespace` for details.  

In addition to `parse`, `CSVParser` provides other methods to read CSV data:
*   `get` to load data into `List[Record]`, which may be handy for small data sets,
*   `process` to deal with data record by record through a callback function,
*   `async` to process data through a callback function in asynchronous way.

The three above functions return the result (`List` or `Unit`) wrapped in an effect and require calling one of the
"at the end of the world" methods (`unsafeRunSync` or `unsafeRunAsync` for `cats.effect.IO`) to trigger computation.
```scala
val stream: Stream[IO, Char] = ???
val parser: CSVParser[IO] = CSVParser[IO]()
val list: List[Record] = parser.get(stream).unsafeRunSync()
```
Alternatively, instead of calling an unsafe function,
whole processing may run through [IOApp](https://typelevel.org/cats-effect/datatypes/ioapp.html).

If we have to work with a stream of `String` (e.g. from FS2 `text.utf8Decode`) we may convert it to a character stream:
```scala
val ss: Stream[IO, String] = ???
val sc: Stream[IO, Char] = ss.map(s => Chunk.chars(s.toCharArray)).flatMap(Stream.chunk)
```

See [Reading source data](#reading-source-data) for helper methods to get stream of characters from various sources.

### Configuration

`CSVParser` is configured through `CSVConfig`, which is a parameter to its constructor.
A more convenient way may be a builder-like method, which takes the defaults provided by `CSVParser` object
and allows altering selected parameters:
```scala
val parser = CSVReader.config.fieldDelimiter(';').noHeader().get[IO]()
```

Individual configuration parameters are described in
`CSVConfig`'s [Scaladoc](https://javadoc.io/doc/info.fingo/spata_2.13/latest/info/fingo/spata/CSVConfig.html).

A specific setting is the header mapping, available through `CSVConfig.mapHeader`.
It allows replacement of original header values with more convenient ones or even defining header if no one is present.
The new values are then used in all operations referencing individual fields,
including automatic conversion to case classes or tuples.
Mapping may be defined only for a subset of fields, leaving the rest in their original form.
```csv
date,max temparature,min temparature
2020-02-02,13.7,-2.2
```
```scala
val stream: Stream[IO, Char] = ???
val parser: CSVParser[IO] =
  CSVParser.config.mapHeader(Map("max temparature" -> "tempMax", "min temparature" -> "tempMin")).get[IO]()
val frosty: Stream[IO, Char] = stream.through(parser.parse).filter(_.get[Double]("minTemp").exists(_ < 0))
```
It may also be defined for more fields than there are present in any particular data source,
which allows using a single parser for multiple data sets with different headers.

There is also index-based header mapping available. It may be used not only to define / redefine header,
but to remove duplicates as well:
```csv
date,temparature,temparature
2020-02-02,13.7,-2.2
```
```scala
val stream: Stream[IO, Char] = ???
val parser: CSVParser[IO] =
  CSVParser.config.mapHeader(Map(1 -> "tempMax", 2 -> "tempMin")).get[IO]()
val frosty: Stream[IO, Char] = stream.through(parser.parse).filter(_.get[Double]("minTemp").exists(_ < 0))
```

FS2 takes care of limiting the amount of processed data and consumed memory to the required level.
This works well to restrict the number of records, however each record has to be fully loaded into memory,
no matter how large it is.
This is not a  problem if everything goes well - individual records are typically not that large.
A record can however grow uncontrollably in case of incorrect configuration (e.g. wrong record delimiter)
or malformed structure (e.g. unclosed quotation).
To prevent `OutOfMemoryError` in such situations,
spata can be configured to limit the maximum size of a single field using `fieldSizeLimit`.
If this limit is exceeded during parsing, the processing stops with an error. 
By default, no limit is specified.

### Reading source data

As mentioned earlier, `CSVParser` requires a stream of characters as its input.
To simplify working with common data sources, like files or sockets, spata provides a few convenience methods,
available through its `io.reader` object.

There are two groups of `read` methods in `reader`:

*   basic ones, accessible through `reader.plain`, where reading is done synchronously on the current thread,

*   with support for [thread shifting](https://typelevel.org/cats-effect/datatypes/io.html#thread-shifting),
    accessible through `reader.shifting`.

It is recommended to use the thread shifting version, especially for long reading operation,
for better thread pools utilization.
See [a post from Daniel Spiewak](https://gist.github.com/djspiewak/46b543800958cf61af6efa8e072bfd5c)
about thread pools configuration.
More information about threading may be found in
[Cats Concurrency Basics](https://typelevel.org/cats-effect/concurrency/basics.html).

The simplest way to read a file is:
```scala
val stream: Stream[IO, Char] = reader.plain[IO]().read(Path.of("data.csv"))
```
or even:
```scala
val stream: Stream[IO, Char] = reader[IO]().read(Path.of("data.csv")) // reader.apply is an alias for reader.plain
```
The thread shifting reader provides a similar method, but requires implicit `ContextShift`:
```scala
implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
val stream: Stream[IO, Char] = reader.shifting[IO]().read(Path.of("data.csv"))
```
The `ExecutionContext` provided to `ContextShift` is used to switch the context back to the CPU-bound one,
used for regular, non-blocking operation, after the blocking I/O operation finishes.
The `Blocker`, which provides the thread pool for blocking I/O, may be passed to `shifting` or will be created internally.

All `read` operations load data in [chunks](https://fs2.io/guide.html#chunks) for better performance.
Chunk size may be supplied while creating a reader:
```scala
val stream: Stream[IO, Char] = reader.plain[IO](1024).read(Path.of("data.csv"))
```
If not provided explicitly, a default chunk size will be used.

Except for `Source`, which is already character-based, other data sources require an implicit `Codec`
to convert bytes into characters:
```scala
implicit val codec: Codec = Codec.UTF8
```

The caller to a `read` function which takes a resource as parameter (`Source` or `InputStream`)
is responsible for resource cleanup. This may be achieved through FS2 `Stream.bracket`:
```scala
val stream: Stream[IO, Char] = for {
   source <- Stream.bracket(IO { Source.fromFile("data.csv") })(source => IO { source.close() })
   char <- reader.shifting[IO]().read(source)
} yield char
```
Other methods of resource acquisition and releasing are described in
[Cats Effect tutorial](https://typelevel.org/cats-effect/tutorial/tutorial.html#acquiring-and-releasing-resources).

In addition to the `read` function, `reader` provides a `by` function, to be used with `Stream.through`.
The above example may be rewritten by using `by` into:
```scala
val stream: Stream[IO, Char] = Stream
  .bracket(IO { Source.fromFile("data.csv") })(source => IO { source.close() })
  .through(reader.shifting[IO]().by)
```

### Getting actual data

Sole CSV parsing operation produces a stream of `Record`s.
Each record may be seen as a map from `String` to `String`, where the keys are shared among all records.
The basic method to obtain individual values is through the `apply` function, by key (taken from header):
```scala
val record: Record = ???
val value: Option[String] = record("key")
```
or index:
```scala
val record: Record = ???
val value: Option[String] = record(0)
```

`CSVRecord` supports retrieval of typed values.
In simple cases, when the value is serialized in its canonical form,
which does not require any additional format information, like ISO format for dates,
this may be done with single-parameter `get` function:
```scala
val record: Record = ???
val num: Decoded[Double] = record.get[Double]("123.45")
```
`Decoded[A]` is an alias for `Either[ContentError, A]`.
This method requires a `text.StringParser[A]`, which is described in [the next chapter](#text-parsing).

`get` has sn overloaded versions, which support formatting-aware parsing:
```scala
val record: Record = ???
val df = new DecimalFormat("#,###")
val num: Decoded[Double] = record.get[Double]("123,45", df)
```
This methods requires a `text.FormattedStringParser[A, B]`, which is described in [the next chapter](#text-parsing).
(It uses an intermediary class `Field` to provide a nice syntax, this should be however transparent in most cases).

Above methods are available also in unsafe, exception-throwing version, accessible through `Record.unsafe` object:
```scala
val record: Record = ???
val v1: String = record.unsafe("key")
val v2: String = record.unsafe(0)
val n1: Double = record.unsafe.get[Double]("123.45")
val df = new DecimalFormat("#,###")
val n2: Double = record.unsafe.get[Double]("123,45", df)
```
They may throw `ContentError` exception.

In addition to retrieval of single fields, `Record` may be converted to a case class or a tuple.
Assuming CSV data in the following form:
```csv
element,symnol,melting,boiling
hydrogen,H,13.99,20.271
helium,He,0.95,4.222
lithium,Li,453.65,1603
```
The data can be converted from a record directly into a case class:
```scala
val record: Record = ???
case class Element(symbol: String, melting: Double, boiling: Double)
val element: Decoded[Element] = record.to[Element]()
```
Notice that not all source fields have to be used for conversion.
The conversion is name-based - header strings have to match case class field names exactly, including case.
We can use header mapping, described in [Configuration](#configuration), if they do not match.

For tuples, the header has to match tuple field names (`_1`, `_2`, etc.)
and is automatically generated in this form for data without a header:
```csv
hydrogen,H,13.99,20.271
helium,He,0.95,4.222
lithium,Li,453.65,1603
```
```scala
val record: Record = ???
type Element = (String, String, Double, Double)
val element: Decoded[Element] = record.to[Element]()
```
Notice that in this case the first column has been included in the conversion to ensure header and tuple field matching.

Both forms of conversion require implicit `StringParser`.
Parsers for common types and their default formats are provided through `StringParser` object
and are automatically brought in scope.
Because it is not possible to explicitly provide custom formatter while converting a record into a case class,
an implicit `StringParser` has to be defined in case of specific formats or types:
```csv
element,symnol,melting,boiling
hydrogen,H,"13,99","20,271"
helium,He,"0,95","4,222"
lithium,Li,"453,65","1603"
```
```scala
val record: Record = ???
case class Element(symbol: String, melting: Double, boiling: Double)
val nf = NumberFormat.getInstance(new Locale("pl", "PL"))
implicit val nsp: StringParser[Double] = (str: String) => nf.parse(str).doubleValue()
val element: Decoded[Element] = record.to[Element]()
```

### Text parsing

CSV data is parsed as `String`s.
We often need typed values, e.g. numbers or dates, for further processing.
There is no standard, uniform interface available for Scala or Java to parse strings to different types.
Numbers may be parsed using `java.text.NumberFormat`.
Dates and times through `parse` methods in `java.time.LocalDate` or `LocalTime`, providing format as parameter.
This is awkward when providing single interface for various types, like `Record` does.
This is the place where spata's `text.StringParser` comes in handy.

`StringParser` object provides methods for parsing strings with default format:
```scala
val num: ParseResult[Double] = StringParser.parse[Double]("123.45")
```
where `ParseResult[A]` is just an alias for `Either[ParseError, A]`.

When a specific format has to be provided, an overloaded version of above method is available: 
```scala
val df = new DecimalFormat("#,###")
val num: ParseResult[Double] = StringParser.parse[Double]("123,45", df)
```
(They use intermediary classes `Pattern` to provide nice syntax,
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
We can use it as follows:
```scala
val date = StringParser.parse[Date]("2020-02-02")
```

Defining a parser with support for custom formatting requires the implementation of `FormattedStringParser`:
```scala
import java.sql.Date
import java.text.DateFormat
import info.fingo.spata.text.FormattedStringParser

implicit val sdf: FormattedStringParser[Date, DateFormat] =
  new FormattedStringParser[Date, DateFormat] {
      override def apply(str: String): Date = Date.valueOf(str.strip)
      override def apply(str: String, fmt: DateFormat): Date =  new Date(fmt.parse(str.strip).getTime)
  }
```
And can be used as follows:
```scala
import info.fingo.spata.text.StringParser
import java.util.Locale

val df = DateFormat.getDateInstance(DateFormat.SHORT, new Locale("pl", "PL"))
val date = StringParser.parse[Date]("02.02.2020", df)
```
Please note that this sample implementation accepts partial string parsing,
e.g. `"02.02.2020xyz"` will successfully parse to `2020-02-02`.
This is different from the built-in parsing behaviour for `LocalDate`,
where the entire string has to conform to the format. 

Parsing implementations are expected to throw specific runtime exceptions when parsing fails.
This is converted to `ParseError` in `StringParser` object's `parse` method,
while keeping the original exception in `cause` field.

Although this design decision might be seen as questionable,
as returning `Either` instead of throwing an exception could be the better choice,
it is made deliberately - all available Java parsing methods throw an exception,
so it is more convenient to use them directly while implementing `StringParser` traits,
leaving all exception handling in a single place, i.e. the `StringParser.parse` method.  

### Schema validation

Successful CSV parsing means that the underlying source has the correct format (taking into account parser configuration). 
Nonetheless, the obtained records may have any content - being a collection of strings they are very permissive.
We often require strict data content and format to be able to use it in accordance with our business logic.
spata supports basic fields' format definition and validation.

CSV schema can be defined using `schema.CSVSchema`:
```scala
import info.fingo.spata.schema.CSVSchema
import java.time.LocalDateTime

val schema = CSVSchema()
  .add[String]("symbol")
  .add[LocalDateTime]("time")
  .add[BigDecimal]("price")
  .add[String]("currency")
```
Schema is basically specified by the names of expected CSV fields and their data types.

We do not need to include every field from CSV source in the schema definition.
It is enough to do it only for those fields we are interested in.

Schema is validated as part of a regular stream processing pipeline:
```scala
val schema = ???
val stream: Stream[IO, Char] = ???
val parser: CSVParser[IO] = CSVParser[IO]()
val validatedStream = stream.through(parser.parse).through(schema.validate)
```
As a result of the validation process, the ordinary CSV `Record` is converted to `ValidatedRecord[T]`,
which is an alias for `Validated[InvalidRecord, TypedRecord[T]]`.
The parametric type `T` is the compile-time, [shapeless](https://github.com/milessabin/shapeless) based
representation of the record data type. Because it depends on the schema definition and is quite elaborate,
we are not able to manually provide it - we have to let the compiler infer it.
This is why the type signatures are omitted from some variable definitions in code excerpts in this chapter.
Although the compiler infers the types correctly, the IDEs/linters often wrongly report problems.
Please do not be held back by red marks in your code editor.

[Validated](https://typelevel.org/cats/datatypes/validated.html) is a Cats data type for wrapping validation results.
It is similar to `Either`, with `Valid` corresponding to `Right` and `Invalid` to `Left`,
but more suitable for validation scenarios. Please reach for Cats documentation for in-depth introduction.

The compile-time nature of this process makes future record handling fully type-safe:
```scala
val validatedStream = ???
validatedStream.map { validated =>
  validated.map { typedRecord =>
    val symbol: String = typedRecord("symbol")
    val price: BigDecimal = typedRecord("price")
    // ...
  }
}
```
Please notice, that in contrast to regular record, where the result is wrapped in `Decoded[T]`,
we always get the straight type out of typed record.
If we try to access a non-existing field (not defined by schema) or assign it to a wrong value type,
we will get a compilation error:
```scala
val price: BigDecimal = typedRecord("prce") // does not compile 
```

The key (field name) used to access the record value is a [literal type](https://docs.scala-lang.org/sips/42.type.html).
To be accepted by a typed record, the key has to be a literal value or a variable having a singleton type:
```scala
val typedRecord = ???
val narrow: "symbol" = "symbol" // singleton
val wide = "symbol" // String
val symbol1 = typedRecord("symbol")  // OK
val symbol2 = typedRecord(narrow)  // OK
val symbol3 = typedRecord(wide)  // does not compile
```

Typed records, similarly to regular ones, support conversion to case classes:
```scala
case class StockPrice(symbol: String, price: BigDecimal)
val typedRecord = ???
val stockPrice: StockPrice = typedRecord.to[StockPrice]()
```
Like in the case of [regular records](#getting-actual-data),
the conversion is name-based and may cover only a subset of a record's fields.
However, in contrast to a regular record, the result is not wrapped in `Decoded` anymore.

A field declared in the schema has to be present in the source stream.
Moreover, its values, by default, must not be empty.
If values are optional, they have to be clearly marked as such in the schema definition:
```scala
val schema = CSVSchema()
  .add[String]("symbol")
  .add[Option[LocalDateTime]]("time")
```
Please note, that this still requires the field (column) to be present, only permits it to contain empty values.

While processing a validated stream, we have access to invalid data as well: 
```scala
val validatedStream = ???
validatedStream.map { validated =>
  validated.map { typedRecord =>
    val price: BigDecimal = typedRecord("price")
    // ...
  }.leftMap { invalid =>
    val price: Option[String] = invalid.record("price")
    // ...
  }
}
```
(the above `map`/`leftMap` combination may be simplified to `bimap`).

Schema validation requires string parsing, described in the previous chapter.
Similarly to conversion to case classes, we are not able to directly pass a formatter to validation,
so a regular `StringParser` implicit with correct format has to be provided for each parsed type.
All remarks described in [Text parsing](#text-parsing) apply to the validation process.

Type verification, although probably the most important aspect of schema validation,
is often not the only constraint on CSV which is required to successfully process the data.
We often have to check if the values match many other business rules.
spata provides a mean to declaratively verify basic constraints on the field level:
```scala
import info.fingo.spata.schema.CSVSchema
import java.time.LocalDateTime
import info.fingo.spata.schema.validator._

val schema = CSVSchema()
  .add[String]("symbol", LengthValidator(3, 5))
  .add[LocalDateTime]("time")
  .add[BigDecimal]("price", MinValidator(0.01))
  .add[String]("currency", LengthValidator(3))
```
Records which do not pass the provided schema validators render the result invalid, as in the case of a wrong type.

It is possible to provide multiple validators for each field.
The validation process for a field is stopped on the first failing validator.
The order of running validators is not specified.
Nevertheless, the validation is run independently for each field defined by the schema.
The returned `InvalidRecord` contains error information from all incorrect fields.   

The validators are defined in terms of typed (already correctly parsed) values.
A bunch of typical ones are available as part of `info.fingo.spata.schema.validator` package.
Additional ones may be provided by implementing the `schema.validator.Validator` trait.

### Error handling

There are three types of errors which may arise while parsing CSV:

*   Various I/O errors, including but not limited to `IOException`.
    They are not directly related to parsing logic but CSV is typically read from an external, unreliable source.
    They may be raised by `reader` operations.

*   Errors caused by malformed CSV structure, reported as `StructureException`.
    They may be caused by `CSVParser`'s methods.

*   Errors caused by unexpected / incorrect data in record fields, reported as `HeaderError` or `DataError`.
    They may result from interactions with `Record`.

The two first error categories are unrecoverable and stop stream processing.
For the `StructureException` errors we are able to precisely identify the place that caused the problem.
See Scaladoc for `CSVException` for further information about error location.

The last category is reported on the record level and allows for different handling policies.
Please notice however, that if the error is not handled locally (e.g. using safe functions returning `Decoded`)
and propagates through the stream, further processing of input data is stopped, like for the above error categories.  

Errors are raised and should be handled by using the [FS2 error handling](https://fs2.io/guide.html#error-handling) mechanism.
FS2 captures exceptions thrown or reported explicitly with `raiseError`
and in both cases is able to handle them with `handleErrorWith`.
To fully support this, `CSVParser` requires the `RaiseThrowable` type class instance for its effect `F`,
which is covered with `cats.effect.Sync` type class.

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
      val parser: CSVParser[IO] = CSVParser[IO]()
      implicit val codec: Codec = Codec.UTF8
      val src = Paths.get("testdata/fahrenheit.txt")
      val dst = Paths.get("testdata/celsius.txt")

      reader
        .shifting[IO](blocker)
        .read(src)
        .through(parser.parse)
        .filter(r => r("temp").exists(!_.isBlank)
        .map { r =>
          for {
            date <- r.get[String]("date")
            fTemp <- r.get[Double]("temp")
            cTemp = fahrenheitToCelsius(fTemp)
          } yield s"$date,$cTemp"
        }
        .rethrow
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

The `rethrow` method in above code raises error for `Left`, converting `Either` to a simple values.

Sometimes we would like to convert a stream to a collection.
We should wrap the result in `Either` in such situations to distinguish successful processing from erroneous one.
See the first code snippet in [Basic usage](#basic-usage) for sample.

### Logging

Logging is turned off by default in spata (no-op logger) and may be activated by defining an implicit `util.Logger`,
passing an SLF4J logger instance to it:
```scala
import org.slf4j.LoggerFactory
import info.fingo.spata.util.Logger

val slf4jLogger = LoggerFactory.getLogger("spata")
implicit val spataLogger: Logger[IO] = new Logger[IO](slf4jLogger)
```
spata does not create per-class loggers but uses the provided one for all logging operations.

All logging operations are deferred in the stream effect and executed as part of effect evaluation,
together with main effectful operations.

The logging is currently limited to only a few events per parsed CSV source (single `info` entry,
a couple of `debug` entries and possibly an `error` entry). There are no log events generated per CSV record.
No stack trace is recorded for `error` events.

The `debug` level introduces additional operations on the stream and may slightly impact performace.

No parsed data is explicitly written to the log.
It can however occur, when the CSV source is assumed to have a header row,
but it does not and then the first record of data is assumed to be the header and is logged at debug level.
Please do not use the debug level if data security is crucial.

Alternatives
------------

For those who need a different characteristic of a CSV library, there are a few alternatives available for Scala:
*   [Itto-CSV](https://github.com/gekomad/itto-csv) - CSV handling library based on FS2 and Cats with support for case class conversion.
*   [fs2  data](https://github.com/satabin/fs2-data) - collection of FS2 based parsers, including CSV.
*   [kantan.csv](https://github.com/nrinaudo/kantan.csv) - well documented CSV parser/serializer with support for different parsing engines.
*   [scala-csv](https://github.com/tototoshi/scala-csv) - easy to use CSV reader/writer.

Credits
-------

**spata** makes use of the following tools, languages, frameworks, libraries and data sets (in alphabetical order):
*   [Cats Effect](https://typelevel.org/cats-effect/) licensed under [Apache-2.0](https://github.com/typelevel/cats-effect/blob/master/LICENSE.txt) /C
*   [Codecov](https://codecov.io/) available under following [Terms of Use](https://codecov.io/terms) /D
*   [FS2](https://fs2.io/) licensed under [MIT](https://github.com/functional-streams-for-scala/fs2/blob/master/LICENSE) /C
*   [Git](https://git-scm.com/) licensed under [GPL-2.0](https://git-scm.com/about/free-and-open-source) /D
*   [GitHub](https://github.com/) available under following [Terms of Service](https://help.github.com/en/github/site-policy/github-terms-of-service) /D
*   [Gitter](https://gitter.im/) available under following [Terms of Use](https://about.gitlab.com/terms/) /D
*   [http4s](https://http4s.org/) licensed under [Apache-2.0](https://github.com/http4s/http4s#license) /S
*   [IntelliJ IDEA CE](https://www.jetbrains.com/idea/) licensed under [Apache 2.0](https://www.jetbrains.com/idea/download/) /D
*   [javadoc.io](https://www.javadoc.io/) licensed under [Apache-2.0](https://github.com/maxcellent/javadoc.io/blob/master/LICENSE) /D
*   [Mars weather data](https://github.com/the-pudding/data/tree/master/mars-weather) made publicly available by [NASA](https://pds.nasa.gov/) and [CAB](https://cab.inta-csic.es/rems/en) /T
*   [Metals](https://scalameta.org/metals/) licensed under [Apache-2.0](https://github.com/scalameta/metals/blob/main/LICENSE) /D
*   [OpenJDK](https://adoptopenjdk.net/) licensed under [GPL-2.0 with CE](https://openjdk.java.net/legal/gplv2+ce.html) /C
*   [sbt](https://www.scala-sbt.org/) licensed under [BSD-2-Clause](https://www.lightbend.com/legal/licenses) /D
*   [sbt-api-mappings](https://github.com/ThoughtWorksInc/sbt-api-mappings) licensed under [Apache-2.0](https://github.com/ThoughtWorksInc/sbt-api-mappings/blob/3.0.x/LICENSE) /D
*   [sbt-dynver](https://github.com/dwijnand/sbt-dynver) licensed under [Apache-2.0](https://github.com/dwijnand/sbt-dynver/blob/master/LICENSE) /D
*   [sbt-header](https://github.com/sbt/sbt-header) licensed under [Apache-2.0](https://github.com/sbt/sbt-header/blob/master/LICENSE) /D
*   [sbt-pgp](https://github.com/sbt/sbt-pgp) licensed under [BSD-3-Clause](https://github.com/sbt/sbt-pgp/blob/master/LICENSE) /D
*   [sbt-scoverage](https://github.com/scoverage/sbt-scoverage) licensed under [Apache-2.0](https://github.com/scoverage/sbt-scoverage#license) /D
*   [sbt-sonatype](https://github.com/xerial/sbt-sonatype) licensed under [Apache-2.0](https://github.com/xerial/sbt-sonatype/blob/master/LICENSE.txt) /D
*   [Scala](https://www.scala-lang.org/download/) licensed under [Apache-2.0](https://www.scala-lang.org/license/) /C
*   [Scalafix](https://github.com/scalacenter/scalafix) licensed under [BSD-3-Clause](https://github.com/scalacenter/scalafix/blob/master/LICENSE.md) /D
*   [Scalafmt](https://scalameta.org/scalafmt/docs/installation.html#sbt) licensed under [Apache-2.0](https://github.com/scalameta/scalafmt/blob/master/LICENCE.md) /D
*   [ScalaMeter](https://scalameter.github.io/) licensed under [BSD-3-Clause](https://scalameter.github.io/home/license/) /T
*   [ScalaTest](http://www.scalatest.org/) licensed under [Apache-2.0](http://www.scalatest.org/about) /T
*   [shapeless](https://github.com/milessabin/shapeless) licensed under [Apache-2.0](https://github.com/milessabin/shapeless/blob/master/LICENSE) /C
*   [SLF4J](http://www.slf4j.org/) licensed under [MIT](http://www.slf4j.org/license.html) /C
*   [sonatype OSSRH](https://central.sonatype.org/) available under following [Terms of Service](https://central.sonatype.org/pages/central-repository-producer-terms.html) /D
*   [Travis CI](https://travis-ci.org/) available under following [Terms of Service](https://docs.travis-ci.com/legal/terms-of-service/) /D

**/C** means compile/runtime dependency,
**/T** means test dependency,
**/S** means source code derivative and
**/D** means development tool.
Only direct dependencies are presented in the above list.
