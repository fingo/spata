spata
=====

**spata** is a functional Scala parser for tabular data (`CSV`).

Main goal of the library is to provide precise information about errors in source data (their location) while keeping good performance.

The source data format is assumed to conform to [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt).
It is possible however to configure the parser to accept separator and quote symbols.
Separators and quote tokens are required to be single characters.
`CRLF` is treated as special case - setting record separator to `LF` automatically accepts `CRLF` too.

Examples how to use the library may be found in `src/test/scala/sample`.

The library provides already a practical solution although with minimalistic feature set.  
Planned development includes:
* Providing conversion of records to case classes through shapeless.
* Providing more examples how to use the library.
* Enhancing this readme to provide gentle introduction to the library.
* Tidying and better documenting parsing code.
* Parsing chunks instead of single elements
* Using FS2 I/O library in addition to / in place of Scala's `Source` including safe resource acquisition and release.
* Declaring schema and providing its validation.
* Supporting asynchronous execution.

The library is based on [FS2 - Functional Streams for Scala](https://github.com/functional-streams-for-scala/fs2).

Credits
-------

**spata** makes use of following tools, languages, frameworks, libraries and data sets (in alphabetical order):
* [Cats Effect](https://typelevel.org/cats-effect/) licensed under [Apache 2 License](https://github.com/typelevel/cats-effect/blob/master/LICENSE.txt) /R
* [FS2](https://fs2.io/) licensed under [MIT License](https://github.com/functional-streams-for-scala/fs2/blob/master/LICENSE) /R
* [Git](https://git-scm.com/) licensed under [GPLv2](https://git-scm.com/about/free-and-open-source) /D
* [IntelliJ IDEA CE](https://www.jetbrains.com/idea/) licensed under [Apache 2 License](https://www.jetbrains.com/idea/download/) /D
* [Mars weather data](https://github.com/the-pudding/data/tree/master/mars-weather) made publicly available by [NASA](https://pds.nasa.gov/) and [CAB](https://cab.inta-csic.es/rems/en) /T
* [OpenJDK](https://adoptopenjdk.net/) licensed under [GPLv2+CE](https://openjdk.java.net/legal/gplv2+ce.html) /R
* [sbt](https://www.scala-sbt.org/) licensed under [BSD License](https://www.lightbend.com/legal/licenses) /D
* [sbt-api-mappings](https://github.com/ThoughtWorksInc/sbt-api-mappings) licensed under [Apache 2 License](https://github.com/ThoughtWorksInc/sbt-api-mappings/blob/3.0.x/LICENSE) /D
* [sbt-scoverage](https://github.com/scoverage/sbt-scoverage) licensed under [Apache 2 License](https://github.com/scoverage/sbt-scoverage#license) /D
* [Scala](https://www.scala-lang.org/download/) licensed under [Apache 2 License](https://www.scala-lang.org/license/) /R
* [Scalafmt](https://scalameta.org/scalafmt/docs/installation.html#sbt) licensed under [Apache 2 License](https://github.com/scalameta/scalafmt/blob/master/LICENCE.md) /D
* [ScalaTest](http://www.scalatest.org/) license under [Apache 2 License](http://www.scalatest.org/about) /T

**/R** means runtime dependency, **/T** means test dependency and **/D** means development tool.
For libraries, only direct dependencies are presented on above list.
