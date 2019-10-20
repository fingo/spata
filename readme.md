spata
=====

**spata** is a functional Scala parser for tabular data (`CSV`).

Main goal of the library is to provide precise information about errors in source data (their location) while keeping good performance.

The source data format is assumed to conform to [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt).
It is possible however to configure the parser to accept separator and quote symbols.
Separators and quote tokens are required to be single characters.
`CRLF` is treated as special case - setting record separator to `LF` automatically accepts `CRLF` too.  

The library provides already a practical solution although with minimalistic feature set.  
Planned development includes:
* Providing more comprehensive and clear API and documenting it.
* Tidying / refactoring and documenting parsing code.
* Parsing chunks instead of single elements
* Using FS2 I/O library in addition to / in place of Scala's `Source` including safe resource acquisition and release.
* Declaration of source data format (e.g. data types) and its validation.
* Supporting asynchronous execution.

The library is based on [FS2 - Functional Streams for Scala](https://github.com/functional-streams-for-scala/fs2).