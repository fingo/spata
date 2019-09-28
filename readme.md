spata
=====

**spata** is a functional scala parser for tabular data (`CSV`).

Main goal of the library is providing precise information about errors in source data while keeping good performance.
In real life importing `CSV` data is problematic mainly because the source may be malformed and the end users struggle to find the reason and fix it.

The source data format is assumed to conform to [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt).
It is possible however to configure the parser to accept other field and record separator characters as well as quote character.
Separators and quote tokens are required to be single characters.
`CRLF` is treated as special case - setting record separator to `LF` automatically accepts `CRLF` too.  

The library provides already a practical solution although with minimalistic feature set.  
Planned development includes:
* Better error handling, probably using `handleErrorWith`.
* Providing more comprehensive and clear API and documenting it.
* Tidying / refactoring and documenting parsing code.
* Using FS2 I/O library in addition to / in place of Scala's `Source` including safe resource acquisition and release.
* Declaration of source data format (e.g. data types) and its validation.
* Supporting asynchronous execution.

The library is based on [FS2 - Functional Streams for Scala](https://github.com/functional-streams-for-scala/fs2).