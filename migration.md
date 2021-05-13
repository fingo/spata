Migration notes
===============

This file described changes to be made while upgrading **spata** between incompatible versions.  

Upgrading to 2.x from 1.x
-------------------------

spata 2 introduces rendering functionality.
Some changes to API were required to keep parsing and rendering in pair with each other.
Others were made to improve the API taking the opportunity of already broken compatibility.

### Configuration

*   Call `parser[F]` instead of `get[F]()` to create parser for given configuration.
*   Omit parentheses in calls to `noHeader` and `stripSpaces` methods.
  
E.g. instead of
```scala
val parser = CSVConfig().noHeader().get[IO]()
```
write
```scala
val parser = CSVConfig().noHeader.parser[IO]
```

### Parsing

*   Omit parentheses while calling `CSVParser.apply` (e.g. `CSVParser[IO]`).

### Reading data

*   Replace `io.reader` object with `io.Reader`.
*   Omit parentheses in calls to `apply`, `plain`, and `shifting`.

E.g. instead of
```scala
val stream = reader[IO]().read(Paths.get("source.csv"))
```
write
```scala
val stream = Reader[IO].read(Paths.get("source.csv"))
```

### Miscellaneous

*   Methods with arity-0 have been stripped of parentheses where feasible because they do not have side effects.
*   Many classes and traits have been declared final or sealed.
*   `Reader` trait has been moved from `reader` object into `io` package.
