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

Upgrading to this version from 2.x
----------------------------------

This version of **spata** runs on Scala 2 and requires Cats Effect 3 and FS2 3.

Cats Effect 3 introduced breaking changes, which in turn made FS2 v3 incompatible with previous version.
This required changes in the areas involving effect handling, concurrency and io.

### IO

`Blocker` has been removed from Cats Effect 3 and you do not need (nor can) provide its instance for io operations.
Thread pool assigment for blocking io is handled by runtime based on information provided by spata or FS2 io methods.
For more information about new threading model for io and the ways to control this behavior see
[Cats Effect 3 migration guide](https://typelevel.org/cats-effect/docs/migration-guide#blocker).

This removal simplifies the `Reader.Shifting` and `Writer.Shifting` APIs - 
no blocker is provided as a parameter anymore. Instead of
```scala
Stream.resource(Blocker[IO]).flatMap { blocker =>
	Reader.shifting[IO](blocker).read(Path.of("path"))
	// ...
}
```
you should simply write
```scala
Reader.shifting[IO].read(Path.of("path"))
// ...
```
