Contributor guide
=================

Thanks for taking an interest in improving spata. Here you can find some hints on how to contribute to the project.

How to help
-----------

Different kinds of contributions are welcome, including, but not limited to:
* improvements in the documentation - scope, accuracy, clarity and language, 
* providing additional examples on how to use the library,
* reporting and fixing bugs,
* refactoring existing code and enhancing tests,
* proposing and implementing improvements and new features.

In case of any doubts, talk to us on the [spata Gitter channel](https://gitter.im/fingo-spata/community).

Proposing and submitting a change
---------------------------------

If you would like to make any changes to the project, please:
* let us know about this by commenting on an existing [issue](https://github.com/fingo/spata/issues) 
or creating a new one and describing your idea,
* fork and build the project,
* implement your contribution along with tests and a documentation update,
* format the code and run the tests,
* submit a [pull request](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork).

spata follows a standard [fork and pull](https://guides.github.com/activities/forking/) model for contributions. 

### Building the project

spata requires Java 11 and sbt.

All you need to do to build the project is to run `sbt compile` or preferably `sbt test:compile`.

### Formatting and testing

spata uses [Scalafmt](https://scalameta.org/scalafmt/) to coherently and automatically format the code.
To check if code is properly formatted run `sbt scalafmtCheck`. To format all sources run `sbt scalafmtAll`.

spata also uses [Scalafix](https://scalacenter.github.io/scalafix/) for linting.
You can run a linter check by running `sbt scalafix --check`.

To check both, formatting and linting with single command, run `sbt check`.

Unit and integration tests should be run with `sbt test`.
Test coverage may be verified with `sbt clean coverage test coverageReport`.
Results are written to `target/scala-2.13/scoverage-report/index.html`

Performance tests may be run with `sbt perf:test`.
There is no automatic regression verification for performance test,
however it is still worth running them before and after implementing changes and comparing results.

To check binary compatibility with a previous stable version, please run `sbt mimaReportBinaryIssues`.

API documentation is built with `sbt doc`.

Full project verification before a pull request should be done with:
```
sbt -mem 2048 clean check doc test mimaReportBinaryIssues perf:test
```

Grant of license
----------------
spata is licensed under [Apache-2.0](https://github.com/fingo/spata/blob/master/LICENSE).
Opening a pull request implies your consent to license your contribution under Apache License 2.0.

Attributions
------------

If your contribution has been derived from or inspired by other work,
please state this in its scaladoc comment and provide proper attribution.
When possible, include the original authors’ names and a link to the original work.

*This guide borrows from [fs2](https://github.com/typelevel/fs2/blob/main/CONTRIBUTING.md),
[http4s](https://github.com/http4s/http4s/blob/master/CONTRIBUTING.md) and
[cats](https://github.com/typelevel/cats/blob/master/CONTRIBUTING.md) contribution guides.*

