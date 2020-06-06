lazy val basicSettings = Seq(
  organization := "info.fingo",
  organizationName := "FINGO sp. z o.o.",
  startYear := Some(2020),
  name := "spata",
  scalaVersion := "2.13.2"
)

lazy val PerformanceTest = config("perf").extend(Test)
def perfFilter(name: String): Boolean = name.endsWith("PTS")
def unitFilter(name: String): Boolean = name.endsWith("TS") && !perfFilter(name)

lazy val root = (project in file("."))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(basicSettings: _*)
  .settings(publishSettings: _*)
  .configs(PerformanceTest)
  .settings(
    licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    headerLicenseStyle := HeaderLicenseStyle.SpdxSyntax,
    headerEmptyLine := false,
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "2.1.2",
      "co.fs2" %% "fs2-core" % "2.3.0",
      "co.fs2" %% "fs2-io" % "2.3.0",
      "com.chuusai" %% "shapeless" % "2.3.3",
      "org.scalatest" %% "scalatest" % "3.1.1" % Test,
      "com.storm-enroute" %% "scalameter" % "0.19" % Test,
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.6" % Test
    ),
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    inConfig(PerformanceTest)(Defaults.testTasks),
    Test / testOptions := Seq(Tests.Filter(unitFilter)),
    Test / fork := true,
    PerformanceTest / testOptions := Seq(Tests.Filter(perfFilter)),
    PerformanceTest / fork := false,
    PerformanceTest / logBuffered := false,
    PerformanceTest / parallelExecution := false,
    javaOptions += "-Dfile.encoding=UTF-8",
    scalacOptions ++= scalacSettings,
    scalacOptions in (Compile, console) --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings"),
    autoAPIMappings := true
  )

import xerial.sbt.Sonatype.GitHubHosting
lazy val publishSettings = Seq(
  sonatypeProjectHosting := Some(GitHubHosting("fingo", "spata", "robert.marek@fingo.info")),
  publishMavenStyle := true,
  publishTo := sonatypePublishToBundle.value,
  pgpPublicRing := file("ci/public-key.asc"),
  pgpSecretRing := file("ci/secret-key.asc"),
  developers := List(Developer("susuro", "Robert Marek", "robert.marek@fingo.info", url("https://github.com/susuro")))
)

lazy val scalacSettings = Seq( // based on https://nathankleyn.com/2019/05/13/recommended-scalac-flags-for-2-13/
  "-target:11",
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-explaintypes", // Explain type errors in more detail.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
  "-language:higherKinds", // Allow higher-kinded types
  "-language:implicitConversions", // Allow definition of implicit functions called views
  "-unchecked", // Enable additional warnings where generated code depends on assumptions.
  "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
  "-Xfatal-warnings", // Fail the compilation if there are any warnings.
  "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
  "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
  "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
  "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
  "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
  "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
  "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
  "-Xlint:option-implicit", // Option.apply used implicit view.
  "-Xlint:package-object-classes", // Class or object defined in package object.
  "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
  "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
  "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
  "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
  "-Ywarn-dead-code", // Warn when dead code is identified.
  "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
  "-Ywarn-numeric-widen", // Warn when numerics are widened.
  "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
  "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
  // Bug in Scala: https://github.com/scala/bug/issues/11175
  //"-Ywarn-unused:locals", // Warn if a local definition is unused.
  "-Ywarn-unused:params", // Warn if a value parameter is unused.
  "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
  "-Ywarn-unused:privates", // Warn if a private member is unused.
  "-Ywarn-value-discard", // Warn when non-Unit expression results are unused.
  "-Ybackend-parallelism",
  "8", // Enable paralellisation — change to desired number!
  "-Ycache-plugin-class-loader:last-modified", // Enables caching of classloaders for compiler plugins
  "-Ycache-macro-class-loader:last-modified", // and macro definitions. This can lead to performance improvements.
  "-Yrangepos", // required by SemanticDB compiler plugin
  // sbt-api-mappings cannot link to Java API
  """-Wconf:cat=scaladoc&msg=(Could not find any member to link for "(Runtime|IO|NoSuchElement|IndexOutOfBounds)Exception"):s"""
)
