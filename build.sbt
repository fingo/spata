lazy val basicSettings = Seq(
  organization := "info.fingo",
  organizationName := "FINGO sp. z o.o.",
  organizationHomepage := Some(url("http://fingo.info")),
  startYear := Some(2020),
  name := "spata",
  description := "Functional, stream based CSV processor for Scala",
  scalaVersion := "3.3.5"
)

addCommandAlias("check", "; scalafmtCheck ; scalafix --check")
addCommandAlias("audit", "; mimaReportBinaryIssues ; licenseCheck")
addCommandAlias(
  "validate",
  "; clean; compile; Test/compile; scalafmtCheck; scalafix --check; test; mimaReportBinaryIssues; licenseCheck; doc; Perf/test"
)

lazy val PerformanceTest = config("perf").extend(Test)
def perfFilter(name: String): Boolean = name.endsWith("PTS")
def unitFilter(name: String): Boolean = name.endsWith("TS") && !perfFilter(name)

lazy val root = (project in file("."))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(basicSettings *)
  .settings(publishSettings *)
  .settings(licenseSettings *)
  .configs(PerformanceTest)
  .settings(
    versionScheme := Some("semver-spec"),
    headerLicenseStyle := HeaderLicenseStyle.SpdxSyntax,
    headerEmptyLine := false,
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.6.0",
      "co.fs2" %% "fs2-core" % "3.12.0",
      "co.fs2" %% "fs2-io" % "3.12.0",
      "org.slf4j" % "slf4j-api" % "2.0.17",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      (("com.storm-enroute" %% "scalameter" % "0.21").cross(CrossVersion.for3Use2_13) % Test)
        .exclude("org.scala-lang.modules", "scala-xml_2.13"),
      "org.slf4j" % "slf4j-simple" % "2.0.17" % Test
    ),
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    inConfig(PerformanceTest)(Defaults.testTasks),
    Test / testOptions := Seq(Tests.Filter(unitFilter)),
    Test / fork := true,
    PerformanceTest / testOptions := Seq(Tests.Filter(perfFilter)),
    PerformanceTest / logBuffered := false,
    PerformanceTest / parallelExecution := false,
    javaOptions += "-Dfile.encoding=UTF-8",
    scalacOptions ++= scalacSettings,
    Compile / console / scalacOptions --= Seq("-Xfatal-warnings"),
    Test / scalacOptions --= Seq("-Wnonunit-statement"),
    mimaPreviousArtifacts := Set("info.fingo" %% "spata" % "3.2.0"),
    semanticdbEnabled := true,
    autoAPIMappings := true
  )

lazy val licenseSettings = Seq(
  licenses += ("Apache-2.0", new URI("https://www.apache.org/licenses/LICENSE-2.0.txt").toURL),
  licenseCheckAllow := Seq(
    LicenseCategory.Apache,
    LicenseCategory.BouncyCastle,
    LicenseCategory.BSD,
    LicenseCategory.CC0,
    LicenseCategory.MIT,
    LicenseCategory.PublicDomain,
    LicenseCategory.JSON,
    LicenseCategory.Unicode
  )
)

import xerial.sbt.Sonatype.GitHubHosting
lazy val publishSettings = Seq(
  ThisBuild / sonatypeCredentialHost := "oss.sonatype.org",
  sonatypeRepository := "https://oss.sonatype.org/service/local",
  sonatypeProjectHosting := Some(GitHubHosting("fingo", "spata", "robert.marek@fingo.info")),
  developers := List(Developer("susuro", "Robert Marek", "robert.marek@fingo.info", url("https://github.com/susuro")))
)

lazy val scalacSettings = Seq(
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-explain", // Explain errors in more detail.
  "-explain-types", // Explain type errors in more detail.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-language:existentials,higherKinds,implicitConversions", // Enable language features.
  "-new-syntax", // Require Scala 3 syntax
  "-pagewidth:120", // Set output page width.
  "-release:11", // Set target JVM version.
  "-unchecked", // Enable additional warnings where generated code depends on assumptions.
  "-Wvalue-discard", // Warn about unused expression results.
  "-Wunused:all", // Warn about unused code.
  "-Wnonunit-statement", // Warn when statemens are non-Unit expressions.
  "-Wconf:cat=deprecation:w,any:e", // Fail the compilation if there are any warnings except deprecation.
  "-Xverify-signatures" // Verify generic signatures in generated bytecode.
)
