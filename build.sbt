lazy val basicSettings = Seq(
  organization := "info.fingo",
  organizationName := "FINGO",
  name := "spata",
  version := "0.2-SNAPSHOT",
  scalaVersion := "2.13.1"
)

lazy val root = (project in file(".")).
  settings(basicSettings: _*).
  settings(
    fork in run := true,
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-core" % "2.0.1",
      "co.fs2" %% "fs2-io" % "2.0.1",
      "org.scalatest" %% "scalatest" % "3.0.8" % "test"
    )
  )
