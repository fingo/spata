lazy val basicSettings = Seq(
  organization := "info.fingo",
  organizationName := "FINGO",
  name := "ScalaCSV",
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.12.8"
)

lazy val root = (project in file(".")).
  settings(basicSettings: _*).
  settings(
    fork in run := true,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.5" % "test"
    )
  )
