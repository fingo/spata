lazy val basicSettings = Seq(
  organization := "info.fingo",
  organizationName := "FINGO",
  name := "ScalaCSV",
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.12.5"
)

lazy val root = (project in file(".")).
  settings(basicSettings: _*).
  settings(
    fork in run := true,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      // added explicitly only to avoid warning about multiple dependencies with the same organization/name but different versions
      "org.scala-lang" % "scala-reflect" % "2.12.5",
      "org.scala-lang.modules" %% "scala-xml" % "1.1.0"
    )
  )
