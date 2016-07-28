lazy val basicSettings = Seq(
  organization := "info.fingo",
  organizationName := "FINGO",
  name := "ScalaCSV",
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(basicSettings: _*).
  settings(
    fork in run := true,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.2.6" % "test",
      // added explicitly only to avoid warning about multiple dependencies with the same organization/name but different versions
      "org.scala-lang" % "scala-reflect" % "2.11.8",
      "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4"
    )
  )
