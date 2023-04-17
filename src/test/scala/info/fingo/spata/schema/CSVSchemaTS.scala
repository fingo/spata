/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor7}
import info.fingo.spata.Record
import info.fingo.spata.io.Reader
import info.fingo.spata.CSVParser
import info.fingo.spata.schema.validator._
import info.fingo.spata.text.StringParser

class CSVSchemaTS extends AnyFunSuite with TableDrivenPropertyChecks:

  private val separator = ','

  type LV[A] = List[Validator[A]]
  type TestCase = (String, String, LV[Int], LV[String], LV[String], LV[LocalDate], LV[Double])
  type TestCaseTable = TableFor7[String, String, LV[Int], LV[String], LV[String], LV[LocalDate], LV[Double]]

  given dateParser: StringParser[LocalDate] with
    def apply(str: String) = LocalDate.parse(str.strip, DateTimeFormatter.ofPattern("dd.MM.yyyy"))

  test("validation should positively verify data compliant with schema") {
    val id: "id" = "id"
    for testData <- validCases yield
      val result = validate(testData)
      assert(result.length == 3)
      result.foreach(validated =>
        assert(validated.isValid)
        validated.map(typedRecord =>
          val knownFrom: LocalDate = typedRecord("appeared")
          assert(typedRecord(id) > 0)
          assert(knownFrom.getYear < 2020)
          assert(typedRecord("score") >= 0)
        )
      )
  }

  test("validation should provide errors for data not compliant with schema") {
    for testData <- invalidCases yield
      val result = validate(testData)
      assert(result.length == 3)
      result.foreach(validated =>
        assert(validated.isInvalid)
        validated.leftMap(invalidRecord =>
          invalidRecord.flaws.map(fieldFlaw =>
            assert(fieldFlaw.error.message.nonEmpty)
            val ec = fieldFlaw.error.code
            assert(ec.endsWith("Validator") || ec.endsWith("Type"))
          )
        )
      )
  }

  test("it should be possible to convert typed record to case class after validation") {
    case class FullData(id: Int, name: String, occupation: String, appeared: LocalDate, score: Double)
    case class Person(id: Int, score: Double, name: String)
    val id: "id" = "id"
    for testData <- validCases yield
      val result = validate(testData)
      result.foreach(validated =>
        validated.map(typedRecord =>
          val fd = typedRecord.to[FullData]
          assert(fd.id == typedRecord(id))
          assert(fd.score >= 0)
          val person = typedRecord.to[Person]
          assert(person.id == typedRecord(id))
          assert(person.name == typedRecord("name"))
        )
      )
  }

  test("it should be possible to validate types only") {
    val schema = CSVSchema()
      .add[Int]("id")
      .add[String]("name")
      .add[LocalDate]("appeared")
      .add[Double]("score")
    val validated = recordStream("basic").through(schema.validate)
    val result = validated.compile.toList.unsafeRunSync()
    assert(result.length == 3)
    assert(result.forall(_.isValid))
    result.head.map(tr => assert(tr("id") == 1))
  }

  test("validation should support optional values") {
    val name: "name" = "name"
    val schema = CSVSchema()
      .add[String](name, RegexValidator(".+?"))
      .add[Option[Double]]("score", FiniteValidator())
    val validated = recordStream("missing values").through(schema.validate)
    val result = validated.compile.toList.unsafeRunSync()
    assert(result.length == 3)
    result.foreach(validated =>
      assert(validated.isValid)
      validated.map(typedRecord =>
        assert(typedRecord(name).length > 0)
        assert(typedRecord("score").forall(_ >= 0))
      )
    )
  }

  test("validation against empty schema should be always correct") {
    val schema = CSVSchema()
    for dataSet <- dataSets yield
      val validated = recordStream(dataSet).through(schema.validate)
      val result = validated.compile.toList.unsafeRunSync()
      assert(result.length == 3)
      result.foreach(validated =>
        assert(validated.isValid)
        validated.map(typedRecord => assert(typedRecord.rowNum > 0))
      )
  }

  test("validation with non-existent field should yield invalid for all records") {
    val schema = CSVSchema()
      .add[String]("name")
      .add[String]("nonexistent")
    for dataSet <- dataSets yield
      val validated = recordStream(dataSet).through(schema.validate)
      val result = validated.compile.toList.unsafeRunSync()
      assert(result.length == 3)
      result.foreach(validated =>
        assert(validated.isInvalid)
        validated.leftMap(invalidRecord =>
          assert(invalidRecord.record.rowNum > 0)
          invalidRecord.flaws.map(fieldFlaw =>
            assert(fieldFlaw.error.message.nonEmpty)
            assert(fieldFlaw.error.code.endsWith("Key"))
          )
        )
      )
  }

  test("validation should work for large records") {
    val schema = CSVSchema()
      .add[String]("h10")
      .add[String]("h20")
      .add[String]("h30")
      .add[String]("h40")
      .add[String]("h50")
      .add[Int]("h51", MinValidator(50))
      .add[Int]("h52", MinValidator(50))
      .add[Int]("h53", MinValidator(50))
      .add[Int]("h54", MinValidator(50))
      .add[Int]("h55", MinValidator(50))
      .add[Int]("h56", MinValidator(50))
      .add[Int]("h57", MinValidator(50))
      .add[Int]("h58", MinValidator(50))
      .add[Int]("h59", MinValidator(50))
      .add[String]("h60")
      .add[String]("h70")
      .add[Int]("h71")
      .add[Int]("h72")
      .add[Int]("h73")
      .add[Int]("h74")
      .add[Int]("h75")
      .add[Int]("h76")
      .add[Int]("h77")
      .add[Int]("h78")
      .add[Int]("h79")
    val validated = recordStream("large").through(schema.validate)
    val result = validated.compile.toList.unsafeRunSync()
    assert(result.length == 3)
    result.foreach(validated =>
      assert(validated.isValid)
      validated.map(typedRecord =>
        assert(typedRecord("h10") == "10")
        assert(typedRecord("h77") == 77)
      )
    )
  }

  test("schema should not accept the same column name twice") {
    assertDoesNotCompile("""CSVSchema()
      .add[Int]("id")
      .add[Int]("id")""")
    assertDoesNotCompile("""CSVSchema()
      .add[Int]("id")
      .add[String]("name")
      .add[Int]("id", MinValidator(0))
      .add[Double]("score")""")
    assertDoesNotCompile("""CSVSchema()
      .add[String]("id")
      .add[Int]("id", MinValidator(0))
      .add[Double]("score")""")
  }

  private def validate(testData: TestCase) =
    val (_, data, idValidators, nameValidators, occupationValidators, appearedValidators, scoreValidators) = testData
    val schema = CSVSchema()
      .add[Int]("id", idValidators: _*)
      .add[String]("name", nameValidators: _*)
      .add[String]("occupation", occupationValidators: _*)
      .add[LocalDate]("appeared", appearedValidators: _*)
      .add[Double]("score", scoreValidators: _*)
    val validated = recordStream(data).through(schema.validate)
    validated.compile.toList.unsafeRunSync()

  private lazy val validCases: TestCaseTable = Table(
    ("testCase", "data", "idValidator", "nameValidator", "occupationValidator", "appearedValidator", "scoreValidator"),
    ("basic", "basic", Nil, List(RegexValidator("""\w*\s\wo.*""")), Nil, Nil, List(RangeValidator(0.0, 1000.0))),
    ("multiple", "basic", Nil, Nil, Nil, Nil, List(MinValidator(0.0), MaxValidator(1000.0))),
    ("no validators", "basic", Nil, Nil, Nil, Nil, Nil)
  )

  private lazy val invalidCases: TestCaseTable = Table(
    ("testCase", "data", "idValidator", "nameValidator", "occupationValidator", "appearedValidator", "scoreValidator"),
    (
      "incongruent validators",
      "basic",
      List(MinValidator(2)),
      List(RegexValidator("Fun.*")),
      Nil,
      Nil,
      List(RangeValidator(100.0, 200.0))
    ),
    ("multiple first", "basic", Nil, Nil, Nil, Nil, List(MinValidator(1000.0), MaxValidator(1000.0))),
    ("multiple last", "basic", Nil, Nil, Nil, Nil, List(MinValidator(0.0), MaxValidator(-1.0))),
    ("wrong types", "wrong types", Nil, Nil, Nil, Nil, Nil),
    ("no validators", "missing values", Nil, Nil, Nil, Nil, Nil),
    (
      "congruent validators",
      "missing values",
      List(MinValidator(0)),
      Nil,
      Nil,
      List(MaxValidator(LocalDate.of(2020, 1, 1))),
      Nil
    )
  )

  private lazy val dataSets = Seq("basic", "wrong types", "missing values", "large")

  private val largeRange = 1 to 100

  private def recordStream(dataSet: String) =
    val content = csvContent(dataSet)
    val header = csvHeader(dataSet)
    val source = Source.fromString(s"$header\n$content")
    Reader[IO].read(source).through(CSVParser[IO].parse)

  private def csvHeader(dataSet: String) = dataSet match
    case "large" => largeRange.map(i => s"h$i").mkString(separator.toString)
    case _ => s"id${separator}name${separator}occupation${separator}appeared${separator}score"

  private def csvContent(dataSet: String): String =
    val s = separator
    dataSet match
      case "basic" =>
        s"""1${s}Funky Koval${s}detective${s}01.03.1987${s}100.00
          |2${s}Eva Solo${s}lady${s}31.12.2012${s}0
          |3${s}Han Solo${s}hero${s}09.09.1999${s}999.99""".stripMargin
      case "wrong types" =>
        s"""1${s}Funky Koval${s}detective${s}01031987${s}100.00
          |two${s}Eva Solo${s}lady${s}31.12.2012${s}0
          |3${s}Han Solo${s}hero${s}09.09.1999${s}999-99""".stripMargin
      case "missing values" =>
        s""""1"${s}Funky Koval$s${s}01.01.2001$s
          |""${s}Eva Solo$s""${s}31.12.2012${s}123.45
          |"3"${s}Han Solo${s}hero${s}09.09.1999$s""""".stripMargin
      case "large" =>
        val row = largeRange.mkString(s.toString)
        (1 to 3).map(_ => row).mkString("\n")
