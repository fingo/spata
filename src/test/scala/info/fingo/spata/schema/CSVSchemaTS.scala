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
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor7}
import info.fingo.spata.io.reader
import info.fingo.spata.CSVParser
import info.fingo.spata.schema.validator.{MaxValidator, MinMaxValidator, MinValidator, RegexValidator, Validator}
import info.fingo.spata.text.StringParser

class CSVSchemaTS extends AnyFunSuite with TableDrivenPropertyChecks {

  private val separator = ','

  type LV[A] = List[Validator[A]]
  type TestCase = (String, String, LV[Int], LV[String], LV[String], LV[LocalDate], LV[Double])
  type TestCaseTable = TableFor7[String, String, LV[Int], LV[String], LV[String], LV[LocalDate], LV[Double]]

  implicit val dateParser: StringParser[LocalDate] =
    (str: String) => LocalDate.parse(str.strip, DateTimeFormatter.ofPattern("dd.MM.yyyy"))

  test("validation should positively verify data compliant with schema") {
    val id: "id" = "id"
    for (testData <- validCases) yield {
      val result = validate(testData)
      assert(result.length == 3)
      result.foreach { validated =>
        assert(validated.isValid)
        validated.map { typedRecord =>
          val knownFrom: LocalDate = typedRecord("appeared")
          assert(typedRecord(id) > 0)
          assert(knownFrom.getYear < 2020)
          assert(typedRecord("score") >= 0)
        }
      }
    }
  }

  test("validation should provide errors for data not compliant with schema") {
    for (testData <- invalidCases) yield {
      val result = validate(testData)
      assert(result.length == 3)
      result.foreach { validated =>
        assert(validated.isInvalid)
        validated.leftMap { invalidRecord =>
          assert(invalidRecord.flaws.nonEmpty)
          invalidRecord.flaws.foreach { fieldFlaw =>
            assert(fieldFlaw.error.message.nonEmpty)
            val ec = fieldFlaw.error.code
            assert(ec.endsWith("Validator") || ec.endsWith("Type"))
          }
        }
      }
    }
  }

  test("it should be possible to convert typed record to case class after validation") {
    case class FullData(id: Int, name: String, occupation: String, appeared: LocalDate, score: Double)
    case class Person(id: Int, score: Double, name: String)
    val id: "id" = "id"
    for (testData <- validCases) yield {
      val result = validate(testData)
      result.foreach { validated =>
        validated.map { typedRecord =>
          val fd = typedRecord.to[FullData]()
          assert(fd.id == typedRecord(id))
          assert(fd.score >= 0)
          val person = typedRecord.to[Person]()
          assert(person.id == typedRecord(id))
          assert(person.name == typedRecord("name"))
        }
      }
    }
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
      .add[Option[Double]]("score")
    val validated = recordStream("missing values").through(schema.validate)
    val result = validated.compile.toList.unsafeRunSync()
    assert(result.length == 3)
    result.foreach { validated =>
      assert(validated.isValid)
      validated.map { typedRecord =>
        assert(typedRecord(name).length > 0)
        assert(typedRecord("score").forall(_ >= 0))
      }
    }
  }

  test("validation against empty schema should be always correct") {
    val schema = CSVSchema()
    for (dataSet <- dataSets) yield {
      val validated = recordStream(dataSet).through(schema.validate)
      val result = validated.compile.toList.unsafeRunSync()
      assert(result.length == 3)
      result.foreach { validated =>
        assert(validated.isValid)
        validated.map(typedRecord => assert(typedRecord.rowNum > 0))
      }
    }
  }

  test("validation with non-existent field should yield invalid for all records") {
    val schema = CSVSchema()
      .add[String]("name")
      .add[String]("nonexistent")
    for (dataSet <- dataSets) yield {
      val validated = recordStream(dataSet).through(schema.validate)
      val result = validated.compile.toList.unsafeRunSync()
      assert(result.length == 3)
      result.foreach { validated =>
        assert(validated.isInvalid)
        validated.leftMap { invalidRecord =>
          assert(invalidRecord.record.rowNum > 0)
          invalidRecord.flaws.foreach { fieldFlaw =>
            assert(fieldFlaw.error.message.nonEmpty)
            assert(fieldFlaw.error.code.endsWith("Key"))
          }
        }
      }
    }
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

  private def validate(testData: TestCase) = {
    val (_, data, idValidators, nameValidators, occupationValidators, appearedValidators, scoreValidators) = testData
    val schema = CSVSchema()
      .add[Int]("id", idValidators: _*)
      .add[String]("name", nameValidators: _*)
      .add[String]("occupation", occupationValidators: _*)
      .add[LocalDate]("appeared", appearedValidators: _*)
      .add[Double]("score", scoreValidators: _*)
    val validated = recordStream(data).through(schema.validate)
    validated.compile.toList.unsafeRunSync()
  }

  private def recordStream(dataSet: String) = {
    val content = csvContent(dataSet)
    val source = Source.fromString(s"$header\n$content")
    val parser = CSVParser.config.get[IO]()
    reader[IO]().read(source).through(parser.parse)
  }

  private val header = s"id${separator}name${separator}occupation${separator}appeared${separator}score"

  private lazy val validCases: TestCaseTable = Table(
    ("testCase", "data", "idValidator", "nameValidator", "occupationValidator", "appearedValidator", "scoreValidator"),
    ("basic", "basic", Nil, List(RegexValidator("""\w*\s\wo.*""")), Nil, Nil, List(MinMaxValidator(0, 1000))),
    ("multiple", "basic", Nil, Nil, Nil, Nil, List(MinValidator(0), MaxValidator(1000))),
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
      List(MinMaxValidator(100, 200))
    ),
    ("multiple first", "basic", Nil, Nil, Nil, Nil, List(MinValidator(1000), MaxValidator(1000))),
    ("multiple last", "basic", Nil, Nil, Nil, Nil, List(MinValidator(0), MaxValidator(-1))),
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

  lazy val dataSets = Seq("basic", "wrong types", "missing values")

  private def csvContent(dataSet: String): String = {
    val s = separator
    dataSet match {
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
    }
  }
}
