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
import info.fingo.spata.schema.validator.{MinMaxValidator, MinValidator, RegexValidator, Validator}
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
        validated.leftMap { ir =>
          assert(ir.flaws.nonEmpty)
          ir.flaws.foreach { ff =>
            assert(ff.error.message.nonEmpty)
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
    val parser = CSVParser.config.get[IO]()
    val validated = csvStream("basic").through(parser.parse).through(schema.validate)
    val result = validated.compile.toList.unsafeRunSync()
    assert(result.length == 3)
    assert(result.forall(_.isValid))
    result.head.map { tr =>
      assert(tr("id") == 1)
    }
  }

  test("validation should support optional values") {
    val name: "name" = "name"
    val schema = CSVSchema()
      .add[String](name, RegexValidator(".+?"))
      .add[Option[Double]]("score")
    val parser = CSVParser.config.get[IO]()
    val validated = csvStream("empty values").through(parser.parse).through(schema.validate)
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

  test("validation against empty schema should be always correct") {
    val schema = CSVSchema()
    val parser = CSVParser.config.get[IO]()
    val dataSets = Seq("basic", "empty values")
    for (dataSet <- dataSets) yield {
      val validated = csvStream(dataSet).through(parser.parse).through(schema.validate)
      val result = validated.compile.toList.unsafeRunSync()
      assert(result.length == 3)
      result.foreach { validated =>
        assert(validated.isValid)
        validated.map(typedRecord => assert(typedRecord.rowNum > 0))
      }
    }
  }

  private def validate(testData: TestCase) = {
    val (_, data, idValidators, nameValidators, occupationValidators, appearedValidators, scoreValidators) = testData
    val schema = CSVSchema()
      .add[Int]("id", idValidators: _*)
      .add[String]("name", nameValidators: _*)
      .add[String]("occupation", occupationValidators: _*)
      .add[LocalDate]("appeared", appearedValidators: _*)
      .add[Double]("score", scoreValidators: _*)
    val parser = CSVParser.config.get[IO]()
    val validated = csvStream(data).through(parser.parse).through(schema.validate)
    validated.compile.toList.unsafeRunSync()
  }

  private def csvStream(dataSet: String) = {
    val content = csvContent(dataSet)
    val source = Source.fromString(s"$header\n$content")
    reader[IO]().read(source)
  }

  private val header = s"id${separator}name${separator}occupation${separator}appeared${separator}score"

  private lazy val validCases: TestCaseTable = Table(
    ("testCase", "data", "idValidator", "nameValidator", "occupationValidator", "appearedValidator", "scoreValidator"),
    ("basic", "basic", Nil, List(RegexValidator("""\w*\s\wo.*""")), Nil, Nil, List(MinMaxValidator(0, 1000))),
    ("no validators", "basic", Nil, Nil, Nil, Nil, Nil)
  )

  private lazy val invalidCases: TestCaseTable = Table(
    ("testCase", "data", "idValidator", "nameValidator", "occupationValidator", "appearedValidator", "scoreValidator"),
    ("basic", "basic", List(MinValidator(2)), List(RegexValidator("Fun.*")), Nil, Nil, List(MinMaxValidator(100, 200))),
    ("empty", "empty values", Nil, Nil, Nil, Nil, Nil)
  )

  private def csvContent(dataSet: String): String = {
    val s = separator
    dataSet match {
      case "basic" =>
        s"""1${s}Funky Koval${s}detective${s}01.03.1987${s}100.00
           |2${s}Eva Solo${s}lady${s}31.12.2012${s}0
           |3${s}Han Solo${s}hero${s}09.09.1999${s}999.99""".stripMargin
      case "empty values" =>
        s""""1"${s}Funky Koval$s${s}01.01.2001$s
           |""${s}Eva Solo$s""${s}31.12.2012${s}123.45
           |"3"${s}Han Solo${s}hero${s}09.09.1999$s""""".stripMargin
    }
  }
}
