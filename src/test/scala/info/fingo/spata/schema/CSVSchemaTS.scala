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
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor6}
import info.fingo.spata.io.reader
import info.fingo.spata.CSVParser
import info.fingo.spata.text.StringParser

class CSVSchemaTS extends AnyFunSuite with TableDrivenPropertyChecks {

  private val separator = ','

  type LV[A] = List[Validator[A]]
  type TestCase = (String, String, LV[Int], LV[String], LV[LocalDate], LV[Double])
  type TestCaseTable = TableFor6[String, String, LV[Int], LV[String], LV[LocalDate], LV[Double]]

  implicit val dateParser: StringParser[LocalDate] =
    (str: String) => LocalDate.parse(str.strip, DateTimeFormatter.ofPattern("dd.MM.yyyy"))

  test("spata should positively validate data compliant with schema") {
    for (testData <- validCases) yield {
      val result = validate(testData)
      assert(result.length == 3)
      result.foreach { validated =>
        assert(validated.isValid)
        validated.map { typedRecord =>
          val id: "ID" = "ID"
          val date: LocalDate = typedRecord("DATE")
          assert(typedRecord(id) > 0)
          assert(date.getYear < 2020)
          assert(typedRecord("VALUE") > 0)
        }
      }
    }
  }

  test("spata should provide validation errors for data not compliant with schema") {
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
    case class Person(ID: Int, VALUE: Double, NAME: String)
    for (testData <- validCases) yield {
      val result = validate(testData)
      result.foreach { validated =>
        validated.map { typedRecord =>
          val person = typedRecord.to[Person]()
          val id: "ID" = "ID"
          assert(person.ID == typedRecord(id))
          assert(person.NAME == typedRecord("NAME"))
        }
      }
    }
  }

  test("It should be possible to validate types only") {
    val schema = CSVSchema.builder
      .add[Int]("ID")
      .add[String]("NAME")
      .add[LocalDate]("DATE")
      .add[Double]("VALUE")
      .build
    val parser = CSVParser.config.get[IO]()
    val validated = csvStream("basic").through(parser.parse).through(schema.validate)
    val result = validated.compile.toList.unsafeRunSync()
    assert(result.length == 3)
    assert(result.forall(_.isValid))
    result.head.map { tr =>
      assert(tr("ID") == 1)
    }
  }

  private def validate(testData: TestCase) = {
    val (_, data, idValidators, nameValidators, dateValidators, valueValidators) = testData
    val schema = CSVSchema.builder
      .add[Int]("ID", idValidators: _*)
      .add[String]("NAME", nameValidators: _*)
      .add[LocalDate]("DATE", dateValidators: _*)
      .add[Double]("VALUE", valueValidators: _*)
      .build
    val parser = CSVParser.config.get[IO]()
    val validated = csvStream(data).through(parser.parse).through(schema.validate)
    validated.compile.toList.unsafeRunSync()
  }

  private def csvStream(testCase: String) = {
    val content = csvContent(testCase)
    val source = Source.fromString(s"$header\n$content")
    reader[IO]().read(source)
  }

  private val header = s"ID${separator}NAME${separator}DATE${separator}VALUE"

  private lazy val validCases: TestCaseTable = Table(
    ("testCase", "data", "idValidator", "nameValidator", "dateValidator", "valueValidator"),
    ("basic", "basic", Nil, List(RegexValidator("""\w*\s\wo.*""")), Nil, List(MinMaxValidator(0, 1000))),
    ("no validators", "basic", Nil, Nil, Nil, Nil)
  )

  private lazy val invalidCases: TestCaseTable = Table(
    ("testCase", "data", "idValidator", "nameValidator", "dateValidator", "valueValidator"),
    ("basic", "basic", List(MinValidator(2)), List(RegexValidator("Fun.*")), Nil, List(MinMaxValidator(100, 200))),
    ("empty", "empty values", Nil, Nil, Nil, Nil)
  )

  private def csvContent(testCase: String): String = {
    val s = separator
    testCase match {
      case "basic" =>
        s"""1${s}Funky Koval${s}01.01.2001${s}100.00
           |2${s}Eva Solo${s}31.12.2012${s}123.45
           |3${s}Han Solo${s}09.09.1999${s}999.99""".stripMargin
      case "empty values" =>
        s""""1"$s${s}01.01.2001$s
           |""${s}Eva Solo${s}31.12.2012${s}123.45
           |"3"$s""${s}09.09.1999$s""""".stripMargin
    }
  }
}
