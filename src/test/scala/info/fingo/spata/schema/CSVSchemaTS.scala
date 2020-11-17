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
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor5}
import info.fingo.spata.io.reader
import info.fingo.spata.CSVParser
import info.fingo.spata.text.StringParser
import org.scalatest.Assertion

class CSVSchemaTS extends AnyFunSuite with TableDrivenPropertyChecks {

  private val separator = ','

  type LV[A] = List[Validator[A]]
  type TestCaseTable = TableFor5[String, LV[Int], LV[String], LV[LocalDate], LV[Double]]

  implicit val dateParser: StringParser[LocalDate] =
    (str: String) => LocalDate.parse(str.strip, DateTimeFormatter.ofPattern("dd.MM.yyyy"))

  def testWrapper(testCases: TestCaseTable)(testCode: List[ValidatedRecord] => Assertion): Assertion =
    forAll(testCases) {
      (
        testCase: String,
        idValidators: List[Validator[Int]],
        nameValidators: List[Validator[String]],
        dateValidators: List[Validator[LocalDate]],
        valueValidators: List[Validator[Double]]
      ) =>
        val idColumn = Column[Int]("ID", idValidators)
        val nameColumn = Column[String]("NAME", nameValidators)
        val dateColumn = Column[LocalDate]("DATE", dateValidators)
        val valueColumn = Column[Double]("VALUE", valueValidators)
        val columns = List(idColumn, nameColumn, dateColumn, valueColumn)
        val schema = CSVSchema[IO](columns)
        val parser = CSVParser.config.get[IO]()
        val validated = csvStream(testCase).through(parser.parse).through(schema.validate)
        val result = validated.compile.toList.unsafeRunSync()
        testCode(result)
    }

  test("spata should positively validate data compliant with schema") {
    testWrapper(validCases) { result =>
      assert(result.length == 3)
      assert(result.forall(_.isValid))
    }
  }

  test("spata should provide validation errors for data not compliant with schema") {
    testWrapper(invalidCases) { result =>
      assert(result.length == 3)
      assert(result.forall(!_.isValid))
    }
  }

  private def csvStream(testCase: String) = {
    val content = csvContent(testCase)
    val source = Source.fromString(s"$header\n$content")
    reader[IO]().read(source)
  }

  private val header = s"ID${separator}NAME${separator}DATE${separator}VALUE"

  private lazy val validCases: TestCaseTable = Table(
    ("testCase", "idValidator", "nameValidator", "dateValidator", "valueValidator"),
    ("basic", List(MinMaxValidator()), List(RegexValidator("""\w*\s\wo.*""")), Nil, List(MinMaxValidator(0, 1000)))
  )

  private lazy val invalidCases: TestCaseTable = Table(
    ("testCase", "idValidator", "nameValidator", "dateValidator", "valueValidator"),
    ("basic", List(MinMaxValidator(2)), List(RegexValidator("Fun.*")), Nil, List(MinMaxValidator(100, 200)))
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
           |"2"${s}Eva Solo${s}31.12.2012${s}123.45
           |"3"$s""${s}09.09.1999$s""""".stripMargin
    }
  }
}
