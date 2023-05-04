/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema.validator

import java.time.LocalDate
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

class ValidatorTS extends AnyFunSuite:

  test("it is possible to validate if value is exactly as expected"):
    val validatorDecimal = ExactValidator(BigDecimal(0.0))
    val validatorDate = ExactValidator(LocalDate.of(2000, 1, 1))
    val validatorString = ExactValidator("FINGO")
    assert(validatorDecimal.name == "exactValidator")
    assert(validatorDate.name == "exactValidator")
    assert(validatorString.name == "exactValidator")
    assertOK(validatorDecimal, BigDecimal.valueOf(0))
    assertBad(validatorDecimal, BigDecimal.valueOf(1))
    assertOK(validatorDate, LocalDate.ofYearDay(2000, 1))
    assertBad(validatorDate, LocalDate.ofYearDay(2000, 2))
    assertOK(validatorString, "FINGO")
    assertBad(validatorString, "fingo")

  test("it is possible to validate if value is contained in a set"):
    val validatorInt = OneOfValidator(1, 2, 3, 5, 8, 13, 21)
    val validatorString = OneOfValidator("lorem", "ipsum", "dolor", "sit", "amet")
    assert(validatorInt.name == "oneOfValidator")
    assert(validatorString.name == "oneOfValidator")
    val iValuesOK = Seq(1, 5, 21)
    val sValuesOK = Seq("lorem", "dolor", "amet")
    for v <- iValuesOK do assertOK(validatorInt, v)
    for v <- sValuesOK do assertOK(validatorString, v)
    val iValuesBad = Seq(0, 4, 15, Int.MaxValue, -1)
    val sValuesBad = Seq("LOREM", " ipsum", "dolor ", "Sit", "", "?")
    for v <- iValuesBad do assertBad(validatorInt, v)
    for v <- sValuesBad do assertBad(validatorString, v)

  test("it is possible to validate if string matches a set of values"):
    val validator = StringsValidator("lorem", "ipsum", "dolor", "sit", "amet", "dźwięk")
    assert(validator.name == "stringsValidator")
    val valuesOK = Seq("lorem", "IPSUM", " dolor", " Sit ", "ameT ", "DŹWIĘK")
    for v <- valuesOK do assertOK(validator, v)
    val valuesBad = Seq("LO REM", " ippsum", "我思故我在 ", "łódź", "", "?")
    for v <- valuesBad do assertBad(validator, v)

  test("it is possible to validate string length"):
    val validatorMinL = MinLenValidator(3)
    val validatorMaxL = MaxLenValidator(8)
    val validatorLengthR = LengthValidator(3, 8)
    val validatorLength = LengthValidator(3)
    assert(validatorMinL.name == "minLenValidator")
    assert(validatorMaxL.name == "maxLenValidator")
    assert(validatorLengthR.name == "lengthValidator")
    val valuesOK = Seq("123", "12345678", "  12345678 ", "FINGO", "1 2", "łódź", "我思故我在")
    for v <- valuesOK do
      assertOK(validatorMinL, v)
      assertOK(validatorMaxL, v)
      assertOK(validatorLengthR, v)
    val valuesSmall = Seq("", "   ", "12", "  12 ", "F")
    for v <- valuesSmall do
      assertBad(validatorMinL, v)
      assertOK(validatorMaxL, v)
      assertBad(validatorLengthR, v)
    val valuesLarge = Seq("123456789", "1 2 3 4 5", "fingo.info")
    for v <- valuesLarge do
      assertOK(validatorMinL, v)
      assertBad(validatorMaxL, v)
      assertBad(validatorLengthR, v)
    val valuesExactOK = Seq("123", "1 2", " 123 ", "FIN")
    for v <- valuesExactOK do assertOK(validatorLength, v)
    val valuesExactBad = Seq("1234", "12", "1 2 3", "12 ", "F")
    for v <- valuesExactBad do assertBad(validatorLength, v)

  test("it is possible to validate string with regular expression"):
    val validator = RegexValidator("""[a-z]+\.[a-z]+""")
    assert(validator.name == "regexValidator")
    val valueOK = "fingo.info"
    val valueBad = ".net"
    assertOK(validator, valueOK)
    assertBad(validator, valueBad)

  test("it is possible to validate number with min and max values"):
    val validatorMin = MinValidator(0)
    val validatorMax = MaxValidator(100)
    val validatorRange = RangeValidator(0, 100)
    assert(validatorMin.name == "minValidator")
    assert(validatorMax.name == "maxValidator")
    assert(validatorRange.name == "rangeValidator")
    val valuesOK = Seq(0, 50, 100)
    for v <- valuesOK do
      assertOK(validatorMin, v)
      assertOK(validatorMax, v)
      assertOK(validatorRange, v)
    val valuesSmall = Seq(-1, Integer.MIN_VALUE)
    for v <- valuesSmall do
      assertBad(validatorMin, v)
      assertOK(validatorMax, v)
      assertBad(validatorRange, v)
    val valuesLarge = Seq(101, Integer.MAX_VALUE)
    for v <- valuesLarge do
      assertOK(validatorMin, v)
      assertBad(validatorMax, v)
      assertBad(validatorRange, v)

  test("it is possible to validate date with min and max values"):
    val minDate = LocalDate.of(2000, 1, 1)
    val maxDate = LocalDate.of(2020, 12, 31)
    val validatorMin = MinValidator(minDate)
    val validatorMax = MaxValidator(maxDate)
    val validatorRange = RangeValidator(minDate, maxDate)
    assert(validatorMin.name == "minValidator")
    assert(validatorMax.name == "maxValidator")
    assert(validatorRange.name == "rangeValidator")
    val valuesOK = Seq(minDate, minDate.plusDays(1), maxDate)
    for v <- valuesOK do
      assertOK(validatorMin, v)
      assertOK(validatorMax, v)
      assertOK(validatorRange, v)
    val valuesSmall = Seq(minDate.minusDays(1), LocalDate.MIN)
    for v <- valuesSmall do
      assertBad(validatorMin, v)
      assertOK(validatorMax, v)
      assertBad(validatorRange, v)
    val valuesLarge = Seq(maxDate.plusDays(1), LocalDate.MAX)
    for v <- valuesLarge do
      assertOK(validatorMin, v)
      assertBad(validatorMax, v)
      assertBad(validatorRange, v)

  test("it is possible to validate if double is finite"):
    val validator = FiniteValidator()
    assert(validator.name == "finiteValidator")
    val valuesOK = Seq(0.0, 3.14, -273.15, 6.626e-34, Double.MinPositiveValue, Double.MaxValue, Double.MinValue)
    val valuesBad = Seq(Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity)
    for v <- valuesOK do assertOK(validator, v)
    for v <- valuesBad do assertBad(validator, v)

  test("it is possible to validate optional values"):
    val validator = RangeValidator(0.0, 100.0)
    assert(validator.name == "rangeValidator")
    val valuesOK = Seq(Some(0.0), Some(50.0), Some(100.0), None)
    val valuesBad = Seq(Some(-1.0), Some(101.0), Some(Double.MaxValue), Some(Double.NaN), Some(Double.NegativeInfinity))
    for v <- valuesOK do assertOptOK(validator, v)
    for v <- valuesBad do assertOptBad(validator, v)

  private def assertOK[A](validator: Validator[A], value: A): Assertion =
    val validated = validator(value)
    assert(validated.isValid)
    assert(validated.exists(_ == value))

  private def assertBad[A](validator: Validator[A], value: A): Assertion =
    val validated = validator(value)
    assert(validated.isInvalid)
    assert(validated.fold(_.code, _ => "") == validator.name)

  private def assertOptOK[A](validator: Validator[Option[A]], value: Option[A]): Assertion =
    val validated = validator(value)
    assert(validated.isValid)
    assert(validated.exists(_ == value))

  private def assertOptBad[A](validator: Validator[Option[A]], value: Option[A]): Assertion =
    val validated = validator(value)
    assert(validated.isInvalid)
    assert(validated.fold(_.code, _ => "") == validator.name)
