/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import java.time.LocalDate
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import info.fingo.spata.schema.validator._

class ValidatorTS extends AnyFunSuite {

  test("it is possible to validate string with regular expressions") {
    val validator = RegexValidator("""[a-z]+\.[a-z]+""")
    assert(validator.name == "regexValidator")
    val valueOK = "fingo.info"
    val valueBad = ".net"
    assertOK(validator, valueOK)
    assertBad(validator, valueBad)
  }

  test("it is possible to validate numbers with min and max values") {
    val validatorMin = MinValidator(0)
    val validatorMax = MaxValidator(100)
    val validatorMinMax = MinMaxValidator(0, 100)
    assert(validatorMin.name == "minValidator")
    assert(validatorMax.name == "maxValidator")
    assert(validatorMinMax.name == "minMaxValidator")
    val valuesOK = Seq(0, 50, 100)
    for (v <- valuesOK) {
      assertOK(validatorMin, v)
      assertOK(validatorMax, v)
      assertOK(validatorMinMax, v)
    }
    val valuesSmall = Seq(-1, Integer.MIN_VALUE)
    for (v <- valuesSmall) {
      assertBad(validatorMin, v)
      assertOK(validatorMax, v)
      assertBad(validatorMinMax, v)
    }
    val valuesLarge = Seq(101, Integer.MAX_VALUE)
    for (v <- valuesLarge) {
      assertOK(validatorMin, v)
      assertBad(validatorMax, v)
      assertBad(validatorMinMax, v)
    }
  }

  test("it is possible to validate dates with min and max values") {
    val minDate = LocalDate.of(2000, 1, 1)
    val maxDate = LocalDate.of(2020, 12, 31)
    val validatorMin = MinValidator(minDate)
    val validatorMax = MaxValidator(maxDate)
    val validatorMinMax = MinMaxValidator(minDate, maxDate)
    assert(validatorMin.name == "minValidator")
    assert(validatorMax.name == "maxValidator")
    assert(validatorMinMax.name == "minMaxValidator")
    val valuesOK = Seq(minDate, minDate.plusDays(1), maxDate)
    for (v <- valuesOK) {
      assertOK(validatorMin, v)
      assertOK(validatorMax, v)
      assertOK(validatorMinMax, v)
    }
    val valuesSmall = Seq(minDate.minusDays(1), LocalDate.MIN)
    for (v <- valuesSmall) {
      assertBad(validatorMin, v)
      assertOK(validatorMax, v)
      assertBad(validatorMinMax, v)
    }
    val valuesLarge = Seq(maxDate.plusDays(1), LocalDate.MAX)
    for (v <- valuesLarge) {
      assertOK(validatorMin, v)
      assertBad(validatorMax, v)
      assertBad(validatorMinMax, v)
    }
  }

  test("it is possible to validate if double is finite") {
    val validator = FiniteValidator()
    assert(validator.name == "finiteValidator")
    val valuesOK = Seq(0.0, 3.14, -273.15, 6.626e-34, Double.MinPositiveValue, Double.MaxValue, Double.MinValue)
    val valuesBad = Seq(Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity)
    for (v <- valuesOK)
      assertOK(validator, v)
    for (v <- valuesBad)
      assertBad(validator, v)
  }

  test("it is possible to validate optional values") {
    val validator = MinMaxValidator(0.0, 100.0)
    assert(validator.name == "minMaxValidator")
    val valuesOK = Seq(Some(0.0), Some(50.0), Some(100.0), None)
    val valuesBad = Seq(Some(-1.0), Some(101.0), Some(Double.MaxValue), Some(Double.NaN), Some(Double.NegativeInfinity))
    for (v <- valuesOK)
      assertOptOK(validator, v)
    for (v <- valuesBad)
      assertOptBad(validator, v)
  }

  private def assertOK[A](validator: Validator[A], value: A): Assertion = {
    val validated = validator(value)
    assert(validated.isValid)
    assert(validated.exists(_ == value))
  }

  private def assertBad[A](validator: Validator[A], value: A): Assertion = {
    val validated = validator(value)
    assert(validated.isInvalid)
    assert(validated.fold(_.code, _ => "") == validator.name)
  }

  private def assertOptOK[A](validator: Validator[A], value: Option[A]): Assertion = {
    val validated = validator(value)
    assert(validated.isValid)
    assert(validated.exists(_ == value))
  }

  private def assertOptBad[A](validator: Validator[A], value: Option[A]): Assertion = {
    val validated = validator(value)
    assert(validated.isInvalid)
    assert(validated.fold(_.code, _ => "") == validator.name)
  }
}
