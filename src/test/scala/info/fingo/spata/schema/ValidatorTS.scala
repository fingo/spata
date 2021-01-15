/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import info.fingo.spata.schema.validator.{FiniteValidator, MaxValidator, MinMaxValidator, MinValidator, RegexValidator}
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate

class ValidatorTS extends AnyFunSuite {

  test("it is possible to validate string with regular expressions") {
    val validator = RegexValidator("""[a-z]+\.[a-z]+""")
    val valueOK = "fingo.info"
    val valueBad = ".net"
    assert(validator(valueOK).isEmpty)
    assert(validator(valueBad).exists(_.code == "regexValidator"))
  }

  test("it is possible to validate numbers with min and max values") {
    val validatorMin = MinValidator(0)
    val validatorMax = MaxValidator(100)
    val validatorMinMax = MinMaxValidator(0, 100)
    val valuesOK = Seq(0, 50, 100)
    for (v <- valuesOK) {
      assert(validatorMin(v).isEmpty)
      assert(validatorMax(v).isEmpty)
      assert(validatorMinMax(v).isEmpty)
    }
    val valuesSmall = Seq(-1, Integer.MIN_VALUE)
    for (v <- valuesSmall) {
      assert(validatorMin(v).exists(_.code == "minValidator"))
      assert(validatorMax(v).isEmpty)
      assert(validatorMinMax(v).exists(_.code == "minMaxValidator"))
    }
    val valuesLarge = Seq(101, Integer.MAX_VALUE)
    for (v <- valuesLarge) {
      assert(validatorMin(v).isEmpty)
      assert(validatorMax(v).exists(_.code == "maxValidator"))
      assert(validatorMinMax(v).exists(_.code == "minMaxValidator"))
    }
  }

  test("it is possible to validate dates with min and max values") {
    val minDate = LocalDate.of(2000, 1, 1)
    val maxDate = LocalDate.of(2020, 12, 31)
    val validatorMin = MinValidator(minDate)
    val validatorMax = MaxValidator(maxDate)
    val validatorMinMax = MinMaxValidator(minDate, maxDate)
    val valuesOK = Seq(minDate, minDate.plusDays(1), maxDate)
    for (v <- valuesOK) {
      assert(validatorMin(v).isEmpty)
      assert(validatorMax(v).isEmpty)
      assert(validatorMinMax(v).isEmpty)
    }
    val valuesSmall = Seq(minDate.minusDays(1), LocalDate.MIN)
    for (v <- valuesSmall) {
      assert(validatorMin(v).exists(_.code == "minValidator"))
      assert(validatorMax(v).isEmpty)
      assert(validatorMinMax(v).exists(_.code == "minMaxValidator"))
    }
    val valuesLarge = Seq(maxDate.plusDays(1), LocalDate.MAX)
    for (v <- valuesLarge) {
      assert(validatorMin(v).isEmpty)
      assert(validatorMax(v).exists(_.code == "maxValidator"))
      assert(validatorMinMax(v).exists(_.code == "minMaxValidator"))
    }
  }

  test("it is possible to validate if double is finite") {
    val validator = FiniteValidator()
    val valuesOK = Seq(0.0, 3.14, -273.15, 6.626e-34, Double.MinPositiveValue, Double.MaxValue, Double.MinValue)
    val valuesBad = Seq(Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity)
    for (v <- valuesOK)
      assert(validator(v).isEmpty)
    for (v <- valuesBad)
      assert(validator(v).exists(_.code == "finiteValidator"))
  }

  test("it is possible to validate optional values") {
    val validator = MinMaxValidator(0.0, 100.0)
    val valuesOK = Seq(Some(0.0), Some(50.0), Some(100.0), None)
    val valuesBad = Seq(Some(-1.0), Some(101.0), Some(Double.MaxValue), Some(Double.NaN), Some(Double.NegativeInfinity))
    for (v <- valuesOK)
      assert(validator(v).isEmpty)
    for (v <- valuesBad)
      assert(validator(v).exists(_.code == "minMaxValidator"))
  }
}
