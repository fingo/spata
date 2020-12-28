/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import scala.util.matching.Regex
import cats.data.Validated

trait Validator[A] {
  def apply(value: A): Validated[ValidationError, A]
}

object RegexValidator {
  def apply(regex: Regex): Validator[String] =
    (value: String) => Validated.valid(value).ensure(NotRegexConform)(regex.matches)
  def apply(regex: String): Validator[String] = apply(new Regex(regex))
}

object MinValidator {
  def apply[A: Ordering](min: A): Validator[A] =
    (value: A) => Validated.valid(value).ensure(ValueToSmall)(implicitly[Ordering[A]].lteq(min, _))
}

object MaxValidator {
  def apply[A: Ordering](max: A): Validator[A] =
    (value: A) => Validated.valid(value).ensure(ValueToLarge)(implicitly[Ordering[A]].gteq(max, _))
}

object MinMaxValidator {
  def apply[A: Numeric](min: A, max: A): Validator[A] = (value: A) => {
    val minV = MinValidator[A](min)
    val maxV = MaxValidator[A](max)
    minV(value).andThen(_ => maxV(value))
  }
}

object FiniteValidator {
  def apply(): Validator[Double] =
    (value: Double) => Validated.valid(value).ensure(NotFiniteNumber)(_.isFinite)
}
