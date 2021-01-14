/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema.validator

import info.fingo.spata.schema.error.ValidationError
import scala.util.matching.Regex

trait Validator[A] {
  def apply(value: A): Option[ValidationError]
}

object RegexValidator {
  private def error(v: String) = ValidationError(this, s"Value [$v] does not conform to regex")
  def apply(regex: Regex): Validator[String] =
    (value: String) => if (regex.matches(value)) None else Some(error(value))
  def apply(regex: String): Validator[String] = apply(new Regex(regex))
}

object MinValidator {
  private def error[A](v: A) = ValidationError(this, s"Value [$v] is to small")
  def apply[A: Ordering](min: A): Validator[A] =
    (value: A) => if (implicitly[Ordering[A]].lteq(min, value)) None else Some(error(value))
}

object MaxValidator {
  private def error[A](v: A) = ValidationError(this, s"Value [$v] is to large")
  def apply[A: Ordering](max: A): Validator[A] =
    (value: A) => if (implicitly[Ordering[A]].gteq(max, value)) None else Some(error(value))
}

object MinMaxValidator {
  def apply[A: Ordering](min: A, max: A): Validator[A] = (value: A) => {
    val minV = MinValidator[A](min)
    val maxV = MaxValidator[A](max)
    minV(value).orElse(maxV(value))
  }
}

object FiniteValidator {
  private def error(v: Double) = ValidationError(this, s"Number [$v] is not finite")
  def apply(): Validator[Double] =
    (value: Double) => if (value.isFinite) None else Some(error(value))
}
