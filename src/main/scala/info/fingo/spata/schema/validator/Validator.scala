/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema.validator

import cats.data.Validated
import info.fingo.spata.schema.error._

import scala.util.matching.Regex

trait Validator[A] {
  def apply(value: A): Validated[ValidationError, A]
}

object RegexValidator {
  private def error(v: String) = ValidationError(this, s"Value [$v] does not conform to regex")
  def apply(regex: Regex): Validator[String] =
    (value: String) => Validated.valid(value).ensure(error(value))(regex.matches)
  def apply(regex: String): Validator[String] = apply(new Regex(regex))
}

object MinValidator {
  private def error[A](v: A) = ValidationError(this, s"Value [$v] is to small")
  def apply[A: Ordering](min: A): Validator[A] =
    (value: A) => Validated.valid(value).ensure(error(value))(implicitly[Ordering[A]].lteq(min, _))
}

object MaxValidator {
  private def error[A](v: A) = ValidationError(this, s"Value [$v] is to large")
  def apply[A: Ordering](max: A): Validator[A] =
    (value: A) => Validated.valid(value).ensure(error(value))(implicitly[Ordering[A]].gteq(max, _))
}

object MinMaxValidator {
  def apply[A: Numeric](min: A, max: A): Validator[A] = (value: A) => {
    val minV = MinValidator[A](min)
    val maxV = MaxValidator[A](max)
    minV(value).andThen(_ => maxV(value))
  }
}

object FiniteValidator {
  private def error(v: Double) = ValidationError(this, s"Number [$v] is not finite")
  def apply(): Validator[Double] =
    (value: Double) => Validated.valid(value).ensure(error(value))(_.isFinite)
}
