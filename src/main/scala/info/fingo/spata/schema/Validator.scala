/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import scala.util.matching.Regex
import cats.data.Validated

trait Validator[A] {
  def validate(value: A): Validated[ValidationError, A]
}

class RegexValidator(regex: Regex) extends Validator[String] {
  def validate(value: String): Validated[ValidationError, String] =
    Validated.valid(value).ensure[ValidationError](NotRegexConform)(regex.matches)
}
object RegexValidator {
  def apply(regex: Regex): RegexValidator = new RegexValidator(regex)
  def apply(regex: String): RegexValidator = new RegexValidator(new Regex(regex))
}

class MinValidator[A: Ordering](min: A) extends Validator[A] {
  def validate(value: A): Validated[ValidationError, A] =
    Validated.valid(value).ensure[ValidationError](ValueToSmall)(implicitly[Ordering[A]].lteq(min, _))
}
object MinValidator {
  def apply[A: Ordering](min: A): MinValidator[A] = new MinValidator(min)
}

class MaxValidator[A: Ordering](max: A) extends Validator[A] {
  def validate(value: A): Validated[ValidationError, A] =
    Validated.valid(value).ensure[ValidationError](ValueToLarge)(implicitly[Ordering[A]].gteq(max, _))
}
object MaxValidator {
  def apply[A: Ordering](max: A): MaxValidator[A] = new MaxValidator(max)
}

class MinMaxValidator[A: Ordering](min: A, max: A) extends Validator[A] {
  def validate(value: A): Validated[ValidationError, A] = {
    val minV = MinValidator[A](min)
    val maxV = MaxValidator[A](max)
    minV.validate(value).andThen(_ => maxV.validate(value))
  }
}
object MinMaxValidator {
  def apply[A: Numeric](min: A, max: A): MinMaxValidator[A] = new MinMaxValidator(min, max)
}
