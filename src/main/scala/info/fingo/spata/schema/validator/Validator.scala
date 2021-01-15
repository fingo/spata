/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema.validator

import info.fingo.spata.schema.error.ValidationError
import info.fingo.spata.util.classLabel
import scala.util.matching.Regex

trait Validator[A] {

  def isValid(value: A): Boolean

  def errorMessage(value: A) = s"Invalid value [$value] reported by ${classLabel(vld)}"

  final private[schema] def apply(value: A): Option[ValidationError] =
    if (isValid(value)) None
    else Some(ValidationError(vld, errorMessage(value)))

  protected[validator] val vld: Validator[_] = this
}

object Validator {
  implicit def option[A](validator: Validator[A]): Validator[Option[A]] = new Validator[Option[A]] {
    def isValid(value: Option[A]): Boolean = value.forall(validator.isValid)

    override protected[validator] val vld: Validator[_] = validator
  }
}

object RegexValidator {
  def apply(regex: Regex): Validator[String] = (value: String) => regex.matches(value)
  def apply(regex: String): Validator[String] = apply(new Regex(regex))
}

object MinValidator {
  def apply[A: Ordering](min: A): Validator[A] = new Validator[A] {
    def isValid(value: A): Boolean = implicitly[Ordering[A]].lteq(min, value)
    override def errorMessage(value: A): String = s"Value [$value] is to small"
  }
}

object MaxValidator {
  def apply[A: Ordering](max: A): Validator[A] = new Validator[A] {
    def isValid(value: A): Boolean = implicitly[Ordering[A]].gteq(max, value)
    override def errorMessage(value: A): String = "Value [$v] is to large"
  }
}

object MinMaxValidator {
  def apply[A: Ordering](min: A, max: A): Validator[A] = (value: A) => {
    val minV = MinValidator[A](min)
    val maxV = MaxValidator[A](max)
    minV.isValid(value) && maxV.isValid(value)
  }
}

object FiniteValidator {
  def apply(): Validator[Double] = new Validator[Double] {
    def isValid(value: Double): Boolean = value.isFinite
    override def errorMessage(value: Double) = s"Number [$value] is not finite"
  }
}
