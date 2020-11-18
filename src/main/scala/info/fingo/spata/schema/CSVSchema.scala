/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import scala.util.matching.Regex
import cats.data.Validated
import cats.effect.Sync
import fs2.{Pipe, Stream}
import info.fingo.spata.Record
import info.fingo.spata.error.ContentError
import info.fingo.spata.text.StringParser
import info.fingo.spata.util.Logger

abstract class ValidationError(val message: String) {
  // TODO: extract and share with ParsingErrorCode
  def code: String = {
    val name = this.getClass.getSimpleName.stripSuffix("$")
    val first = name.take(1)
    name.replaceFirst(first, first.toLowerCase)
  }
}

case class NotParsed(override val message: String, cause: ContentError) extends ValidationError(message)
case object Unknown extends ValidationError("Unknown validation error")
case object ValueToSmall extends ValidationError("Value to small")
case object ValueToLarge extends ValidationError("Value to large")
case object NotRegexConform extends ValidationError("Value not conforming to regex")

class ValidatedRecord(val record: Record, val errors: List[ValidationError]) {
  def isValid: Boolean = errors.isEmpty
}

class CSVSchema[F[_]: Sync: Logger](columns: List[Column[_]]) {
  def validate: Pipe[F, Record, ValidatedRecord] =
    (in: Stream[F, Record]) =>
      in.map { r =>
        val validated =
          columns.map(_.validate(r)).filter(_.isInvalid).map(_.fold[ValidationError](identity, _ => Unknown))
        new ValidatedRecord(r, validated)
      }
}
object CSVSchema {
  def apply[F[_]: Sync: Logger](columns: List[Column[_]]) = new CSVSchema(columns)
}

class Column[A: StringParser](val name: String, val validators: Seq[Validator[A]]) {
  def validate(record: Record): Validated[ValidationError, A] = {
    val typeValidated: Validated[ValidationError, A] =
      Validated.fromEither(record.get(name)).leftMap(e => NotParsed(e.messageCode, e))
    typeValidated.andThen { v =>
      validators.map(_.validate(v)).foldLeft(typeValidated)((prev, curr) => prev.andThen(_ => curr))
    }
  }
}
object Column {
  def apply[A: StringParser](name: String): Column[A] = new Column(name, Nil)
  def apply[A: StringParser](name: String, validators: Seq[Validator[A]]): Column[A] = new Column(name, validators)
}

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
