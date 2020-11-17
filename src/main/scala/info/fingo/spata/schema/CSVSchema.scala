/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import cats.data.Validated
import cats.effect.Sync
import fs2.{Pipe, Stream}
import info.fingo.spata.Record
import info.fingo.spata.text.StringParser
import info.fingo.spata.util.Logger

import scala.util.matching.Regex

class ValidatedRecord(val record: Record, val errors: List[String]) {
  def isValid: Boolean = errors.isEmpty
}

class CSVSchema[F[_]: Sync: Logger](columns: List[Column[_]]) {
  def validate: Pipe[F, Record, ValidatedRecord] =
    (in: Stream[F, Record]) =>
      in.map { r =>
        val validated = columns.map { c =>
          c.validate(r).fold[String](identity, _ => "")
        }.filter(_.nonEmpty)
        new ValidatedRecord(r, validated)
      }
}
object CSVSchema {
  def apply[F[_]: Sync: Logger](columns: List[Column[_]]) = new CSVSchema(columns)
}

class Column[A: StringParser](val name: String, val validators: Seq[Validator[A]]) {
  def validate(record: Record): Validated[String, A] = {
    val typeValidated = Validated.fromEither(record.get(name)).leftMap(_.getMessage)
    validators.foldLeft(typeValidated)((prev, curr) => curr.validate(prev))
  }
}
object Column {
  def apply[A: StringParser](name: String): Column[A] = new Column(name, Nil)
  def apply[A: StringParser](name: String, validators: Seq[Validator[A]]): Column[A] = new Column(name, validators)
}

trait Validator[A] {
  def validate(value: Validated[String, A]): Validated[String, A]
}

class RegexValidator(regex: Regex) extends Validator[String] {
  def validate(value: Validated[String, String]): Validated[String, String] =
    value.ensure[String]("Value not conforming to regex")(regex.matches)
}
object RegexValidator {
  def apply(regex: Regex): RegexValidator = new RegexValidator(regex)
  def apply(regex: String): RegexValidator = new RegexValidator(new Regex(regex))
}

class MinMaxValidator[A: Numeric](min: Option[A], max: Option[A]) extends Validator[A] {
  def validate(value: Validated[String, A]): Validated[String, A] =
    value
      .ensure[String]("Value to small")(v => min.forall(implicitly[Numeric[A]].lteq(_, v)))
      .ensure[String]("Value to large")(v => max.forall(implicitly[Numeric[A]].gteq(_, v)))
}
object MinMaxValidator {
  def apply[A: Numeric](): MinMaxValidator[A] = new MinMaxValidator(None, None)
  def apply[A: Numeric](min: A): MinMaxValidator[A] = new MinMaxValidator(Some(min), None)
  def apply[A: Numeric](min: A, max: A): MinMaxValidator[A] = new MinMaxValidator(Some(min), Some(max))
}
