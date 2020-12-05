/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import scala.util.matching.Regex
import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}
import cats.effect.Sync
import fs2.{Pipe, Stream}
import info.fingo.spata.Record
import info.fingo.spata.error.ContentError
import info.fingo.spata.text.StringParser
import info.fingo.spata.util.Logger
import shapeless.{::, DepFn2, HList, HNil}
import shapeless.labelled.{field, FieldType}

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

trait SchemaEnforcer[L <: HList] extends DepFn2[L, Record] {
  type Out <: VLE[HList]
}

object SchemaEnforcer {
  type Aux[I <: HList, O <: VLE[HList]] = SchemaEnforcer[I] { type Out = O }

  private val empty = Validated.valid[List[ValidationError], HNil](HNil)

  implicit val nil: Aux[HNil, VLE[HNil]] = new SchemaEnforcer[HNil] {
    override type Out = VLE[HNil]
    override def apply(columns: HNil, record: Record): Out = empty
  }

  implicit def cons[K <: StrSng, V, T <: HList, TTR <: HList](
    implicit tailEnforcer: Aux[T, VLE[TTR]]
  ): Aux[Column[K, V] :: T, VLE[FieldType[K, V] :: TTR]] =
    new SchemaEnforcer[Column[K, V] :: T] {
      override type Out = VLE[FieldType[K, V] :: TTR]
      override def apply(columns: Column[K, V] :: T, record: Record): Out = {
        val column = columns.head
        val validated = column.validate(record)
        val tailV = tailEnforcer(columns.tail, record)
        validated match {
          case Valid(vv) => tailV.map(vv :: _)
          case Invalid(iv) =>
            val errors = tailV match {
              case Valid(_) => Nil
              case Invalid(ivt) => ivt
            }
            Validated.invalid[List[ValidationError], FieldType[K, V] :: TTR](iv :: errors)
        }
      }
    }

  def enforce[L <: HList](columns: L, record: Record)(implicit enforcer: SchemaEnforcer[L]): enforcer.Out =
    enforcer(columns, record)
}

class CSVSchema[L <: HList: SchemaEnforcer](columns: L) {

  def validate[F[_]: Sync: Logger](implicit enforcer: SchemaEnforcer[L]): Pipe[F, Record, enforcer.Out] =
    (in: Stream[F, Record]) => in.map(r => validateRecord(r)(enforcer))

  private def validateRecord(r: Record)(implicit enforcer: SchemaEnforcer[L]): enforcer.Out = enforcer(columns, r)
}

object CSVSchema {
  def apply[L <: HList: SchemaEnforcer](columns: L) = new CSVSchema(columns)
}

class Column[K <: StrSng, V: StringParser](val name: K, val validators: Seq[Validator[V]]) {
  def validate(record: Record): Validated[ValidationError, FieldType[K, V]] = {
    val typeValidated: Validated[ValidationError, V] =
      Validated.fromEither(record.get(name)).leftMap(e => NotParsed(e.messageCode, e))
    val fullyValidated: Validated[ValidationError, V] = typeValidated.andThen { v =>
      validators.map(_.validate(v)).foldLeft(typeValidated)((prev, curr) => prev.andThen(_ => curr))
    }
    fullyValidated.map(field[K](_))
  }
}
object Column {
  def apply[A: StringParser](name: String): Column[name.type, A] = new Column(name, Nil)
  def apply[A: StringParser](name: String, validators: Seq[Validator[A]]): Column[name.type, A] =
    new Column(name, validators)
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
