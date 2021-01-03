/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import scala.annotation.unused
import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}
import cats.effect.Sync
import fs2.{Pipe, Stream}
import shapeless.{::, =:!=, DepFn2, HList, HNil}
import shapeless.labelled.{field, FieldType}
import info.fingo.spata.Record
import info.fingo.spata.schema.error.NotParsed
import info.fingo.spata.schema.validator.Validator
import info.fingo.spata.text.StringParser
import info.fingo.spata.util.Logger

class CSVSchema[L <: HList] private (columns: L) {

  def add[V: StringParser](key: Key, validators: Validator[V]*)(
    implicit @unused ev: NotPresent[key.type, L]
  ): CSVSchema[Column[key.type, V] :: L] =
    new CSVSchema[Column[key.type, V] :: L](Column.apply[V](key, validators) :: columns)

  def add[V: StringParser](key: Key)(
    implicit @unused ev: NotPresent[key.type, L]
  ): CSVSchema[Column[key.type, V] :: L] =
    new CSVSchema[Column[key.type, V] :: L](Column.apply[V](key) :: columns)

  def validate[F[_]: Sync: Logger](implicit enforcer: SchemaEnforcer[L]): Pipe[F, Record, enforcer.Out] =
    (in: Stream[F, Record]) => in.map(r => validateRecord(r)(enforcer))

  private def validateRecord(r: Record)(implicit enforcer: SchemaEnforcer[L]): enforcer.Out = enforcer(columns, r)
}

object CSVSchema {
  def apply(): CSVSchema[HNil] = new CSVSchema[HNil](HNil)
}

class Column[K <: Key, V: StringParser](val name: K, val validators: Seq[Validator[V]]) {
  def validate(record: Record): Validated[FieldFlaw, FieldType[K, V]] = {
    val typeValidated =
      Validated.fromEither(record.get(name)).leftMap(e => FieldFlaw(name, NotParsed(e.messageCode, e)))
    val fullyValidated = typeValidated.andThen { v =>
      validators
        .map(_(v).leftMap(FieldFlaw(name, _)))
        .foldLeft(typeValidated)((prev, curr) => prev.andThen(_ => curr))
    }
    fullyValidated.map(field[K](_))
  }
}

object Column {
  def apply[A: StringParser](name: String): Column[name.type, A] = new Column(name, Nil)
  def apply[A: StringParser](name: String, validators: Seq[Validator[A]]): Column[name.type, A] =
    new Column(name, validators)
}

trait SchemaEnforcer[L <: HList] extends DepFn2[L, Record] {
  type Out <: Validated[InvalidRecord, TypedRecord[HList]]
}

object SchemaEnforcer {
  type Aux[I <: HList, O <: ValidatedRecord[HList]] = SchemaEnforcer[I] { type Out = O }

  private def empty(record: Record) =
    Validated.valid[InvalidRecord, TypedRecord[HNil]](TypedRecord(HNil, record.lineNum, record.rowNum))

  implicit val enforceHNil: Aux[HNil, ValidatedRecord[HNil]] = new SchemaEnforcer[HNil] {
    override type Out = ValidatedRecord[HNil]
    override def apply(columns: HNil, record: Record): Out = empty(record)
  }

  implicit def enforceHCons[K <: Key, V, T <: HList, TTR <: HList](
    implicit tailEnforcer: Aux[T, ValidatedRecord[TTR]]
  ): Aux[Column[K, V] :: T, ValidatedRecord[FieldType[K, V] :: TTR]] =
    new SchemaEnforcer[Column[K, V] :: T] {
      override type Out = ValidatedRecord[FieldType[K, V] :: TTR]
      override def apply(columns: Column[K, V] :: T, record: Record): Out = {
        val column = columns.head
        val validated = column.validate(record)
        val tailV = tailEnforcer(columns.tail, record)
        validated match {
          case Valid(vv) => tailV.map(tv => TypedRecord(vv :: tv.data, record.lineNum, record.rowNum))
          case Invalid(iv) =>
            val flaws = tailV match {
              case Valid(_) => Nil
              case Invalid(ivt) => ivt.flaws
            }
            Validated.invalid[InvalidRecord, TypedRecord[FieldType[K, V] :: TTR]](
              InvalidRecord(record, iv :: flaws)
            )
        }
      }
    }
}

trait NotPresent[K <: Key, L <: HList]

object NotPresent {

  implicit def notPresentHNil[K <: Key]: NotPresent[K, HNil] = new NotPresent[K, HNil] {}

  implicit def notPresentHCons[K <: Key, V, H <: Key, T <: HList](
    implicit @unused neq: K =:!= H,
    @unused tailNP: NotPresent[K, T]
  ): NotPresent[K, Column[H, V] :: T] =
    new NotPresent[K, Column[H, V] :: T] {}
}
