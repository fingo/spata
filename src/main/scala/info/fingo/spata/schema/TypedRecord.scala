/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import info.fingo.spata.schema.TypedRecord.ToProduct
import shapeless.labelled.{field, FieldType}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy}
import shapeless.ops.record.Selector

class TypedRecord[+L <: HList](val data: L, val lineNum: Int, val rowNum: Int) {
  def apply[M >: L <: HList](key: StrSng)(implicit selector: Selector[M, key.type]): selector.Out = selector(data)
  def to[P <: Product](implicit gen: LabelledGeneric[P]): ToProduct[P, L, gen.Repr] =
    new ToProduct[P, L, gen.Repr](data)
}

private[schema] object TypedRecord {
  def apply[L <: HList](data: L, lineNum: Int, rowNum: Int): TypedRecord[L] = new TypedRecord[L](data, lineNum, rowNum)

  class ToProduct[P <: Product, +L <: HList, R](data: L) {
    final def apply[M >: L <: HList](
      implicit gen: LabelledGeneric.Aux[P, R],
      kc: KeyConv.Aux[M, R]
    ): P = gen.from(convert[M])

    private def convert[M >: L <: HList](implicit kc: KeyConv.Aux[M, R]): R = KeyConv.convert[M](data)
  }
}

private[schema] trait KeyConv[L <: HList] {
  type Out

  def apply(in: L): Out
}

private[schema] object KeyConv {
  type Aux[L <: HList, O] = KeyConv[L] { type Out = O }
  implicit val toHNil: Aux[HNil, HNil] = new KeyConv[HNil] {
    override type Out = HNil
    override def apply(in: HNil): Out = HNil
  }

  implicit def toHCons[KI <: StrSng, KO <: Symbol, V, TI <: HList, TO <: HList](
    implicit sToSK: Lazy[Aux[TI, TO]]
  ): Aux[FieldType[KI, V] :: TI, FieldType[KO, V] :: TO] = new KeyConv[FieldType[KI, V] :: TI] {
    override type Out = FieldType[KO, V] :: TO
    override def apply(in: FieldType[KI, V] :: TI): Out = {
      val value: V = in.head
      val tail = sToSK.value.apply(in.tail)
      field[KO][V](value) :: tail
    }
  }

  def convert[L <: HList](in: L)(implicit kc: KeyConv[L]): kc.Out = kc(in)
}
