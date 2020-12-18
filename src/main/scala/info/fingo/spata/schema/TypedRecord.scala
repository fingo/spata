/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import shapeless.{::, HList, HNil, LabelledGeneric, Lazy}
import shapeless.labelled.{field, FieldType}
import shapeless.ops.record.Selector
import shapeless.tag.@@
import info.fingo.spata.schema.TypedRecord.ToProduct

class TypedRecord[+L <: HList](val data: L, val lineNum: Int, val rowNum: Int) {
  def apply[M >: L <: HList](key: StrSng)(implicit selector: Selector[M, key.type]): selector.Out = selector(data)
  def to[P <: Product]: ToProduct[P, L] = new ToProduct[P, L](data)
}

private[schema] object TypedRecord {
  def apply[L <: HList](data: L, lineNum: Int, rowNum: Int): TypedRecord[L] = new TypedRecord[L](data, lineNum, rowNum)

  class ToProduct[P <: Product, +L <: HList](data: L) {
    def apply[M >: L <: HList, R <: HList]()(implicit lg: LabelledGeneric.Aux[P, R], tr: ToRepr[M, R]): P =
      lg.from(tr(data))
  }
}

trait ToRepr[L <: HList, R <: HList] {
  def apply(in: L): R
}

object ToRepr {
  implicit def toHNilRepr[L <: HList]: ToRepr[L, HNil] = (_: L) => HNil

  implicit def toHConsRepr[L <: HList, K, V, TR <: HList](
    implicit sel: Selector.Aux[L, K, V],
    tailRepr: Lazy[ToRepr[L, TR]]
  ): ToRepr[L, FieldType[@@[Symbol, K], V] :: TR] = (l: L) => {
    val head = field[@@[Symbol, K]][V](sel(l))
    head :: tailRepr.value(l)
  }
}
