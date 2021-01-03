/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.converter

import shapeless.{::, HList, HNil}
import shapeless.labelled.{field, FieldType}
import shapeless.ops.record.Selector
import shapeless.tag.@@

/** Converter from record-like [[shapeless.HList]] of string-based [[shapeless.labelled.FieldType]]s
  * (`FieldType` key type is a singleton string) to symbol-like ones (`FieldType` key type is a [[scala.Symbol]])
  * as required by [[shapeless.LabelledGeneric]] representation for case classes.
  *
  * @tparam L type of source `HList`
  * @tparam R type of target `HList`
  */
trait RecHListToRepr[L <: HList, R <: HList] {

  /** Converts string-based to symbol-based heterogeneous list of [[shapeless.labelled.FieldType]]s.
    *
    * @param in the source `HList`
    * @return the result `HList`
    */
  def apply(in: L): R
}

/** Implicits to converter [[shapeless.HList]] from string to symbol-based fields. */
object RecHListToRepr {

  /** Converter to [[shapeless.HNil]].
    *
    * @tparam L type of source `HList`
    * @return converter from any HList to HNil
    */
  implicit def toHNilRepr[L <: HList]: RecHListToRepr[L, HNil] = (_: L) => HNil

  /** Converter to [[shapeless.::]].
    *
    * @param sel the selector for source list fields
    * @param tailRepr converter for tail of target list
    * @tparam L type of source `HList`
    * @tparam K type of source field key
    * @tparam V type of field value
    * @tparam TR type of tail of target list
    * @return recursive converter from source to target HList
    */
  implicit def toHConsRepr[L <: HList, K, V, TR <: HList](
    implicit sel: Selector.Aux[L, K, V],
    tailRepr: RecHListToRepr[L, TR]
  ): RecHListToRepr[L, FieldType[@@[Symbol, K], V] :: TR] = (l: L) => {
    val head = field[@@[Symbol, K]][V](sel(l))
    head :: tailRepr(l)
  }
}
