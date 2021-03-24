/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.converter

import info.fingo.spata.RecordBuilder
import info.fingo.spata.text.StringRenderer
import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, Lazy, Witness}

/** Converter from specific [[shapeless.HList]] to Record through RecordBuilder.
  *
  * This trait defines behavior to be implemented by concrete, implicit converters.
  *
  * @tparam L type of source `HList`
  */
trait RecordFromHList[L <: HList] {

  /** Converts [[shapeless.HList]] to Record through RecordBuilder.
    *
    * @param hList the source `HList` to be converted
    * @return
    */
  def apply(hList: L): RecordBuilder
}

/** Implicits to convert [[shapeless.HNil]] and [[shapeless.::]] to RecordBuilder. */
object RecordFromHList {

  /** Converter from [[shapeless.HNil]] */
  implicit val fromHNil: RecordFromHList[HNil] = _ => RecordBuilder()

  /** Converter from [[shapeless.::]].
    *
    * @param witness carrier of field information (name as singleton type)
    * @param renderer renderer from typed value to string
    * @param rFromHL converter for [[shapeless.HList]] tail
    * @tparam K singleton type representing field name being converted from cons
    * @tparam V type of single `HList` element which value is being rendered
    * @tparam T type of `HList` tail
    * @return converter from `HList` to RecordBuilder
    */
  implicit def fromHCons[K <: Symbol, V, T <: HList](
    implicit witness: Witness.Aux[K],
    renderer: StringRenderer[V],
    rFromHL: Lazy[RecordFromHList[T]]
  ): RecordFromHList[FieldType[K, V] :: T] =
    hl => {
      val key = witness.value.name
      val value: V = hl.head
      rFromHL.value(hl.tail).add(key, value)
    }
}
