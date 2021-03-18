/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.converter

import info.fingo.spata.RecordBuilder
import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, Lazy, Witness}

trait RecordFromHList[L <: HList] {

  def apply(hlist: L): RecordBuilder
}

object RecordFromHList {

  implicit val fromHNil: RecordFromHList[HNil] = _ => new RecordBuilder()

  implicit def fromHCons[K <: Symbol, V, T <: HList](
    implicit witness: Witness.Aux[K],
    rFromHL: Lazy[RecordFromHList[T]]
  ): RecordFromHList[FieldType[K, V] :: T] =
    hlist => {
      val key = witness.value.name
      val value: V = hlist.head
      rFromHL.value(hlist.tail).add(key, value)
    }
}
