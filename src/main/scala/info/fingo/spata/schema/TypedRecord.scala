/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import shapeless.HList
import shapeless.ops.record.Selector

class TypedRecord[+L <: HList](val data: L, val lineNum: Int, val rowNum: Int) {
  def apply[M >: L <: HList](key: StrSng)(implicit selector: Selector[M, key.type]): selector.Out = selector(data)
}

private[schema] object TypedRecord {
  def apply[L <: HList](data: L, lineNum: Int, rowNum: Int): TypedRecord[L] = new TypedRecord[L](data, lineNum, rowNum)
}
