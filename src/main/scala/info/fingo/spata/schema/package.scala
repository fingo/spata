/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import cats.data.Validated
import shapeless.HList

/** Schema validation package. */
package object schema {

  /** Type alias for typed record field key. */
  type Key = String with Singleton

  /** Type alias for validation result. */
  type ValidatedRecord[L <: HList] = Validated[InvalidRecord, TypedRecord[L]]
}
