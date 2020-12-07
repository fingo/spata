/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import cats.data.Validated

package object schema {
  type StrSng = String with Singleton
  type VF[A] = Validated[FieldFlaw, A]
  type VR[A] = Validated[InvalidRecord, TypedRecord[A]]
}
