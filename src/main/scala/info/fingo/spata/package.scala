/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo

import info.fingo.spata.error.ContentError

/** Spata primary package. */
package object spata {

  /** Convenience type representing result of retrieving record data. */
  type Decoded[A] = Either[ContentError, A]

  /** Convenience type. */
  type S2S = PartialFunction[String, String]

  /** Convenience type. */
  type I2S = PartialFunction[Int, String]
}
