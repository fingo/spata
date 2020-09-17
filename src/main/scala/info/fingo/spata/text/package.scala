/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

/** Text parsing package. */
package object text {

  /** Convenience type. */
  type ValueOrError[A] = Either[DataParseException, A]
}
