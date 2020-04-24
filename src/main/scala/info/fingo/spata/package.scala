/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo

import scala.util.Try

/** Spata primary package. */
package object spata {

  /** Convenience type. */
  type Maybe[A] = Either[Throwable, A]

  /** Convenience type. */
  type S2S = PartialFunction[String, String]

  /** Convenience function to wrap code in [[scala.util.Try]] and converter to [[scala.util.Either]]. */
  def maybe[A](code: => A): Maybe[A] = Try(code).toEither
}
