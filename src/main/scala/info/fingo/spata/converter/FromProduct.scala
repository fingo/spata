/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.converter

import info.fingo.spata.{Decoded, Record}

trait FromProduct[P <: Product]:
  def encode(p: P): Record

trait FromTuple[-T <: Tuple]:
  def encode(t: T): Record

object FromTuple:
  // FIXME: implement
  given fromTuple[T <: Tuple]: FromTuple[T] = ???

object FromProduct:
  // FIXME: implement
  given fromProduct[P <: Product]: FromProduct[P] = ???
