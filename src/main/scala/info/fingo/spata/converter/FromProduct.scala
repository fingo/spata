/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.converter

import info.fingo.spata.text.StringRenderer
import info.fingo.spata.{Decoded, Header, Record}

import scala.deriving.Mirror

trait FromProduct[P <: Product]:
  def encode(p: P): Record

trait FromTuple[-T <: Tuple]:
  def encode(t: T): Record = Record.fromValues(encodeRaw(t)*)
  def encodeRaw(t: T): List[String]

object FromTuple:
  given fromEmpty: FromTuple[EmptyTuple] with
    def encodeRaw(et: EmptyTuple): List[String] = Nil
  given fromCons[H: StringRenderer, T <: Tuple: FromTuple]: FromTuple[H *: T] with
    def encodeRaw(t: H *: T): List[String] = summon[StringRenderer[H]](t.head) :: summon[FromTuple[T]].encodeRaw(t.tail)

object FromProduct:
  inline given fromProduct[P <: Product](using
    m: Mirror.ProductOf[P],
    ft: FromTuple[m.MirroredElemTypes]
  ): FromProduct[P] = (p: P) =>
    val values = ft.encodeRaw(Tuple.fromProductTyped(p))
    val labels = p.productElementNames.toList
    Record(values*)(Header(labels*))
