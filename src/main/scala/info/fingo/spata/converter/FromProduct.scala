/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.converter

import info.fingo.spata.text.StringRenderer
import info.fingo.spata.{Decoded, Header, Record}
import scala.deriving.Mirror

/** Converter from a tuple to a record.
  *
  * This trait defines behavior to be implemented by concrete, given converters.
  *
  * @tparam P type of source entity
  */
trait FromTuple[-T <: Tuple]:

  /** Converts tuple to record.
    *
    * @param t the source tuple to be converted.
    * @return the record with field from provided tuple.
    */
  def encode(t: T): Record = Record.fromValues(encodeRaw(t)*)

  /** Converts tuple to list of strings.
    *
    * @param t the source tuple to be converted.
    * @return the list of strings containing values of provided tuple.
    */
  def encodeRaw(t: T): List[String]

/** Converter from a product (case class) to a record.
  *
  * This trait defines behavior to be implemented by concrete, given converters.
  *
  * @tparam P type of source entity.
  */
trait FromProduct[P <: Product]:

  /** Converts product (case class) to record.
    *
    * @param p the source entity to be converted.
    * @return the record with fields from provided entity.
    */
  def encode(p: P): Record

/** `FromTuple` companion object with given instances of tuple to record converter. */
object FromTuple:

  /** Given instance for converter from empty tuple. */
  given fromEmpty: FromTuple[EmptyTuple] with
    def encodeRaw(et: EmptyTuple): List[String] = Nil

  /** Given instance for recursive converter from tuple cons. */
  given fromCons[H: StringRenderer, T <: Tuple: FromTuple]: FromTuple[H *: T] with
    def encodeRaw(t: H *: T): List[String] = summon[StringRenderer[H]](t.head) :: summon[FromTuple[T]].encodeRaw(t.tail)

/** `FromProduct` companion object with given instance of product to record converter. */
object FromProduct:

  /** Given instance for converter from product (case class) to record. */
  inline given fromProduct[P <: Product](using
    m: Mirror.ProductOf[P],
    ft: FromTuple[m.MirroredElemTypes]
  ): FromProduct[P] with
    def encode(p: P): Record =
      val values = ft.encodeRaw(Tuple.fromProductTyped(p))
      val labels = p.productElementNames.toList
      Record(values*)(Header(labels*))
