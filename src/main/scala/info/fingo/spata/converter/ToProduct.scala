/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.converter

import scala.deriving.Mirror
import scala.compiletime.{constValue, erasedValue, summonInline}
import info.fingo.spata.Decoded
import info.fingo.spata.Record
import info.fingo.spata.text.StringParser

/** Converter from a record to a tuple.
  *
  * This trait defines behavior to be implemented by concrete, given converters.
  *
  * @tparam T type of target tuple
  */
trait ToTuple[+T <: Tuple]:

  /** Converts record to tuple.
    *
    * @param r record to be converted.
    * @return either converted tuple or an exception.
    */
  def decode(r: Record): Decoded[T] = decodeAt(r, 0)

  /* Converts record to tuple starting at given index. */
  private[converter] def decodeAt(r: Record, pos: Int): Decoded[T]

/** Converter from a record to a product (case class).
  *
  * This trait defines behavior to be implemented by concrete, given converters.
  *
  * @tparam P type of target entity.
  */
trait ToProduct[P <: Product]:

  /** Converts record to product (case class).
    *
    * @param r record to be converted.
    * @return either converted tuple or an exception.
    */
  def decode(r: Record): Decoded[P]

/** `ToTuple` companion object with given instances of record to tuple converter. */
object ToTuple:

  /** Given instance for converter to empty tuple. */
  given toEmpty: ToTuple[EmptyTuple] with
    private[converter] def decodeAt(r: Record, pos: Int): Decoded[EmptyTuple] = Right(EmptyTuple)

  /** Given instance for recursive converter to tuple cons. */
  given toCons[H: StringParser, T <: Tuple: ToTuple]: ToTuple[H *: T] with
    private[converter] def decodeAt(r: Record, pos: Int): Decoded[H *: T] =
      for // loop ends with reduced tuple, so no guard for growing position is required (wrong index returns error)
        h <- r.get(pos)
        t <- summon[ToTuple[T]].decodeAt(r, pos + 1)
      yield h *: t

/** `ToProduct` companion object with given instance of record to product converter. */
object ToProduct:

  /** Given instance for converter from record product (case class). */
  inline given toProduct[P <: Product](using m: Mirror.ProductOf[P]): ToProduct[P] =
    val labels = getLabels[m.MirroredElemLabels]
    val types = getTypes[m.MirroredElemTypes]
    decodeProduct(m, labels.zip(types))

  /* Get tuple values as list of string - used to get names of fields of case class (provided by `Mirror`). */
  private[spata] inline def getLabels[T <: Tuple]: List[String] = inline erasedValue[T] match
    case _: EmptyTuple => Nil
    case _: (t *: ts) => constValue[t].toString :: getLabels[ts]

  /* Get tuple types as list of string parsers - used to get types of fields of case class (provided by `Mirror`). */
  private inline def getTypes[T <: Tuple]: List[StringParser[?]] = inline erasedValue[T] match
    case _: EmptyTuple => Nil
    case _: (t *: ts) => summonInline[StringParser[t]] :: getTypes[ts]

  /* Assemble product from record using field names and typed parsers retrieved from `Mirror`. */
  private def decodeProduct[T <: Product](
    p: Mirror.ProductOf[T],
    elements: List[(String, StringParser[?])]
  ): ToProduct[T] =
    (r: Record) =>
      val instance = for (label, parser) <- elements yield r.get(label)(parser)
      val (left, right) = instance.partitionMap(identity)
      left.headOption.toLeft(p.fromProduct(Tuple.fromArray(right.toArray)))
