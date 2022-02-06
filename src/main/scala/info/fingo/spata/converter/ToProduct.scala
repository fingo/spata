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

trait ToProduct[P <: Product]:
  def decode(r: Record): Decoded[P]

trait ToTuple[+T <: Tuple]:
  def decode(r: Record): Decoded[T]

object  ToTuple:
  given toEmpty: ToTuple[EmptyTuple] with
    def decode(r: Record): Decoded[EmptyTuple] = Right(EmptyTuple)
  given toCons[H: StringParser, T <: Tuple: ToTuple]: ToTuple[H *: T] with
    def decode(r: Record): Decoded[H *: T] =
      // TODO: avoid unnecessary intermediate records creation
      for
        h <- r.get(0)
        t <- summon[ToTuple[T]].decode(Record.fromValues(r.values.tail*))
      yield h *: t

object ToProduct:
  inline given toProduct[P <: Product](using m: Mirror.ProductOf[P]): ToProduct[P] =
    val types = getTypes[m.MirroredElemTypes]
    val labels = getLabels[m.MirroredElemLabels]
    decodeProduct(m, labels.zip(types))

  inline private def getTypes[T <: Tuple]: List[StringParser[_]] = inline erasedValue[T] match
    case _: EmptyTuple => Nil
    case _: (t *: ts) => summonInline[StringParser[t]] :: getTypes[ts]

  inline private def getLabels[T <: Tuple]: List[String] = inline erasedValue[T] match
    case _: EmptyTuple => Nil
    case _: (t *: ts) => constValue[t].toString :: getLabels[ts]

  private def decodeProduct[T <: Product](p: Mirror.ProductOf[T], elements: List[(String,StringParser[_])]): ToProduct[T] =
    (r: Record) =>
      // TODO: check if this could be simplified
      val instance = for (label,parser) <- elements yield r.get(label)(parser)
      val (left,right) = instance.partitionMap(identity)
      left.headOption.toLeft(p.fromProduct(Tuple.fromArray(right.toArray)))
