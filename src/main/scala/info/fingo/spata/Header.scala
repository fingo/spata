/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import info.fingo.spata.Header.generate
import info.fingo.spata.error.{ParsingErrorCode, StructureException}

/** CSV header with names of each field.
  *
  * Header created through parsing process ensured to have no duplicate names.
  * This guarantee is not held for user-created headers.
  * Providing duplicates does not cause any erroneous conditions while accessing record data,
  * however the values associated with duplicates will be not accessible by header name.
  *
  * @param names the sequence of names
  */
class Header private (val names: IndexedSeq[String]) {

  /** Size of header. */
  val size: Int = names.size

  private val index = names.zipWithIndex.toMap

  private[spata] def apply(name: String): Option[Int] = index.get(name)

  /** Safely get header element (single name).
    *
    * @param idx index of retrieved name, starting from `0`
    * @return a string representing single header element (field name) or `None` if index is out of bounds.
    */
  def apply(idx: Int): Option[String] = names.unapply(idx)

  /** String representation of header */
  override def toString: String = names.mkString("Header(", ", ", ")")

  /* Shrink this header to requested length. */
  private[spata] def shrink(to: Int) = new Header(names.take(to))

  /* Extend this header to requested length. */
  private[spata] def extend(to: Int) = new Header(names ++ generate(to, size))
}

/** Header companion */
object Header {

  /** Creates regular header from provided values.
    *
    * This method does to enforce header to have unique elements.
    *
    * @param names the sequence of names forming this header
    * @return new header
    */
  def apply(names: String*): Header = new Header(names.toIndexedSeq)

  /** Creates tuple-style header (`_1`, `_2`, `_3` etc.) of requested size.
    *
    * @param size the size of this header
    * @return new header
    */
  def apply(size: Int): Header = new Header(generate(size))

  /* Creates regular header and reset / remap it. Ensures that there are no duplicates in header. */
  private[spata] def create(names: IndexedSeq[String], headerMap: HeaderMap): Either[StructureException, Header] = {
    val remapped = headerMap.remap(names)
    checkDuplicates(remapped)
  }

  /* Creates tuple-style header: _1, _2, _3 etc. (if not reset/remapped). Ensures that there are no duplicates. */
  private[spata] def create(size: Int, headerMap: HeaderMap): Either[StructureException, Header] = {
    val remapped = headerMap.remap(generate(size))
    checkDuplicates(remapped)
  }

  /* Generate tuple-style sequence: _1, _2, _3 etc. */
  private def generate(size: Int, start: Int = 0) = (start until size).map(i => s"_${i + 1}")

  /* Gets duplicates from a collection (single occurrences of them), preserving their sequence */
  private def duplicates[A](seq: Seq[A]): Seq[A] = {
    val firstDuplicates = for {
      (_, group) <- seq.zipWithIndex.groupBy { case (elem, _) => elem }
      if group.size > 1 // duplicate
      first = group.minBy { case (_, idx) => idx }
    } yield first
    firstDuplicates.toSeq.sortBy { case (_, idx) => idx }.map { case (elem, _) => elem }
  }

  /* Check if there are duplicates and return header or error */
  private def checkDuplicates(header: IndexedSeq[String]): Either[StructureException, Header] = {
    val doubles = duplicates(header)
    if (doubles.isEmpty)
      Right(new Header(header))
    else
      Left(new StructureException(ParsingErrorCode.DuplicatedHeader, Position.some(0, 1), None, doubles.headOption))
  }
}

/** Trait representing header remapping methods.
  * It is not used directly but through conversion of [S2S] or [I2S] partial function to one its implementation classes.
  *
  * @see [CSVConfig] for sample usage.
  */
sealed trait HeaderMap {

  /** Remap selected header names.
    * The actual remapping is provided by HeaderMap implementation class, created from a partial function,
    * as described in [CSVConfig].
    */
  def remap(header: IndexedSeq[String]): IndexedSeq[String]
}

/** Implicit conversions for [HeaderMap] trait. */
object HeaderMap {

  /** Provides conversion from `PartialFunction[String, String]` to [HeaderMap]. */
  implicit def s2sHdrMap(f: S2S): HeaderMap = new NameHeaderMap(f)

  /** Provides conversion from `PartialFunction[Int, String]` to [HeaderMap]. */
  implicit def i2sHdrMap(f: I2S): HeaderMap = new IndexHeaderMap(f)
}

/* No-op implementation of HeaderMap. */
private[spata] object NoHeaderMap extends HeaderMap {
  def remap(header: IndexedSeq[String]): IndexedSeq[String] = header
}

/* String to string implementation of HeaderMap. */
private[spata] class NameHeaderMap(f: S2S) extends HeaderMap {
  def remap(header: IndexedSeq[String]): IndexedSeq[String] = {
    val mapName = (name: String) => if (f.isDefinedAt(name)) f(name) else name
    header.map(mapName)
  }
}

/* Int to string implementation of HeaderMap. */
private[spata] class IndexHeaderMap(f: I2S) extends HeaderMap {
  def remap(header: IndexedSeq[String]): IndexedSeq[String] = {
    val mapName = (name: String, index: Int) => if (f.isDefinedAt(index)) f(index) else name
    header.zipWithIndex.map { case (name, index) => mapName(name, index) }
  }
}
