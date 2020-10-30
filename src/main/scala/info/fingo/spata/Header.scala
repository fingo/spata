/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import info.fingo.spata.error.{ParsingErrorCode, StructureException}

/* CSV header with names of each field */
private[spata] class Header private (names: IndexedSeq[String]) {

  def this(names: String*) = this(names.toIndexedSeq)

  private val index = names.zipWithIndex.toMap

  val size: Int = names.size

  def apply(name: String): Option[Int] = index.get(name)

  def get(idx: Int): Option[String] = names.unapply(idx)

  override def toString: String = names.mkString("Header(", ", ", ")")
}

/* Header companion */
private[spata] object Header {

  /* Create regular header from provided values */
  def apply(names: String*): Either[StructureException, Header] = checkDuplicates(names.toIndexedSeq)

  /* Create regular header and reset / remap it */
  def apply(names: IndexedSeq[String], headerMap: HeaderMap): Either[StructureException, Header] = {
    val remapped = headerMap.remap(names)
    checkDuplicates(remapped)
  }

  /* Create tuple-style header: _1, _2, _3 etc. (if not reset/remapped). */
  def apply(size: Int, headerMap: HeaderMap): Either[StructureException, Header] = {
    val remapped = headerMap.remap(generate(size))
    checkDuplicates(remapped)
  }

  /* Generate tuple-style sequence: _1, _2, _3 etc. */
  private def generate(size: Int) = (0 until size).map(i => s"_${i + 1}")

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
      Left(new StructureException(ParsingErrorCode.DuplicatedHeader, 1, 0, None, doubles.headOption))
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
  implicit def s2sHeaderMap(f: S2S): HeaderMap = new NameHeaderMap(f)

  /** Provides conversion from `PartialFunction[Int, String]` to [HeaderMap]. */
  implicit def i2sHeaderMap(f: I2S): HeaderMap = new IndexHeaderMap(f)
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
