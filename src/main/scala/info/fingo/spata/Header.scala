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
}

/* Header companion */
private[spata] object Header {

  /* Create regular header from provided values */
  def apply(names: String*): Either[StructureException, Header] = checkDuplicates(names.toIndexedSeq)

  /* Create regular header and reset / remap it */
  def apply(names: IndexedSeq[String], headerSeq: I2S, headerMap: S2S): Either[StructureException, Header] = {
    val reset = set(names, headerSeq)
    val remapped = reset.map(hRemap(_, headerMap))
    checkDuplicates(remapped)
  }

  /* Create tuple-style header: _1, _2, _3 etc. (if not reset/remapped). */
  def apply(size: Int, headerSeq: I2S, headerMap: S2S): Either[StructureException, Header] = {
    val reset = set(generate(size), headerSeq)
    val remapped = reset.map(hRemap(_, headerMap))
    checkDuplicates(remapped)
  }

  /* Set selected values in sequence provided by partial function */
  private def set(seq: IndexedSeq[String], f: I2S) = seq.zipWithIndex.map {
    case (name, index) => hReset(name, index, f)
  }

  /* Generate tuple-style sequence: _1, _2, _3 etc. */
  private def generate(size: Int) = (0 until size).map(i => s"_${i + 1}")

  /* Remap provided header values, leave intact the rest. */
  private def hRemap(header: String, f: S2S): String = if (f.isDefinedAt(header)) f(header) else header

  /* Remap provided header values, leave intact the rest. */
  private def hReset(name: String, index: Int, f: I2S): String = if (f.isDefinedAt(index)) f(index) else name

  /* Gets duplicates from a collection, preserving their sequence */
  private def duplicates[A](seq: Seq[A]): Seq[A] =
    seq.zipWithIndex.groupBy(_._1).filter(_._2.size > 1).values.map(_.minBy(_._2)).toSeq.sortBy(_._2).map(_._1)

  /* Check if there are duplicates and return header or error */
  private def checkDuplicates(header: IndexedSeq[String]): Either[StructureException, Header] = {
    val doubles = duplicates(header)
    if (doubles.isEmpty)
      Right(new Header(header))
    else
      Left(new StructureException(ParsingErrorCode.DuplicatedHeader, 1, 0, None, doubles.headOption))
  }
}
