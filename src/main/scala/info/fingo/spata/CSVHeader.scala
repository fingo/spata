/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

/* CSV header with names of each field */
private[spata] class CSVHeader private (names: IndexedSeq[String]) {

  private val index = names.zipWithIndex.toMap

  val size: Int = names.size

  def apply(name: String): Option[Int] = index.get(name)

  def get(idx: Int): Option[String] = names.unapply(idx)
}

/* CSVHeader companion */
private[spata] object CSVHeader {

  /* Create regular header from provided values */
  def apply(names: String*): CSVHeader = new CSVHeader(names.toIndexedSeq)

  /* Create regular header and remap it */
  def apply(names: IndexedSeq[String], headerMap: S2S): CSVHeader = {
    val remapped = names.map(hRemap(_, headerMap))
    new CSVHeader(remapped)
  }

  /* Create tuple-style header: _1, _2, _3 etc. (if not remapped). */
  def apply(size: Int, headerMap: S2S): CSVHeader = {
    val remapped = generate(size).map(hRemap(_, headerMap))
    new CSVHeader(remapped)
  }

  /* Generate tuple-style sequence: _1, _2, _3 etc. */
  private def generate(size: Int) = (0 until size).map(i => s"_${i + 1}")

  /* Remap provided header values, leave intact the rest. */
  private def hRemap(header: String, f: S2S): String = if (f.isDefinedAt(header)) f(header) else header
}
