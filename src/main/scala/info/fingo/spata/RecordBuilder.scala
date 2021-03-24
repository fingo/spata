/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import info.fingo.spata.text.StringRenderer

/** Helper to incrementally build records from typed values.
  *
  * @param buf buffer used to incrementally build record's content.
  */
class RecordBuilder private (buf: List[(String, String)]) {

  /** Gets final record from this builder.
    *
    * @return new record with values from this builder.
    */
  def result(): Record = Record.fromPairs(buf.reverse: _*)

  /* Gets final record from this builder with reversed order of the fields,
   * which really means preserving the order, because values ate prepended.
   */
  private[spata] def reversed(): Record = Record.fromPairs(buf: _*)

  /** Enhance builder with a new value.
    *
    * @param key the key (field name) of added value
    * @param value the added value
    * @param renderer the renderer to convert the value to string
    * @tparam A value type
    * @return builder augmented with the new value
    */
  def add[A](key: String, value: A)(implicit renderer: StringRenderer[A]): RecordBuilder =
    new RecordBuilder((key, renderer(value)) :: buf)
}

/** Record builder companion. */
object RecordBuilder {

  /** Creates new [[RecordBuilder]]. */
  def apply(): RecordBuilder = new RecordBuilder(List[(String, String)]())

  /** Implicitly converts [[RecordBuilder]] to [[Record]].
    *
    * @param rb the RecordBuilder to be converted
    * @return new Record with values from provided builder
    */
  implicit def toRecord(rb: RecordBuilder): Record = rb.result()
}
