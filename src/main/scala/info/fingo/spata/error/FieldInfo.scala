/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.error

import info.fingo.spata.Header

/** Field information provided in case of error.
  *
  * Depending and the stage of CSV processing (parsing, accessing) and the type of error,
  * it may be available fully (with name and index), partially (with name or index) or not available at all.
  *
  * @param name optional field name
  * @param index optional field index
  */
final case class FieldInfo private[spata] (name: Option[String], index: Option[Int]):

  /* Constructor in case of missing index information. */
  private[spata] def this(name: Option[String]) = this(name, None)

  /** Provides string representation of field info.
    *
    * Provides information about field name and index if available.
    * For unavailable values triple question mark is used.
    *
    * @return information about field as string
    */
  override def toString: String =
    val ns = name.map(n => s"'$n'").getOrElse("???")
    val is = index.map(i => i.toString).getOrElse("???")
    s"name $ns, index $is"

  /** Gets string representation of field name.
    *
    * @return field name or empty string if field name is unavailable
    */
  def nameInfo: String = name.getOrElse("")

  /** Gets string representation of field index.
    *
    * @return field index or empty string if field index is unavailable
    */
  def indexInfo: String = index.map(_.toString).getOrElse("")

  /** Check if any field information is available.
    *
    * @return true if field name or index are defined.
    */
  inline def isDefined: Boolean = name.isDefined || index.isDefined

/* FieldInfo companion with factory methods. */
private[spata] object FieldInfo:

  /* Construct FieldInfo with full information. */
  def apply(name: String, index: Int): FieldInfo = new FieldInfo(Some(name), Some(index))

  /* Construct FieldInfo with name only. */
  def apply(name: String): FieldInfo = new FieldInfo(Some(name), None)

  /* Construct FieldInfo with index only. */
  def apply(index: Int): FieldInfo = new FieldInfo(None, Some(index))

  /* Construct FieldInfo with index and possibly with name when available through header. */
  def apply(index: Int, header: Header): FieldInfo = new FieldInfo(header(index), Some(index))

  /* Construct FieldInfo with no information. */
  val none: FieldInfo = new FieldInfo(None, None)
