/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.text

import java.util.Locale

/** Formatter used to format and parse string representation of boolean values.
  *
  * @constructor Creates formatter.
  * @param tt the term representing `true`, case insensitive
  * @param ft the term representing `false`, case insensitive
  * @param locale the locale used to handle case conversion
  */
class BooleanFormatter(tt: String, ft: String, locale: Locale) {
  val trueTerm: String = tt.toLowerCase(locale)
  val falseTerm: String = ft.toLowerCase(locale)

  /** Creates formatter with default locale.
    *
    * @param trueTerm the term representing `true`, case insensitive
    * @param falseTerm the term representing `false`, case insensitive
    */
  def this(trueTerm: String, falseTerm: String) = this(trueTerm, falseTerm, Locale.getDefault())

  /** Returns string representation of boolean value.
    *
    * @param value the value to be converted to string
    * @return textual representation of value
    */
  def format(value: Boolean): String = if (value) trueTerm else falseTerm

  /** Obtains boolean value from text.
    *
    * @param string the text to parse
    * @return `true` or `false`
    * @throws ParseError if text cannot be parsed to boolean
    */
  @throws[ParseError]("if text cannot be parsed to boolean")
  def parse(string: String): Boolean = string.strip().toLowerCase(locale) match {
    case `trueTerm` => true
    case `falseTerm` => false
    case _ => throw new ParseError(string, Some("boolean"))
  }
}

/** [[BooleanFormatter]] companion object, used for formatter creation. */
object BooleanFormatter {

  /** Creates new formatter.
    *
    * @param tt the term representing `true`, case insensitive
    * @param ft the term representing `false`, case insensitive
    * @param locale the locale used to handle case conversion
    * @return new formatter
    */
  def apply(tt: String, ft: String, locale: Locale): BooleanFormatter = new BooleanFormatter(tt, ft, locale)

  /** Creates new formatter with default locale.
    *
    * @param tt the term representing `true`, case insensitive
    * @param ft the term representing `false`, case insensitive
    * @return new formatter
    */
  def apply(tt: String, ft: String): BooleanFormatter = new BooleanFormatter(tt, ft)

  /** Provides default [[BooleanFormatter]], with `true` and `false` as textual representation. */
  val default: BooleanFormatter = apply(true.toString, false.toString)
}
