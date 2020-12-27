/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import info.fingo.spata.error.ContentError

abstract class ValidationError(val message: String) {
  // TODO: extract and share with ParsingErrorCode
  def code: String = {
    val name = this.getClass.getSimpleName.stripSuffix("$")
    val first = name.take(1)
    name.replaceFirst(first, first.toLowerCase)
  }
}

case class NotParsed(override val message: String, cause: ContentError) extends ValidationError(message)
case object Unknown extends ValidationError("Unknown validation error")
case object NotRegexConform extends ValidationError("Value not conforming to regex")
case object ValueToSmall extends ValidationError("Value to small")
case object ValueToLarge extends ValidationError("Value to large")
case object NotFiniteNumber extends ValidationError("Number not finite")
