/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema.error

import info.fingo.spata.error.ContentError
import info.fingo.spata.error.DataError
import info.fingo.spata.error.HeaderError
import info.fingo.spata.error.IndexError

/** Error for schema validation. */
sealed trait SchemaError:

  /** Gets short code of error. This value can be used as a key to provide localized error information. */
  def code: String

  /** Gets default error message. */
  def message: String

/** Error for invalid type of field encountered during schema validation.
  *
  * @param ce content error returned by record parsing - see [[Record.get]]
  */
final class TypeError private[schema] (ce: ContentError) extends SchemaError:

  /** @inheritdoc */
  def code: String = ce.messageCode

  /** @inheritdoc */
  def message: String = ce match
    case _: DataError => ce.getCause.getMessage
    case _: HeaderError => "Key not found"
    case _: IndexError => "Incorrect index"

/** Result of erroneous custom validation.
  *
  * @param code the error code, which may be used to provide localized message
  * @param message default error message
  */
final class ValidationError private[schema] (val code: String, val message: String) extends SchemaError
