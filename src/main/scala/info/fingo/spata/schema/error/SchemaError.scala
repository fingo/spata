/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema.error

import info.fingo.spata.error.{ContentError, DataError, HeaderError}

/** Error for schema validation. */
sealed trait SchemaError {

  /** Gets short code of error. This value can be used as a key to provide localized error information. */
  def code: String

  /** Gets default error message. */
  def message: String
}

/** Error for invalid type of field encountered during schema validation.
  *
  * @param ce content error returned by record parsing - see [[Record.get[A](* Record.get]]
  */
class TypeError private (ce: ContentError) extends SchemaError {

  /** @inheritdoc */
  def code: String = ce.messageCode

  /** @inheritdoc */
  def message: String = ce match {
    case _: DataError => ce.getCause.getMessage
    case _: HeaderError => "Key not found"
  }
}

/* Companion with TypeError creation method. */
private[schema] object TypeError {
  def apply(ce: ContentError): TypeError = new TypeError(ce)
}

/** Result of erroneous custom validation.
  *
  * @param code the error code, which may be used to provide localized message
  * @param message default error message
  */
class ValidationError private (val code: String, val message: String) extends SchemaError

/* ValidationError companion with creation method. */
private[schema] object ValidationError {
  def apply(code: String, message: String): ValidationError = new ValidationError(code, message)
}
