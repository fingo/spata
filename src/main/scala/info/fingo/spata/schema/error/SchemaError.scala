/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema.error

import info.fingo.spata.error.ContentError
import info.fingo.spata.util.classId

/** Error for schema validation. */
trait SchemaError {

  /** Gets short code of error. This value can be used as a key for to provide localized error information. */
  def code: String

  /** Gets default error message. */
  def message: String
}

/** Error for invalid type of field encounter during schema validation.
  *
  * @param ce content error returned by record parsing - see `Record.get`
  */
class TypeError private[spata] (ce: ContentError) extends SchemaError {

  /** @inheritdoc */
  def code: String = ce.messageCode

  /** @inheritdoc */
  def message: String = ce.getCause.getMessage
}

/* Companion with TypeError creation method. */
private[spata] object TypeError {
  def apply(ce: ContentError): TypeError = new TypeError(ce)
}

/** Product of erroneous custom validation.
  *
  * The provided validator is used to form error code, which is the validator name in camelCase.
  *
  * @param validator the validator which yield the error
  * @param message default error message
  */
class ValidationError(validator: AnyRef, val message: String) extends SchemaError {

  /** @inheritdoc */
  def code: String = classId(validator)
}

/** [[TypeError]] companion with creation method. */
object ValidationError {

  /** Creates validation error.
    *
    * @param validator the validator which yield the error
    * @param message default error message
    * @return new validation error
    */
  def apply(validator: AnyRef, message: String): ValidationError = new ValidationError(validator, message)
}
