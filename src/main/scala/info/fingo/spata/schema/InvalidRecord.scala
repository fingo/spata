/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import cats.data.NonEmptyList
import info.fingo.spata.Record
import info.fingo.spata.schema.error.SchemaError

/** CSV record which has not passed validation against schema.
  *
  * @param record the original record
  * @param flaws list of conflicting fields with their errors
  */
final class InvalidRecord private[schema] (val record: Record, val flaws: NonEmptyList[FieldFlaw]):

  /** Gets description of all validation errors for record.
    *
    * @return error information
    */
  override def toString: String =
    flaws.toList.mkString(s"Invalid record at row ${record.rowNum} (line ${record.lineNum}): ", ", ", "")

/** CSV field which has not passed validation.
  *
  * @param name the name of this field
  * @param error validation error
  */
final class FieldFlaw private[schema] (val name: String, val error: SchemaError):

  /** Gets description of validation error for field.
    *
    * @return error information
    */
  override def toString: String = s"'$name' -> ${error.message}"
