/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import info.fingo.spata.Record

class InvalidRecord(val record: Record, val flaws: List[FieldFlaw])

object InvalidRecord {
  def apply(record: Record, flaws: List[FieldFlaw]): InvalidRecord =
    new InvalidRecord(record, flaws)
}

class FieldFlaw(val name: String, val error: ValidationError)

object FieldFlaw {
  def apply(name: String, error: ValidationError): FieldFlaw = new FieldFlaw(name, error)
}