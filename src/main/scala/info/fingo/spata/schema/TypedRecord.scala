/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import info.fingo.spata.converter.RecHListToRepr
import shapeless.{HList, LabelledGeneric}
import shapeless.ops.record.Selector
import info.fingo.spata.schema.TypedRecord.ToProduct

/** CSV record representation with type-safe access to its values.
  * Typed records are created as result of schema validation.
  *
  * Typed records values are accessed similarly to regular records, as values referenced by string keys.
  * In contract to regular record, however, the keys are verified at compile time. See [[apply]] method for more.
  * The returned value has already required type, as declared by schema definition:
  * {{{ val id: Int = record("id") }}}
  *
  * Thanks to this, in contrast to a regular [[Record]], field access operation through [[apply]]
  * and conversion to case class through [[to]] returns required type directly, without wrapping it in `Either`.
  *
  * For `lineNum` and `rowNum` description see [[Record]].
  *
  * @see [[CSVSchema]] for information about schema validation.
  * @param data the actual record's data
  * @param lineNum last line number in source file this record is built from
  * @param rowNum row number in source file this record comes from
  * @tparam L data type - heterogeneous list
  */
class TypedRecord[+L <: HList] private (private[schema] val data: L, val lineNum: Int, val rowNum: Int) {

  /** Gets record value in type-safe manner.
    *
    * The key is verified at compile time and has to be present in the record to use it.
    * To achieve this, the provided key is required to have a singleton type. This works out of the box for literals,
    * but has to be declared explicitly for variables:
    * {{{
    * val v1 = record("key") // OK
    * val key: "key" = "key"
    * val v2 = record(key) // OK
    * val wrong = "key"
    * val v3 = record(wrong) // does not compile
    * }}}
    * The returned value has the type which has been declared for given field by schema definition and validation
    * (the proper type is inferred, you do not have to provide it explicitly):
    * {{{ val value: Double = record("value") }}}
    * The formal return type, `selector.Out`, is a bit baffling, because it is fixed by calling code.
    *
    * @param key the key of retrieved field
    * @param selector heterogeneous list selector allowing type-safe access to record values
    * @tparam M the data type - this is a supertype of record type parameter to overcome covariance
    * @return field value
    */
  def apply[M >: L <: HList](key: Key)(implicit selector: Selector[M, key.type]): selector.Out = selector(data)

  /** Converts typed record to a case class.
    * Uses intermediary class [[TypedRecord.ToProduct]] and its `apply` method.
    *
    * Because typed record has already got all the values properly typed, it may be safely converted to a case class.
    * Assuming, that the record has a field `name` of type `String` and a field birthdate of type `LocalDate`,
    * the conversion is as simple as that:
    * {{{
    * case class Person(name: String, birthdate: LocalDate)
    * val person = record.to[Person]()
    * }}}
    *
    * Please note that the conversion is name-based (case class field names have to match record fields),
    * is case sensitive and only shallow conversion is supported.
    * Case class may be narrower and effectively retrieve only a subset of record's fields.
    *
    * It is possible to use a tuple instead of case class.
    * In such a case the field names must match the tuple field naming convention: `_1`, `_2` etc.
    *
    * @tparam P the Product type to converter this record to
    * @return intermediary to infer representation type and return proper type
    */
  def to[P <: Product]: ToProduct[P, L] = new ToProduct[P, L](data)
}

/** Typed record helper object. */
object TypedRecord {

  /* Constructs typed record */
  private[schema] def apply[L <: HList](data: L, lineNum: Int, rowNum: Int): TypedRecord[L] =
    new TypedRecord[L](data, lineNum, rowNum)

  /** Intermediary to delegate conversion to in order to infer [[shapeless.HList]] representation type.
    *
    * @see [[TypedRecord.to]] for usage scenario.
    * @param data the record's data
    * @tparam P the target type for conversion - product
    * @tparam L the source type for conversion - heterogeneous list
    */
  class ToProduct[P <: Product, +L <: HList] private[schema] (data: L) {

    /** Converts typed record to [[scala.Product]], e.g. case class.
      *
      * @param lg the generic for specified target type and [[shapeless.HList]] representation
      * @param rrConv intermediary converter from string-based [[shapeless.HList]] of [[shapeless.labelled.FieldType]]s,
      * as used by typed record, to symbol-based one, as required by [[shapeless.LabelledGeneric]]
      * @tparam M the source data type - this is a supertype of record type parameter to overcome covariance
      * @tparam R [[shapeless.LabelledGeneric]] representation type
      * @return converted product
      */
    def apply[M >: L <: HList, R <: HList]()(implicit lg: LabelledGeneric.Aux[P, R], rrConv: RecHListToRepr[M, R]): P =
      lg.from(rrConv(data))
  }
}
