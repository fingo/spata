/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import scala.deriving.Mirror
import scala.compiletime.{constValue, erasedValue}
import info.fingo.spata.converter.ToProduct
import info.fingo.spata.schema.TypedRecord.*

/** CSV record representation with type-safe access to its values.
  * Typed records are created as result of schema validation.
  *
  * Typed records values are accessed similarly to regular records, as values referenced by string keys.
  * In contract to regular record, however, the keys are verified at compile time. See [[apply]] method for more.
  * The returned value has already required type, as declared by schema definition:
  * ```
  * val id: Int = record("id")
  * ```
  *
  * Thanks to this, in contrast to a regular [[Record]], field access operation through [[apply]]
  * and conversion to case class through [[to]] return required type directly, without wrapping it in `Either`.
  *
  * For `lineNum` and `rowNum` description see [[Record]].
  *
  * @see [[CSVSchema]] for information about schema validation.
  * @param keys actual record's keys
  * @param values actual record's values
  * @param lineNum last line number in source file this record is built from
  * @param rowNum row number in source file this record comes from
  * @tparam KS keys type - a tuple
  * @tparam VS values type - a tuple
  */
final class TypedRecord[KS <: Tuple, VS <: Tuple] private (
  private[schema] val keys: KS,
  private[schema] val values: VS,
  val lineNum: Int,
  val rowNum: Int
)(using ev1: Tuple.Size[KS] =:= Tuple.Size[VS], ev2: Tuple.Union[KS] <:< Key):

  /** Gets record value in type-safe manner.
    *
    * The key is verified at compile time and has to be present in the record to use it.
    * To achieve this, the provided key is required to have a singleton type. This works out of the box for literals,
    * but has to be declared explicitly for variables:
    * ```
    * val v1 = record("key") // OK
    * val key: "key" = "key"
    * val v2 = record(key) // OK
    * val wrong = "key"
    * val v3 = record(wrong) // does not compile
    * ```
    * The returned value has the type which has been declared for given field by schema definition and validation
    * (the proper type is inferred, you do not have to provide it explicitly):
    * ``` val value: Double = record("value") ```
    * The formal return type, `Select[K, KS, VS]`, is reduced to proper type through match type.
    *
    * @param key the key of retrieved field
    * @tparam K the key type - this is a singleton type representing the key
    * @return field value
    */
  def apply[K <: Key](key: K): Select[K, KS, VS] = get(key, keys, values)

  /** Converts typed record to a case class.
    *
    * Because typed record has already got all the values properly typed, it may be safely converted to a case class.
    * Assuming, that the record has a field `name` of type `String` and a field `birthdate` of type `LocalDate`,
    * the conversion is as simple as that:
    * ```
    * case class Person(name: String, birthdate: LocalDate)
    * val person = record.to[Person]
    * ```
    *
    * Please note that the conversion is name-based (case class field names have to match record fields),
    * is case sensitive and only shallow conversion is supported.
    * Case class may be narrower and effectively retrieve only a subset of record's fields.
    *
    * It is possible to use a tuple instead of case class.
    * In such a case the field names must match the "classic" tuple field naming convention: `_1`, `_2` etc.
    *
    * @tparam P the Product type to converter this record to
    * @return the requested case class or tuple
    */
  inline def to[P <: Product](using
    m: Mirror.ProductOf[P],
    ev: Tuple.Union[Tuple.Zip[m.MirroredElemLabels, m.MirroredElemTypes]] <:< Tuple.Union[Tuple.Zip[KS, VS]]
  ): P =
    val labels = ToProduct.getLabels[m.MirroredElemLabels]
    val vals = labels.map(l => get(l, keys, values))
    m.fromProduct(Tuple.fromArray(vals.toArray))

  /* Gets value from `values` matching the `key` from `keys`.
   * It starts with full tuples of keys and values and recursivelyreduces them until matching key is found.
   * This method  and `getT` call each other alternately to reduce keys and values accordingly.
   * This is a dependently typed method corresponding to `Select` match type.
   */
  private def get[K <: Key, KS <: Tuple, VS <: Tuple](key: K, keys: KS, values: VS): Select[K, KS, VS] =
    (keys: @unchecked) match
      case `key` *: _: *:[K @unchecked, ?] => getH(values)
      case _ *: tk: *:[?, ?] => getT(key, tk, values)

  /* Gets value from `values` matching the `key` from `keys`.
   * This method and `get` call each other alternately to reduce values and keys accordingly.
   * This is a dependently typed method corresponding to `SelectT` match type.
   */
  private def getT[K <: Key, KS <: Tuple, VS <: Tuple](key: K, keys: KS, values: VS): SelectT[K, KS, VS] =
    (values: @unchecked) match
      case _ *: t: *:[?, ?] => get(key, keys, t)

  /* Gets head value from tuple of `values`.
   * It is used to retrieve correctly typed value which key has been already matched.
   * This is a dependently typed method corresponding to `SelectH` match type.
   */
  private def getH[VS <: Tuple](values: VS): SelectH[VS] =
    (values: @unchecked) match
      case h *: _: *:[?, ?] => h

/** Typed record helper object. */
object TypedRecord:

  /** Match type to select type of value matching provided key.
    * This type and [[SelectT]] call each other alternately to reduce tuples of keys and values accordingly.
    */
  type Select[K <: Key, KS <: Tuple, VS <: Tuple] = KS match
    case K *: ? => SelectH[VS]
    case h *: t => SelectT[K, t, VS]

  /** Match type to reduce tuple of values to its tail.
    * This type and [[Select]] call each other alternately to reduce tuples of values and keys accordingly.
    */
  type SelectT[K <: Key, KS <: Tuple, VS <: Tuple] = VS match
    case ? *: tv => Select[K, KS, tv]

  /** Match type to reduce tuple of values to its head. */
  type SelectH[VS <: Tuple] = VS match
    case h *: ? => h

  /* Constructs typed record while constraining its keys and values. */
  private[schema] def apply[KS <: Tuple, VS <: Tuple](keys: KS, values: VS, lineNum: Int, rowNum: Int)(using
    ev1: Tuple.Size[KS] =:= Tuple.Size[VS],
    ev2: Tuple.Union[KS] <:< Key
  ): TypedRecord[KS, VS] =
    new TypedRecord[KS, VS](keys, values, lineNum, rowNum)
