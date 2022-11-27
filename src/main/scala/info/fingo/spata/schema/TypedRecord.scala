/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import scala.deriving.Mirror
import scala.compiletime.{constValue, erasedValue}
import info.fingo.spata.schema.TypedRecord._

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
  * @param keys actual record's keys
  * @param values actual record's values
  * @param lineNum last line number in source file this record is built from
  * @param rowNum row number in source file this record comes from
  * @tparam KS keys type - tuple
  * @tparam VS values type - tuple
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
    * @tparam K the key type - this is a singleton type representing the key
    * @return field value
    */
  def apply[K <: Key](key: K): Select[K, KS, VS] = get(key, keys, values)

  /** Converts typed record to a case class.
    *
    * Because typed record has already got all the values properly typed, it may be safely converted to a case class.
    * Assuming, that the record has a field `name` of type `String` and a field birthdate of type `LocalDate`,
    * the conversion is as simple as that:
    * {{{
    * case class Person(name: String, birthdate: LocalDate)
    * val person = record.to[Person]
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
  inline def to[P <: Product](using
    m: Mirror.ProductOf[P],
    ev: Tuple.Union[Tuple.Zip[m.MirroredElemLabels, m.MirroredElemTypes]] <:< Tuple.Union[Tuple.Zip[KS, VS]]
  ): P =
    val labels = getLabels[m.MirroredElemLabels]
    val vals = labels.map(l => get(l, keys, values))
    m.fromProduct(Tuple.fromArray(vals.toArray))

  private def get[K <: Key, KS <: Tuple, VS <: Tuple](key: K, keys: KS, values: VS): Select[K, KS, VS] =
    (keys: @unchecked) match
      case `key` *: _: *:[K @unchecked, _] => getH(values)
      case _ *: tk: *:[_, _] => getT(key, tk, values)

  private def getH[VS <: Tuple](values: VS): SelectH[VS] = (values: @unchecked) match
    case h *: _: *:[_, _] => h

  private def getT[K <: Key, KS <: Tuple, VS <: Tuple](key: K, keys: KS, values: VS): SelectT[K, KS, VS] =
    (values: @unchecked) match
      case _ *: t: *:[_, _] => get(key, keys, t)

  private inline def getLabels[T <: Tuple]: List[String] = inline erasedValue[T] match
    case _: EmptyTuple => Nil
    case _: (t *: ts) => constValue[t].toString :: getLabels[ts]

/** Typed record helper object. */
object TypedRecord:
  type SelectH[VS <: Tuple] = VS match
    case h *: _ => h

  type SelectT[K <: Key, KS <: Tuple, VS <: Tuple] = VS match
    case _ *: tv => Select[K, KS, tv]

  type Select[K <: Key, KS <: Tuple, VS <: Tuple] = KS match
    case K *: t => SelectH[VS]
    case h *: t => SelectT[K, t, VS]

  /* Constructs typed record */
  private[schema] def apply[KS <: Tuple, VS <: Tuple](keys: KS, values: VS, lineNum: Int, rowNum: Int)(using
    ev1: Tuple.Size[KS] =:= Tuple.Size[VS],
    ev2: Tuple.Union[KS] <:< Key
  ): TypedRecord[KS, VS] =
    new TypedRecord[KS, VS](keys, values, lineNum, rowNum)
