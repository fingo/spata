package info.fingo.spata.converter

import info.fingo.spata.{CSVRecord, Maybe}
import info.fingo.spata.text.StringParser
import shapeless.{::, HList, HNil, Lazy, Witness}
import shapeless.labelled.{field, FieldType}

/** Converter from CSVRecord to specific [[shapeless.HList]].
  *
  * This trait defines behavior to be implemented by concrete, implicit converters.
  *
  * @tparam L type of target `HList`
  */
trait RecordToHList[L <: HList] {

  /** Converts record to [[shapeless.HList]].
    *
    * @param record record to be converted
    * @return either converted `HList` or an exception
    */
  def apply(record: CSVRecord): Maybe[L]
}

/** Implicits to converter CSVRecord to [[shapeless.HNil]] and [[shapeless.::]]. */
object RecordToHList {

  /** Converter to [[shapeless.HNil]] */
  implicit val toHNil: RecordToHList[HNil] = _ => Right(HNil)

  /** Converter to [[shapeless.::]].
    *
    * @param witness carrier of field information (name as singleton type)
    * @param parser parser from string to desired type
    * @param rToHL converter for [[shapeless.HList]] tail
    * @tparam K singleton type representing field name being converted to cons
    * @tparam V target value of field - type of single `HList` element
    * @tparam T type of `HList` tail
    * @return converter from record to `HList`
    */
  implicit def toHCons[K <: Symbol, V, T <: HList](
    implicit witness: Witness.Aux[K],
    parser: StringParser[V],
    rToHL: Lazy[RecordToHList[T]]
  ): RecordToHList[FieldType[K, V] :: T] =
    record =>
      for {
        value <- record.seek(witness.value.name)
        tail <- rToHL.value(record)
      } yield field[K][V](value) :: tail
}
