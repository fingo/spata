package info.fingo.spata.convert

import info.fingo.spata.{CSVRecord, Maybe}
import info.fingo.spata.text.StringParser
import shapeless.{::, HList, HNil, Lazy, Witness}
import shapeless.labelled.{field, FieldType}

/* Converter from CSVRecord to specific HList */
private[spata] trait RecordToHList[L <: HList] {
  def apply(r: CSVRecord): Maybe[L]
}

/* Implicits to convert CSVRecord to HNil and recursively to :: */
private[spata] object RecordToHList {

  implicit val toHNil: RecordToHList[HNil] = _ => Right(HNil)

  implicit def toHCons[K <: Symbol, V, T <: HList](
    implicit witness: Witness.Aux[K], // required to carry field information (name as singleton type)
    parser: StringParser[V],
    rToHL: Lazy[RecordToHList[T]]
  ): RecordToHList[FieldType[K, V] :: T] =
    record =>
      for {
        value <- record.seek(witness.value.name)
        tail <- rToHL.value(record)
      } yield field[K][V](value) :: tail
}
