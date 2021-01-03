/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import org.scalatest.funsuite.AnyFunSuite
import shapeless.{HList, HNil}
import shapeless.labelled.field

class TypedRecordTS extends AnyFunSuite {

  test("Typed record allows type-safe access to its values") {
    val name = "Mumintrollet"
    val r1 = tr(trf("id", 1) :: trf("name", name) :: HNil)
    assert(r1("id") == 1)
    assert(r1("name") == name)
    val r2 = tr(trf("pi", 3.14) :: trf("id", 1) :: trf("name", name) :: trf("yes", true) :: HNil)
    assert(r2("id") == 1)
    assert(r2("name") == name)
    val r3 = tr(trf("zero", 0) :: trf("id", 1) :: trf("answer", 42) :: trf("name", name) :: HNil)
    assert(r3("id") == 1)
    assert(r3("name") == name)
  }

  test("Typed record allows conversion to case classes") {
    case class FullData(id: Int, code: String, name: String, description: Option[String], inventory: Int)
    case class PartialData(id: Int, name: String)
    val r =
      tr(
        trf("id", 1)
          :: trf("inventory", 100)
          :: trf("code", "MX1")
          :: trf("name", "Mask X1")
          :: trf("description", None: Option[String])
          :: HNil
      )
    val fd = r.to[FullData]()
    val pd = r.to[PartialData]()
    assert(fd.id == r("id"))
    assert(fd.name == r("name"))
    assert(pd.id == r("id"))
    assert(pd.name == r("name"))
  }

  test("Typed record allows conversion to tuples") {
    type Data = (Int, Int, String)
    val r =
      tr(
        trf("_1", 1)
          :: trf("_2", 100)
          :: trf("_3", "MX1")
          :: HNil
      )
    val data = r.to[Data]()
    assert(data._1 == r("_1"))
    assert(data._2 == r("_2"))
    assert(data._3 == r("_3"))
  }

  test("Typed record can be empty") {
    val r = tr(HNil)
    assert(r.rowNum == 1)
  }

  private def trf[A](key: StrSng, value: A) = field[key.type](value)
  private def tr[L <: HList](values: L) = TypedRecord(values, 1, 1)
}
