/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema

import org.scalatest.funsuite.AnyFunSuite

class TypedRecordTS extends AnyFunSuite:

  test("Typed record allows type-safe access to its values") {
    val name = "Mumintrollet"
    val r1 = TypedRecord(("id", "name"): ("id", "name"), (1, name), 1, 1)
    assert(r1("id") == 1)
    assert(r1("name") == name)
    val r2 = TypedRecord(("pi", "id", "name", "yes"): ("pi", "id", "name", "yes"), (3.14, 1, name, true), 1, 1)
    assert(r2("id") == 1)
    assert(r2("name") == name)
    val r3 = TypedRecord(("zero", "id", "answer", "name"): ("zero", "id", "answer", "name"), (0, 1, 42, name), 1, 1)
    assert(r3("id") == 1)
    assert(r3("name") == name)
  }

  test("Typed record prevent wrongly-typed access to its values") {
    val r = TypedRecord(("title", "year"): ("title", "year"), ("Dark Side of the Moon", 1973), 1, 1)
    assertCompiles("""val year: Int = r("year")""")
    assertDoesNotCompile("""val year: String = r("year")""")
    assertDoesNotCompile("""val year: Int = r("day")""")
  }

  test("Typed record allows conversion to case classes") {
    case class FullData(id: Int, code: String, name: String, description: Option[String], inventory: Int)
    case class PartialData(id: Int, name: String)
    val r = TypedRecord(
      ("id", "inventory", "code", "name", "description"): ("id", "inventory", "code", "name", "description"),
      (1, 100, "MX1", "Mask X1", None: Option[String]),
      1,
      1
    )
    val fd = r.to[FullData]
    val pd = r.to[PartialData]
    assert(fd.id == r("id"))
    assert(fd.name == r("name"))
    assert(pd.id == r("id"))
    assert(pd.name == r("name"))
  }

  test("Typed record prevents convertion to incompatible case classes") {
    case class Correct(id: Int, code: String, name: String)
    case class WrongType(id: Int, code: Int, name: String)
    case class WrongKey(id: Int, cod: String, name: String)
    val r = TypedRecord(("id", "code", "name"): ("id", "code", "name"), (1, "MX1", "Mask X1"), 1, 1)
    assertCompiles("r.to[Correct]")
    assertDoesNotCompile("r.to[WrongType]")
    assertDoesNotCompile("r.to[WrongKey]")
  }

  test("Typed record allows conversion to tuples") {
    type Data = (Int, Int, String)
    val r = TypedRecord(("_1", "_2", "_3"): ("_1", "_2", "_3"), (1, 100, "MX1"), 1, 1)
    val (v1, v2, v3) = r.to[Data]
    assert(v1 == r("_1"))
    assert(v2 == r("_2"))
    assert(v3 == r("_3"))
  }

  test("Typed record can be empty") {
    val r = TypedRecord(EmptyTuple, EmptyTuple, 1, 1)
    assert(r.rowNum == 1)
  }
