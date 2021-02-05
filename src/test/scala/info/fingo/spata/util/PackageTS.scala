/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.util

import org.scalatest.funsuite.AnyFunSuite

class PackageTS extends AnyFunSuite {

  test("classLabel retrieves class/object name correctly") {

    class Regular
    assert(classLabel(new Regular()) == "regular")

    object Regular
    assert(classLabel(Regular) == "regular")

    class Outer { class Inner; object Inner }
    val outer = new Outer()
    assert(classLabel(new outer.Inner()) == "inner")
    assert(classLabel(outer.Inner) == "inner")

    object Outer { class Inner; object Inner }
    assert(classLabel(new Outer.Inner()) == "inner")
    assert(classLabel(Outer.Inner) == "inner")

    object Context {
      trait Anonymous
      assert(classLabel(new Anonymous() {}) == "context")
    }
  }
}
