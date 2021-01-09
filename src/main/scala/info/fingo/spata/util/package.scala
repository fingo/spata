/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

/** Package with utility classes */
package object util {

  /* Gets short class identifier. Used e.g. for error codes. */
  private[spata] def classId(obj: AnyRef): String = {
    val name = obj.getClass.getSimpleName.stripSuffix("$")
    val first = name.take(1)
    name.replaceFirst(first, first.toLowerCase)
  }
}
