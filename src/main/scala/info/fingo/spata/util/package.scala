/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

/** Package with utility classes */
package object util {

  /* Gets short class identifier. Used e.g. for error codes. */
  private[spata] def classLabel(obj: AnyRef): String = {
    val clazz = obj.getClass
    // Get simple name if present (works well for inner classes), use full name if needed (anonymous classes)
    val name = if (clazz.getSimpleName.isEmpty) clazz.getName.split('.').last else clazz.getSimpleName
    val first = name.take(1)
    name.takeWhile(_ != '$').replaceFirst(first, first.toLowerCase)
  }
}
