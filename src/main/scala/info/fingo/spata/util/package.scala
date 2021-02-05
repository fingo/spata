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

    def getSimpleName(cls: Class[_]): String =
      Option(cls).map { c =>
        if (c.getSimpleName.nonEmpty) c.getSimpleName
        else getSimpleName(c.getEnclosingClass)
      }.getOrElse("?")

    val name = getSimpleName(obj.getClass)
    val head = name.take(1).toLowerCase
    val tail = name.takeWhile(_ != '$').tail
    head + tail
  }
}
