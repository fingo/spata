/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.text

import java.text.{DecimalFormat, NumberFormat}
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal
import scala.math.ScalaNumber

trait StringRenderer[-A] {
  def apply(value: A): String
}

trait FormattedStringRenderer[-A, B] extends StringRenderer[A] {
  def apply(value: A, fmt: B): String
}

object StringRenderer {

  class Pattern[A]() {
    def apply[B](value: A, fmt: B)(implicit renderer: FormattedStringRenderer[A, B]): String = renderer(value, fmt)
  }

  def render[A](value: A)(implicit renderer: StringRenderer[A]): String = renderer(value)

  def render[A]: Pattern[A] = new Pattern[A]

  implicit def optionRenderer[A](implicit renderer: StringRenderer[A]): StringRenderer[Option[A]] =
    (value: Option[A]) => value.map(renderer.apply).getOrElse("")

  implicit def optionRendererFmt[A, B](
    implicit renderer: FormattedStringRenderer[A, B]
  ): FormattedStringRenderer[Option[A], B] =
    new FormattedStringRenderer[Option[A], B] {
      override def apply(value: Option[A]): String = value.map(renderer(_)).getOrElse("")
      override def apply(value: Option[A], fmt: B): String = value.map(renderer(_, fmt)).getOrElse("")
    }

  implicit val stringRenderer: StringRenderer[String] = (value: String) => Option(value).getOrElse("")

  implicit val intRenderer: StringRenderer[Int] = (value: Int) => value.toString

  implicit val numberRendererFmt: FormattedStringRenderer[Number, DecimalFormat] =
    new FormattedStringRenderer[Number, DecimalFormat] {
      override def apply(value: Number): String = renderAnyRef(value)
      override def apply(value: Number, fmt: DecimalFormat): String = renderNumber(value, fmt)
    }

  implicit val scalaNumberRendererFmt: FormattedStringRenderer[ScalaNumber, DecimalFormat] =
    new FormattedStringRenderer[ScalaNumber, DecimalFormat] {
      override def apply(value: ScalaNumber): String = renderAnyRef(value)
      override def apply(value: ScalaNumber, fmt: DecimalFormat): String = renderNumber(value, fmt)
    }

  implicit val longRendererFmt: FormattedStringRenderer[Long, NumberFormat] =
    new FormattedStringRenderer[Long, NumberFormat] {
      override def apply(value: Long): String = renderAnyRef(value)
      override def apply(value: Long, fmt: NumberFormat): String = renderNumber(value, fmt)
    }

  implicit val doubleRendererFmt: FormattedStringRenderer[Double, DecimalFormat] =
    new FormattedStringRenderer[Double, DecimalFormat] {
      override def apply(value: Double): String = renderAnyRef(value)
      override def apply(value: Double, fmt: DecimalFormat): String = renderNumber(value, fmt)
    }

  implicit val temporalRendererFmt: FormattedStringRenderer[Temporal, DateTimeFormatter] =
    new FormattedStringRenderer[Temporal, DateTimeFormatter] {
      override def apply(value: Temporal): String = renderAnyRef(value)
      override def apply(value: Temporal, fmt: DateTimeFormatter): String = fmt.format(value)
    }

  implicit val booleanRendererFmt: FormattedStringRenderer[Boolean, BooleanFormatter] =
    new FormattedStringRenderer[Boolean, BooleanFormatter] {
      override def apply(value: Boolean): String = renderAnyRef(value)
      override def apply(value: Boolean, fmt: BooleanFormatter): String = fmt.format(value)
    }

  private def renderAnyRef[A](anyRef: A): String = Option(anyRef).fold("")(_.toString)

  private def renderNumber(num: Number, fmt: NumberFormat): String = fmt.format(num)
}
