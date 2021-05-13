/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.text

import java.text.{DecimalFormat, NumberFormat}
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal

/** Renderer from provided type to `String`.
  * It is similar to `toString`, although it should handle nulls and unwrap `Option` content.
  *
  * This renderer defines behavior to be implemented by concrete, implicit renderers for various types,
  * used by [[StringRenderer.render[A]* render]] function from [[StringRenderer$ StringRenderer]] object.
  *
  * Renderer's contravariance allows limiting implicits to parent types if subclasses are formatted in the same way.
  *
  * If the input is null, all provided implicit implementation return empty string as rendering result.
  * If the input is a [[scala.Option]], provided implicits render its content if or return empty string.
  *
  * @tparam A source type for rendering
  */
trait StringRenderer[-A] {

  /** Renders provided value to string.
    *
    * @note This function uses "standard" string formatting,
    * e.g. point as decimal separator or ISO date and time formats, without any locale support.
    * Use [[FormattedStringRenderer]] if more control over target format is required.
    *
    * @param value the input value
    * @return rendered string
    */
  def apply(value: A): String
}

/** Renderer from provided type to `String` with support for different formats.
  *
  * This renderer defines behavior to be implemented by concrete, implicit renderers for various types,
  * used by [[StringRenderer.render[A,B]* render]] function from [[StringRenderer$ StringRenderer]] object.
  *
  * Renderer's contravariance allows limiting implicits to parent types if subclasses are formatted in the same way,
  * using the same formatter type.
  *
  * If the input is null, all provided implicit implementation return empty string as rendering result.
  * If the input is a [[scala.Option]], provided implicits render its content if or return empty string.
  *
  * @tparam A source type for rendering
  * @tparam B type of formatter
  */
trait FormattedStringRenderer[-A, B] extends StringRenderer[A] {

  /** Renders provided value to string based on provided format.
    *
    * @param value the input value
    * @param fmt formatter, specific for particular input type, e.g. `DateTimeFormatter` for dates and times
    * @return rendered string
    */
  def apply(value: A, fmt: B): String
}

/** Rendering methods from various simple types to `String`.
  *
  * Contains renderers for common types, like numbers, dates and times.
  *
  * Additional renderers may be provided by implementing [[StringRenderer]] or [[FormattedStringRenderer]] traits
  * and putting implicit values in scope.
  * `StringRenderer` may be implemented if there are no different formatting options for given type, e.g. for integers.
  * For all cases when different formatting options exist, `FormattedStringRenderer` should be implemented.
  */
object StringRenderer {

  /** Renders desired type to string using default format.
    *
    * @example {{{
    * import info.fingo.spata.text.StringRenderer._
    * val x = render(123.45)
    * val y = render(Some(123.45))
    * }}}
    * Both values, `x` and `y` will be set to `"123.45"`.
    *
    * @param value the value to render
    * @param renderer the renderer for specific source type
    * @tparam A the source type
    * @return rendered string
    */
  def render[A](value: A)(implicit renderer: StringRenderer[A]): String = renderer(value)

  /** Renders desired type to string based on provided format.
    *
    * @param value the value to render
    * @param fmt formatter specific for particular input type, e.g. `DateTimeFormatter` for dates and times
    * @param renderer the renderer for specific source type
    * @tparam A the source type
    * @tparam B type of formatter
    * @return rendered string
    */
  def render[A, B](value: A, fmt: B)(implicit renderer: FormattedStringRenderer[A, B]): String = renderer(value, fmt)

  /** Renderer for optional values.
    * Allows conversion of any simple renderer to accept `Option[A]` instead of `A`,
    * rendering the value from inside the option or returning empty string if not defined.
    *
    * @param renderer the renderer for underlying simple type
    * @tparam A the simple type wrapped by [[scala.Option]]
    * @return renderer which accepts optional values
    */
  implicit def optionRenderer[A](implicit renderer: StringRenderer[A]): StringRenderer[Option[A]] =
    (value: Option[A]) => value.map(renderer.apply).getOrElse("")

  /** Renderer for optional values with support for different formats.
    * Allows conversion of any simple renderer to accept `Option[A]` instead of `A`,
    * rendering the value from inside the option or returning empty string if not defined.
    *
    * @param renderer the renderer for underlying simple type
    * @tparam A the simple type wrapped by [[scala.Option]]
    * @tparam B type of formatter
    * @return renderer which support formatted input and ccepts optional values
    */
  implicit def optionRendererFmt[A, B](
    implicit renderer: FormattedStringRenderer[A, B]
  ): FormattedStringRenderer[Option[A], B] =
    new FormattedStringRenderer[Option[A], B] {
      override def apply(value: Option[A]): String = value.map(renderer(_)).getOrElse("")
      override def apply(value: Option[A], fmt: B): String = value.map(renderer(_, fmt)).getOrElse("")
    }

  /** Renderer for string. Return the original string or empty one for null. */
  implicit val stringRenderer: StringRenderer[String] = (value: String) => Option(value).getOrElse("")

  /** Renderer for integer values. */
  implicit val intRenderer: StringRenderer[Int] = (value: Int) => value.toString

  /** Renderer for long values with support for formats. */
  implicit val longRendererFmt: FormattedStringRenderer[Long, NumberFormat] =
    new FormattedStringRenderer[Long, NumberFormat] {
      override def apply(value: Long): String = renderAnyRef(value)
      override def apply(value: Long, fmt: NumberFormat): String = renderNumber(value, fmt)
    }

  /** Renderer for double values with support for formats. */
  implicit val doubleRendererFmt: FormattedStringRenderer[Double, DecimalFormat] =
    new FormattedStringRenderer[Double, DecimalFormat] {
      override def apply(value: Double): String = renderAnyRef(value)
      override def apply(value: Double, fmt: DecimalFormat): String = renderNumber(value, fmt)
    }

  /** Renderer for numeric values (BigDecimal`, `BigInt` and various Java classes) with support for formats. */
  implicit val numberRendererFmt: FormattedStringRenderer[Number, DecimalFormat] =
    new FormattedStringRenderer[Number, DecimalFormat] {
      override def apply(value: Number): String = renderAnyRef(value)
      override def apply(value: Number, fmt: DecimalFormat): String = renderNumber(value, fmt)
    }

  /** Renderer for time/date values with support for formats. */
  implicit val temporalRendererFmt: FormattedStringRenderer[Temporal, DateTimeFormatter] =
    new FormattedStringRenderer[Temporal, DateTimeFormatter] {
      override def apply(value: Temporal): String = renderAnyRef(value)
      override def apply(value: Temporal, fmt: DateTimeFormatter): String = fmt.format(value)
    }

  /** Renderer for boolean values with support for formats. */
  implicit val booleanRendererFmt: FormattedStringRenderer[Boolean, BooleanFormatter] =
    new FormattedStringRenderer[Boolean, BooleanFormatter] {
      override def apply(value: Boolean): String = renderAnyRef(value)
      override def apply(value: Boolean, fmt: BooleanFormatter): String = fmt.format(value)
    }

  /* Helper method to render safely values with could be null. */
  private def renderAnyRef[A](anyRef: A): String = Option(anyRef).fold("")(_.toString)

  /* Helper method to render numbers */
  private def renderNumber(num: Number, fmt: NumberFormat): String = fmt.format(num)
}
