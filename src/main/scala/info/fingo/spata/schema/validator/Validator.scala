/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.schema.validator

import scala.util.matching.Regex
import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}
import info.fingo.spata.schema.error.ValidationError
import info.fingo.spata.util.classLabel

/** Validator for typed CVS field value.
  *
  * It is used as part of schema validation, after successful conversion of CSV string value to desired type.
  *
  * A bunch of common validators are provided by the library. Additional ones may be created by implementing this trait.
  * The only method that has to be defined by implementing classes is [[isValid]],
  * since reasonable defaults are provided for other methods.
  *
  * @see [[CSVSchema]]
  * @tparam A value type
  */
trait Validator[A] {

  /** Check if provided value does comply with this validator.
    *
    * @param value the value to validate
    * @return true if value is valid, false otherwise
    */
  def isValid(value: A): Boolean

  /** Name of this validator, to be used while providing error information.
    *
    * The default value is based on validator class/object name. Implementing classes may provide their own values.
    * It is recommended to define one if validator is implemented as top-level anonymous class,
    * without meaningful name.
    */
  val name: String = classLabel(this)

  /** Error message for invalid value.
    *
    * Implementing classes may override this method to provide precise error information for invalid values.
    * The default implementation returns generic message containing validated value and validator name.
    *
    * @param value the validated value
    * @return error message parametrized with value
    */
  def errorMessage(value: A) = s"Invalid value [$value] reported by $name"

  /* Main validation method, used by schema validation. Must not alter the value. */
  final private[schema] def apply(value: A): Validated[ValidationError, A] =
    if (isValid(value)) Valid(value)
    else Invalid(ValidationError(name, errorMessage(value)))
}

/** [[Validator]] companion with implicit converter for optional values. */
object Validator {

  /** Implicit converter of regular validator into validator for optional value.
    *
    * Thanks to this conversion a validator for `Option[A]` is available for every `A`,
    * for which a regular validator is available:
    * {{{
    * val value = 100
    * val validator = MinValidator(0)
    * val validPlain = validator.isValid(value)
    * val validSome = validator.isValid(Some(value))
    * val validNone = validator.isValid(None)
    * }}}
    * As you may see from above example, optional validator treats `None` as valid value.
    *
    * @param validator the regular validator
    * @tparam A value type
    * @return validator for `Option[A]`
    */
  implicit def optional[A](validator: Validator[A]): Validator[Option[A]] = new Validator[Option[A]] {
    def isValid(value: Option[A]): Boolean = value.forall(validator.isValid)
    override val name: String = validator.name
  }
}

/** Validator verifying if value is equal to expected one. */
object ExactValidator {

  /** Creates validator to check equality.
    *
    * @param required expected value
    * @return validator
    */
  def apply[A](required: A): Validator[A] = new Validator[A] {
    override def isValid(value: A): Boolean = value == required
    override def errorMessage(value: A): String = s"Value [$value] does not match required one"
  }
}

/** Validator verifying if value is equal to one of expected. */
object OneOfValidator {

  /** Creates validator to check if value matches enumeration.
    *
    * @param allowed allowed values
    * @return validator
    */
  def apply[A](allowed: A*): Validator[A] = new Validator[A] {
    override def isValid(value: A): Boolean = allowed.contains(value)
    override def errorMessage(value: A): String = s"Value [$value] does not match any of allowed"
  }
}

/** Validator verifying if string matches one of expected. The comparison is case insensitive.
  * This validator does not take locale into account and uses `String.equalsIgnoreCase`.
  * The source string is trimmed off white spaces before comparison.
  */
object StringsValidator {

  /** Creates validator to check if string matches enumeration.
    *
    * @param allowed allowed values
    * @return validator
    */
  def apply(allowed: String*): Validator[String] = new Validator[String] {
    override def isValid(value: String): Boolean = allowed.exists(_.equalsIgnoreCase(value.strip))
    override def errorMessage(value: String): String = s"String [$value] does not match any of allowed"
  }
}

/** Validator verifying string maximum length. Takes the length of string after trimming it off white characters.
  * Treats strings with maximum length as valid.
  */
object MinLenValidator {

  /** Creates string minimum length validator.
    *
    * @param min minimum length
    * @return validator
    */
  def apply(min: Int): Validator[String] = new Validator[String] {
    override def isValid(value: String): Boolean = value.strip.length >= min
    override def errorMessage(value: String): String = s"String [$value] is too short"
  }
}

/** Validator verifying string maximum length. Takes the length of string after trimming it off white characters.
  * Treats strings with maximum length as valid.
  */
object MaxLenValidator {

  /** Creates string maximum length validator.
    *
    * @param max maximum length
    * @return validator
    */
  def apply(max: Int): Validator[String] = new Validator[String] {
    override def isValid(value: String): Boolean = value.strip.length <= max
    override def errorMessage(value: String): String = s"String [$value] is too long"
  }
}

/** Validator verifying string length. Takes the length of string after trimming it off white characters.
  * Treats strings with length at boundary as valid.
  */
object LengthValidator {

  /** Creates string length validator.
    *
    * @param min minimum length
    * @param max maximum length
    * @return validator
    */
  def apply(min: Int, max: Int): Validator[String] = new Validator[String] {
    override def isValid(value: String): Boolean = {
      val len = value.strip.length
      len >= min && len <= max
    }
    override def errorMessage(value: String): String = s"String [$value] length is out of range"
  }

  /** Creates string length validator.
    *
    * @param length required length
    * @return validator
    */
  def apply(length: Int): Validator[String] = new Validator[String] {
    override def isValid(value: String): Boolean = value.strip.length == length
    override def errorMessage(value: String): String = s"String [$value] length is incorrect"
  }
}

/** Regular expression validator for strings. */
object RegexValidator {

  /** Creates regex validator.
    *
    * @param regex the regular expression
    * @return validator
    */
  def apply(regex: Regex): Validator[String] = (value: String) => regex.matches(value)

  /** Creates regex validator.
    *
    * @param regex the regular expression
    * @return validator
    */
  def apply(regex: String): Validator[String] = apply(new Regex(regex))
}

/** Minimum value validator for any types with [[scala.Ordering]].
  * Treats values equal to minimum as valid.
  */
object MinValidator {

  /** Creates minimum value validator.
    *
    * @param min minimum value
    * @tparam A value type
    * @return validator
    */
  def apply[A: Ordering](min: A): Validator[A] = new Validator[A] {
    def isValid(value: A): Boolean = implicitly[Ordering[A]].lteq(min, value)
    override def errorMessage(value: A): String = s"Value [$value] is too small"
  }
}

/** Maximum value validator for any types with [[scala.Ordering]].
  * Treats values equal to maximum as valid.
  */
object MaxValidator {

  /** Creates maximum value validator.
    *
    * @param max maximum value
    * @tparam A value type
    * @return validator
    */
  def apply[A: Ordering](max: A): Validator[A] = new Validator[A] {
    def isValid(value: A): Boolean = implicitly[Ordering[A]].gteq(max, value)
    override def errorMessage(value: A): String = "Value [$v] is too large"
  }
}

/** Range validator for any types with [[scala.Ordering]].
  * Treats values equal to minimum or maximum as valid.
  */
object RangeValidator {

  /** Creates range value validator.
    *
    * @param min minimum value
    * @param max maximum value
    * @tparam A value type
    * @return validator
    */
  def apply[A: Ordering](min: A, max: A): Validator[A] = (value: A) => {
    val minV = MinValidator[A](min)
    val maxV = MaxValidator[A](max)
    minV.isValid(value) && maxV.isValid(value)
  }
}

/** Validator verifying if double value is finite. */
object FiniteValidator {

  /** Creates finite value validator.
    *
    * @return validator
    */
  def apply(): Validator[Double] = new Validator[Double] {
    def isValid(value: Double): Boolean = value.isFinite
    override def errorMessage(value: Double) = s"Number [$value] is not finite"
  }
}
