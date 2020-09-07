/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.parser

/* Error codes provided by parsing functions. */
private[spata] object ParsingErrorCode {

  sealed abstract class ErrorCode(val message: String) {

    def code: String = {
      val name = this.getClass.getSimpleName.stripSuffix("$")
      val first = name.take(1)
      name.replaceFirst(first, first.toLowerCase)
    }
  }

  case object UnclosedQuotation extends ErrorCode("Bad format: not enclosed quotation")
  case object UnescapedQuotation extends ErrorCode("Bad format: not escaped quotation")
  case object UnmatchedQuotation extends ErrorCode("Bad format: unmatched quotation (premature end of file)")
  case object FieldTooLong extends ErrorCode("Value is longer than provided maximum (unmatched quotation?)")
  case object MissingHeader extends ErrorCode("Header not found (empty content?)")
  case object WrongNumberOfFields extends ErrorCode("Number of values doesn't match header size or previous records")
}
