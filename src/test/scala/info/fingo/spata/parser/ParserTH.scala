/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.parser

import CharParser.{CharFailure, CharState}
import CharParser.CharPosition._
import ParsingErrorCode._
import FieldParser.{FieldFailure, RawField}

object Config {
  val sep = ','
  val rs = '\n'
  val qt = '"'
  val limit = 100
  val nl: Char = CharParser.LF
  val cr: Char = CharParser.CR
  val sp = ' '
  val end: Char = CharParser.ETX
}

object CharStates {
  import Config._
  def csr(c: Char): CharState = CharState(Right(c), Regular)
  def csr: CharState = CharState(Left(cr), Regular)
  def cst(c: Char): CharState = CharState(Right(c), Trailing)
  def csq(c: Char): CharState = CharState(Right(c), Quoted)
  def csq: CharState = CharState(Left(qt), Quoted)
  val cses: CharState = CharState(Left(qt), Escape)
  val css: CharState = CharState(Left(sp), Start)
  def cse: CharState = CharState(Left(sp), End)
  def csff: CharState = CharState(Left(sep), FinishedField)
  def csfr: CharState = CharState(Left(rs), FinishedRecord)
  def csf: CharState = CharState(Left(end), FinishedRecord)
}

object CharFailures {
  val cfcq: CharFailure = CharFailure(UnclosedQuotation)
  val cfmq: CharFailure = CharFailure(UnmatchedQuotation)
  val cfeq: CharFailure = CharFailure(UnescapedQuotation)
}

object RawFields {
  def rf(v: String, pos: Int, ln: Int = 1): RawField = RawField(v, Location(pos, ln))
  def rfe(v: String, pos: Int, ln: Int = 1): RawField = RawField(v, Location(pos, ln), endOfRecord = true)
}

object FieldFailures {
  def ffcq(pos: Int, ln: Int = 1): FieldFailure = FieldFailure(UnclosedQuotation, Location(pos, ln))
  def ffmq(pos: Int, ln: Int = 1): FieldFailure = FieldFailure(UnmatchedQuotation, Location(pos, ln))
  def ffeq(pos: Int, ln: Int = 1): FieldFailure = FieldFailure(UnescapedQuotation, Location(pos, ln))
  def ffrtl(pos: Int, ln: Int = 1): FieldFailure = FieldFailure(FieldTooLong, Location(pos, ln))
}

object RecordResults {
  import RecordParser._
  def rr(fields: String*)(pos: Int, ln: Int = 1, rnum: Int = 1): RawRecord =
    RawRecord(Vector(fields: _*), Location(pos, ln), rnum)
  def rfl(code: ErrorCode, pos: Int, ln: Int = 1, rnum: Int = 1, fnum: Int = 1): RecordFailure =
    RecordFailure(code, Location(pos, ln), rnum, fnum)
}
