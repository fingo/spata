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
}

object CharStates {
  def csr(c: Char) = CharState(Some(c), Regular)
  def csr = CharState(None, Regular)
  def cst(c: Char) = CharState(Some(c), Trailing)
  def csq(c: Char) = CharState(Some(c), Quoted)
  def csq = CharState(None, Quoted)
  val cses = CharState(None, Escape)
  val css = CharState(None, Start)
  def cse = CharState(None, End)
  def csff = CharState(None, FinishedField)
  def csfr = CharState(None, FinishedRecord)
}

object CharFailures {
  val cfcq = CharFailure(UnclosedQuotation)
  val cfmq = CharFailure(UnmatchedQuotation)
  val cfeq = CharFailure(UnescapedQuotation)
}

object RawFields {
  def rf(v: String, pos: Int, ln: Int = 1) =
    RawField(v, Location(pos, ln))
  def rfe(v: String, pos: Int, ln: Int = 1) =
    RawField(v, Location(pos, ln), endOfRecord = true)
}

object FieldFailures {
  def ffcq(pos: Int, ln: Int = 1) =
    FieldFailure(UnclosedQuotation, Location(pos, ln))
  def ffmq(pos: Int, ln: Int = 1) =
    FieldFailure(UnmatchedQuotation, Location(pos, ln))
  def ffeq(pos: Int, ln: Int = 1) =
    FieldFailure(UnescapedQuotation, Location(pos, ln))
  def ffrtl(pos: Int, ln: Int = 1) =
    FieldFailure(FieldTooLong, Location(pos, ln))
}

object ParsingResults {
  def rr(fields: String*)(pos: Int, ln: Int = 1, rnum: Int = 1) =
    RawRecord(Vector(fields: _*), Location(pos, ln), rnum)
  def pf(code: ErrorCode, pos: Int, ln: Int = 1, rnum: Int = 1, fnum: Int = 1) =
    ParsingFailure(code, Location(pos, ln), rnum, fnum)
}
