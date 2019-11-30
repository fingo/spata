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
  def csr(c: Char): CharState = CharState(Some(c), Regular)
  def csr: CharState = CharState(None, Regular)
  def cst(c: Char): CharState = CharState(Some(c), Trailing)
  def csq(c: Char): CharState = CharState(Some(c), Quoted)
  def csq: CharState = CharState(None, Quoted)
  val cses: CharState = CharState(None, Escape)
  val css: CharState = CharState(None, Start)
  def cse: CharState = CharState(None, End)
  def csff: CharState = CharState(None, FinishedField)
  def csfr: CharState = CharState(None, FinishedRecord)
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

object ParsingResults {
  def rr(fields: String*)(pos: Int, ln: Int = 1, rnum: Int = 1): RawRecord =
    RawRecord(Vector(fields: _*), Location(pos, ln), rnum)
  def pf(code: ErrorCode, pos: Int, ln: Int = 1, rnum: Int = 1, fnum: Int = 1): ParsingFailure =
    ParsingFailure(code, Location(pos, ln), rnum, fnum)
}
