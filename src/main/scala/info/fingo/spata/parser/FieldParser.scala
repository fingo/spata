package info.fingo.spata.parser

import cats.effect.IO
import fs2.{Pipe, Pull, Stream}
import ParsingErrorCode._

private[spata] class FieldParser(fieldSizeLimit: Option[Int]) {
  import FieldParser._
  import CharParser._
  import CharParser.CharPosition._

  def toFields(counters: Location = Location(0)): Pipe[IO, CharResult, FieldResult] = {

    def loop(
      chars: Stream[IO, CharResult],
      sb: StringBuilder,
      counters: Location,
      lc: LocalCounts
    ): Pull[IO, FieldResult, Unit] =
      if (fieldTooLong(lc))
        Pull.output1(fail(FieldTooLong, counters, lc)) >> Pull.done
      else
        chars.pull.uncons1.flatMap {
          case Some((h, t)) =>
            h match {
              case cs: CharState if cs.finished =>
                val field = RawField(sb.toString().dropRight(lc.toTrim), counters, cs.position == FinishedRecord)
                val newCounters = recalculateCounters(counters, cs)
                Pull.output1(field) >> loop(
                  t,
                  new StringBuilder(),
                  newCounters,
                  LocalCounts(field.counters.nextPosition)
                )
              case cs: CharState =>
                cs.char.map(sb.append)
                val newCounters = recalculateCounters(counters, cs)
                val newLC = recalculateLocalCounts(lc, cs)
                loop(t, sb, newCounters, newLC)
              case cf: CharFailure => Pull.output1(fail(cf.code, counters, lc)) >> Pull.done
            }
          case None => Pull.done
        }

    val sb = new StringBuilder()
    chars => loop(chars, sb, counters, LocalCounts(Location(0))).stream
  }

  private def fail(error: ErrorCode, counters: Location, lc: LocalCounts): FieldFailure = {
    val rc = recalculateCountersAtFailure(error, counters, lc)
    FieldFailure(error, rc)
  }

  private def recalculateCounters(counters: Location, cs: CharState): Location =
    if (cs.isNewLine) counters.nextLine
    else counters.nextPosition

  private def recalculateLocalCounts(lc: LocalCounts, cs: CharState): LocalCounts =
    cs.position match {
      case Start => lc.incLeading()
      case End => lc.incTrailing()
      case Trailing => lc.incTrimming()
      case _ => if (cs.hasChar) lc.incCharacters() else lc.resetTrimming()
    }

  private def recalculateCountersAtFailure(error: ErrorCode, counters: Location, lc: LocalCounts): Location =
    error match {
      case UnclosedQuotation => counters.nextPosition
      case UnescapedQuotation => counters.add(position = -lc.trailSpaces)
      case UnmatchedQuotation => lc.origin.add(position = lc.leadSpaces + 1)
      case _ => counters
    }

  private def fieldTooLong(lc: LocalCounts): Boolean =
    fieldSizeLimit.exists(_ < lc.characters)
}

private[spata] object FieldParser {
  sealed trait FieldResult
  case class FieldFailure(code: ErrorCode, counters: Location) extends FieldResult
  case class RawField(value: String, counters: Location, endOfRecord: Boolean = false) extends FieldResult

  case class LocalCounts(
    origin: Location,
    characters: Int = 0,
    leadSpaces: Int = 0,
    trailSpaces: Int = 0,
    toTrim: Int = 0
  ) {
    def incCharacters(): LocalCounts = copy(characters = this.characters + 1, toTrim = 0)
    def incLeading(): LocalCounts = copy(leadSpaces = this.leadSpaces + 1, toTrim = 0)
    def incTrailing(): LocalCounts = copy(trailSpaces = this.trailSpaces + 1, toTrim = 0)
    def incTrimming(): LocalCounts = copy(characters = this.characters + 1, toTrim = this.toTrim + 1)
    def resetTrimming(): LocalCounts = copy(toTrim = 0)
  }
}
