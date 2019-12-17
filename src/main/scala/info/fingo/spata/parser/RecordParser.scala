package info.fingo.spata.parser

import scala.collection.immutable.VectorBuilder
import cats.effect.IO
import fs2.{Pipe, Pull, Stream}

private[spata] class RecordParser {

  import FieldParser._

  def toRecords: Pipe[IO, FieldResult, ParsingResult] = {

    def loop(fields: Stream[IO, FieldResult], vb: VectorBuilder[String], recNum: Int): Pull[IO, ParsingResult, Unit] =
      fields.pull.uncons1.flatMap {
        case Some((h, t)) =>
          h match {
            case rf: RawField =>
              vb += rf.value
              if (rf.endOfRecord) {
                val rr = RawRecord(vb.result(), rf.counters, recNum)
                if (rr.isEmpty)
                  loop(t, new VectorBuilder[String], recNum)
                else
                  Pull.output1(rr) >> loop(t, new VectorBuilder[String], recNum + 1)
              } else
                loop(t, vb, recNum)
            case ff: FieldFailure =>
              Pull.output1(ParsingFailure(ff.code, ff.counters, recNum, vb.result.size + 1)) >> Pull.done
          }
        case None => Pull.done
      }

    fields => loop(fields, new VectorBuilder[String], 1).stream
  }
}
