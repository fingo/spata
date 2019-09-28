package info.fingo.csv.parser

import scala.collection.immutable.VectorBuilder
import cats.effect.IO
import fs2.{Pipe, Pull, Stream}

private[csv] class RecordParser {

  import FieldParser._

  def toRecords: Pipe[IO,FieldResult,ParsingResult] = {
    def loop(fields: Stream[IO, FieldResult], vb: VectorBuilder[String], recordNum: Int): Pull[IO,ParsingResult,Unit] =
      fields.pull.uncons1.flatMap {
        case Some((h, t)) =>
          h match {
            case rf: RawField =>
              vb += rf.value
              if(rf.endOfRecord) {
                val rr = RawRecord(vb.result(), rf.counters, recordNum)
                if(rr.isEmpty)
                  loop(t, new VectorBuilder[String], recordNum)
                else
                  Pull.output1(rr) >> loop(t, new VectorBuilder[String], recordNum+1)
              } else
                loop(t, vb, recordNum)
            case ff: FieldFailure => Pull.output1(ParsingFailure(ff.code, ff.counters, recordNum, vb.result.size+1)) >> Pull.done
          }
        case None => Pull.done
      }
    fields => loop(fields, new VectorBuilder[String], 1).stream
  }
}
