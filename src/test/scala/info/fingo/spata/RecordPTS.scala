/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata

import java.text.NumberFormat
import java.time.LocalDate
import java.util.Locale
import org.scalameter.{Bench, Gen}
import org.scalameter.Key.exec
import org.scalameter.picklers.noPickler.*

/* Check performance of record */
object RecordPTS extends Bench.LocalTime:

  val amount = 25_000
  val recordSize = 10
  val wideRecordSize = 100
  val sampleSize = 1000
  private val locale = new Locale("pl", "PL")

  case class Sample(
    text1: String,
    int1: Int,
    date: LocalDate,
    decimal: BigDecimal,
    dbl1: Double,
    text2: String,
    bool: Boolean,
    int2: Int,
    long: Long,
    dbl2: Double
  )

  type SampleT = (String, Int, LocalDate, BigDecimal, Double, String, Boolean, Int, Long, Double)

  private val header = makeHeader(recordSize)
  private val values = (1 to sampleSize).map(s => makeValues(s, recordSize))
  private val pairs = values.map(v => header.names.zip(v))
  private val date = LocalDate.now
  private val decimal = BigDecimal(123.45)
  private val classes =
    values.map(v => Sample(v(1), v.head.toInt, date, decimal, 3.14, "another text", true, 0, 99999L, 2.72))
  private val tuples = values.map(v => (v(1), v.head.toInt, date, decimal, 3.14, "another text", true, 0, 99999L, 2.72))
  private val records = classes.map(c => Record.from(c))

  private val wideHeader = makeHeader(wideRecordSize)
  private val wideValues = (1 to sampleSize).map(s => makeValues(s, wideRecordSize))
  private val wideRecords = wideValues.map(vs => Record(vs*)(wideHeader))

  performance.of("record").config(exec.maxWarmupRuns := 1, exec.benchRuns := 3) in {
    measure.method("create") in {
      using(creationModes) in {
        case "with_header" =>
          (1 to amount).map(i => Record(values(i % sampleSize)*)(header)).foreach(effect)
        case "no_header" =>
          (1 to amount).map(i => Record.fromValues(values(i % sampleSize)*)).foreach(effect)
        case "from_pairs" =>
          (1 to amount).map(i => Record.fromPairs(pairs(i % sampleSize)*)).foreach(effect)
        case "build" =>
          (1 to amount)
            .map(i =>
              Record.builder
                .add("header-key-1", values(i % sampleSize)(1))
                .add("header-key-2", values(i % sampleSize).head.toInt)
                .add("header-key-3", date)
                .add("header-key-4", decimal)
                .add("header-key-5", 3.14)
                .add("header-key-6", "another text")
                .add("header-key-7", true)
                .add("header-key-8", 0)
                .add("header-key-9", 99999L)
                .add("header-key-10", 2.72)
                .get
            )
            .foreach(effect)
        case "from_class" =>
          (1 to amount).map(i => Record.from(classes(i % sampleSize))).foreach(effect)
        case "from_tuple" =>
          (1 to amount).map(i => Record.from(tuples(i % sampleSize))).foreach(effect)
        case "extend_header" =>
          val partialHeader = makeHeader(recordSize / 2)
          (1 to amount).map(i => Record(values(i % sampleSize)*)(partialHeader)).foreach(effect)
        case "build_wide" =>
          (1 to amount)
            .map(i =>
              (0 to wideRecordSize)
                .foldLeft(Record.builder) { case (builder, index) =>
                  val hdr = s"header-key-$index"
                  index % 10 match
                    case 0 => builder.add(hdr, values(i % sampleSize)(1))
                    case 1 => builder.add(hdr, values(i % sampleSize).head.toInt)
                    case 2 => builder.add(hdr, date)
                    case 3 => builder.add(hdr, decimal)
                    case 4 => builder.add(hdr, 3.14)
                    case 5 => builder.add(hdr, "another text")
                    case 6 => builder.add(hdr, true)
                    case 7 => builder.add(hdr, 0)
                    case 8 => builder.add(hdr, 99999L)
                    case 9 => builder.add(hdr, 2.72)
                }
                .get
            )
            .foreach(effect)
      }
    }
    measure.method("update") in {
      using(updateModes) in {
        case "by_key" =>
          (1 to amount).map(i => records(i % sampleSize).updated("text2", "new value")).foreach(effect)
        case "by_index" =>
          (1 to amount).map(i => records(i % sampleSize).updated(6, "new value")).foreach(effect)
        case "function" =>
          def fun: String => String = identity
          (1 to amount).map(i => records(i % sampleSize).updatedWith("text2")(fun)).foreach(effect)
        case "altered" =>
          def fun: Int => Int = x => 2 * x
          (1 to amount).map(i => records(i % sampleSize).altered("int1")(fun)).foreach {
            case Right(r) => effect(r)
            case Left(_) => throw new RuntimeException("Exception in altered")
          }
        case "patch" =>
          (1 to amount)
            .map(i => records(i % sampleSize).patch.add("text3", "new value").remove("dbl2").get)
            .foreach(effect)
        case "altered_wide" =>
          def fun: Int => Int = x => 2 * x
          val key = s"header-key-${wideRecordSize / 2 + 1}"
          (1 to amount).map(i => wideRecords(i % sampleSize).altered(key)(fun)).foreach {
            case Right(r) => effect(r)
            case Left(_) => throw new RuntimeException("Exception in altered")
          }
        case "patch_wide" =>
          val key = s"header-key-${wideRecordSize / 2}"
          (1 to amount)
            .map(i => wideRecords(i % sampleSize).patch.add("header-key-new", "new value").remove(key).get)
            .foreach(effect)
      }
    }
    measure.method("get") in {
      using(getModes) in {
        case "by_index" =>
          (1 to amount).map(i => records(i % sampleSize)(recordSize / 2)).foreach(identity)
        case "by_key" =>
          (1 to amount).map(i => records(i % sampleSize)("text2")).foreach(identity)
        case "typed" =>
          (1 to amount).map(i => records(i % sampleSize).get[Long]("long")).foreach(identity)
        case "formatted" =>
          val format = NumberFormat.getInstance(locale)
          (1 to amount).map(i => records(i % sampleSize).get[Long]("long", format)).foreach(identity)
        case "to_class" =>
          (1 to amount).map(i => records(i % sampleSize).to[Sample]).foreach(identity)
        case "to_tuple" =>
          (1 to amount).map(i => records(i % sampleSize).to[SampleT]).foreach(identity)
        case "typed_wide" =>
          val key = s"header-key-${wideRecordSize / 2 + 1}"
          (1 to amount).map(i => wideRecords(i % sampleSize).get[Int](key)).foreach(identity)
      }
    }
  }

  private def effect(r: Record) = r(0)

  private def makeHeader(size: Int): Header = Header((1 to size).map(i => s"header-key-$i")*)
  private def makeValues(sample: Int, size: Int): Seq[String] =
    (1 to size).map(i => if i % 2 == 0 then s"value-$sample-$i" else s"${i + sample * size}")

  private lazy val creationModes = Gen.enumeration("creation_mode")(
    "with_header",
    "no_header",
    "from_pairs",
    "build",
    "from_class",
    "from_tuple",
    "extend_header",
    "build_wide"
  )
  private lazy val updateModes = Gen.enumeration("update_mode")(
    "by_key",
    "by_index",
    "function",
    "altered",
    "patch",
    "altered_wide",
    "patch_wide"
  )
  private lazy val getModes = Gen.enumeration("get_mode")(
    "by_index",
    "by_key",
    "typed",
    "formatted",
    "to_class",
    "to_tuple",
    "typed_wide"
  )
