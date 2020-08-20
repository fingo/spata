/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.sample

import java.io.FileWriter
import cats.effect.IO
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import info.fingo.spata.CSVParser
import info.fingo.spata.io.reader

/* Samples which write processing results to another CSV file */
class FileITS extends AnyFunSuite {

  test("spata allows data conversion to another file") {
    case class DTV(day: String, tempVar: Double) // diurnal temperature variation
    val parser = CSVParser[IO]() // parser with default configuration and IO effect
    // get stream of CSV records while ensuring source cleanup
    val records = Stream
      .bracket(IO { SampleTH.sourceFromResource(SampleTH.dataFile) })(source => IO { source.close() })
      .through(reader[IO]().by)
      .through(parser.parse)
    // converter and aggregate data, get stream of YTs
    val dtvs = records.filter { record =>
      record("max_temp") != "NaN" && record("min_temp") != "NaN"
    }.map { record =>
      val day = record("sol")
      val max = record.get[Double]("max_temp")
      val min = record.get[Double]("min_temp")
      DTV(day, max - min)
    }
    // write data to output file
    val outFile = SampleTH.getTempFile
    val output = Stream
      .bracket(IO { new FileWriter(outFile) })(writer => IO { writer.close() })
      .flatMap { writer =>
        dtvs.evalMap { dtv =>
          IO(writer.write(s"${dtv.day},${dtv.tempVar}\n"))
        }
      }
      .handleErrorWith { ex =>
        println(s"Error while converting data from ${SampleTH.dataFile} to ${outFile.getPath}: ${ex.getMessage}")
        Stream.empty[IO]
      }
    // assert result and remove temp file - deleteOnExit is unreliable (doesn't work from sbt)
    val checkAndClean = Stream.eval_(IO {
      assert(outFile.length > 16000)
      outFile.delete()
    })
    // run
    output.append(checkAndClean).compile.drain.unsafeRunSync()
  }

  test("spata allows data conversion to another file using for comprehension") {
    case class DTV(day: String, tempVar: Double) // diurnal temperature variation
    val parser = CSVParser.config.get[IO]() // parser with default configuration and IO effect
    val outFile = SampleTH.getTempFile
    val outcome = for {
      // get stream of CSV records while ensuring source cleanup
      source <- Stream.bracket(IO { SampleTH.sourceFromResource(SampleTH.dataFile) })(source => IO { source.close() })
      writer <- Stream.bracket(IO { new FileWriter(outFile) })(writer => IO { writer.close() })
      record <- reader[IO]()
        .read(source)
        .through(parser.parse)
        .filter { record =>
          record("max_temp") != "NaN" && record("min_temp") != "NaN"
        }
      // converter and aggregate data, get stream of YTs
      day = record("sol")
      max = record.get[Double]("max_temp")
      min = record.get[Double]("min_temp")
      dtv = DTV(day, max - min)
      // write data to output file
      out <- Stream
        .eval(IO(writer.write(s"${dtv.day},${dtv.tempVar}\n")))
        .handleErrorWith { ex =>
          println(s"Error while converting data from ${SampleTH.dataFile} to ${outFile.getPath}: ${ex.getMessage}")
          Stream.empty[IO]
        }
    } yield out
    // assert result and remove temp file - deleteOnExit is unreliable (doesn't work from sbt)
    val checkAndClean = Stream.eval_(IO {
      assert(outFile.length > 16000)
      outFile.delete()
    })
    // run
    outcome.append(checkAndClean).compile.drain.unsafeRunSync()
  }
}
