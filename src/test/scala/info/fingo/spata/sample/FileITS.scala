/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.sample

import java.io.{File, FileOutputStream}
import cats.effect.IO
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import info.fingo.spata.{CSVParser, CSVRenderer, Record}
import info.fingo.spata.io.{Reader, Writer}

/* Samples which write processing results to another CSV file */
class FileITS extends AnyFunSuite {

  test("spata allows data conversion to another file") {
    case class DTV(day: String, tempVar: Double) // diurnal temperature variation

    def dtvFromRecord(record: Record) =
      for {
        day <- record.get[String]("sol")
        max <- record.get[Double]("max_temp")
        min <- record.get[Double]("min_temp")
      } yield DTV(day, max - min)

    val parser = CSVParser[IO]() // parser with default configuration and IO effect
    // get stream of CSV records while ensuring source cleanup
    val records = Stream
      .bracket(IO { SampleTH.sourceFromResource(SampleTH.dataFile) })(source => IO { source.close() })
      .through(Reader[IO]().by)
      .through(parser.parse)
    // converter and aggregate data, get stream of YTs
    val dtvs = records.map(dtvFromRecord).rethrow // rethrow to get rid of either, which may result in an error
    // write data to output file
    val renderer = CSVRenderer[IO]() // renderer with default configuration and IO effect
    val outFile = SampleTH.getTempFile
    val output = dtvs
      .map(dtv => Record.from(dtv))
      .through(renderer.render)
      .through(Writer[IO]().write(outFile.toPath))
      .handleErrorWith(ex => errorHandler(ex, outFile))
    // assert result and remove temp file - deleteOnExit is unreliable (doesn't work from sbt)
    val checkAndClean = Stream.eval_(IO {
      assert(outFile.length > 16000)
      outFile.delete()
    })
    // run
    output.append(checkAndClean).compile.drain.unsafeRunSync()
  }

  test("spata allows data conversion to another file using for comprehension") {
    val outFile = SampleTH.getTempFile
    val outcome = for {
      // get stream of CSV records while ensuring source cleanup
      source <- Stream.bracket(IO { SampleTH.sourceFromResource(SampleTH.dataFile) })(source => IO { source.close() })
      destination <- Stream.bracket(IO { new FileOutputStream(outFile) })(destination => IO { destination.close() })
      out <- Reader[IO]()
        .read(source)
        .through(CSVParser[IO]().parse) // parser with default configuration and IO effect
        .filter(record => !record("max_temp").contains("NaN") && !record("min_temp").contains("NaN"))
        .through(CSVRenderer[IO]().render) // convert back to string representation
        .through(Writer[IO]().write(destination)) // write data to output file
        .handleErrorWith(ex => errorHandler(ex, outFile))
    } yield out
    // assert result and remove temp file - deleteOnExit is unreliable (doesn't work from sbt)
    val checkAndClean = Stream.eval_(IO {
      assert(outFile.length > 16000)
      outFile.delete()
    })
    // run
    outcome.append(checkAndClean).compile.drain.unsafeRunSync()
  }

  private def errorHandler(ex: Throwable, outFile: File) = Stream.eval(
    IO(println(s"Error while converting data from ${SampleTH.dataFile} to ${outFile.getPath}: ${ex.getMessage}"))
  )
}
