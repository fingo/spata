package info.fingo.spata.sample

import java.io.{Closeable, File, FileNotFoundException}

import scala.io.Source

/* Helper class for samples with shared functions */
object SampleTH {
  val dataFile = "mars-weather.csv"

  /* Source.fromResource throws NullPointerException on access, instead one of IOExceptions,
   * like fromFile or fromURL, so we converter it to be conform with typical usage scenarios.
   */
  def sourceFromResource(name: String): Source = {
    val source = Source.fromResource(name)
    try {
      source.hasNext
    } catch {
      case _: NullPointerException => throw new FileNotFoundException(s"Cannot find resource $name")
    }
    source
  }

  /* This is very simplistic approach - don't use it in production code.
   *  Look for better implementation of loan pattern or user ARM library
   */
  def withResource[A <: Closeable, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      resource.close()
    }

  /* Create temporary file with random name, removes it after closing. */
  def getTempFile: File = {
    val temp = File.createTempFile("spata_", ".csv")
    temp.deleteOnExit()
    temp
  }
}
