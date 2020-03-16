package info.fingo

import scala.util.Try

/** Spata primary package */
package object spata {

  /** Convenience type */
  type Maybe[A] = Either[Throwable, A]

  /** Convenience function to wrap code in [[scala.util.Try]] and convert to [[scala.util.Either]] */
  def maybe[A](code: => A): Maybe[A] = Try(code).toEither
}
