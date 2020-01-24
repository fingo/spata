package info.fingo

import scala.util.Try

package object spata {
  type Maybe[A] = Either[Throwable, A]
  def maybe[A](code: => A): Maybe[A] = Try(code).toEither
}
