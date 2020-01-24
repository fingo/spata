package info.fingo

package object spata {
  type Maybe[A] = Either[Exception, A]

  def maybe[A](code: => A): Maybe[A] =
    try {
      Right(code)
    } catch {
      case ex: Exception => Left(ex)
    }
}
