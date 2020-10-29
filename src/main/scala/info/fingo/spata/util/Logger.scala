/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.util

import cats.effect.Sync
import fs2.Stream
import org.slf4j.{Logger => JLogger}

/** spata logging interface.
  * It defaults to no-op logger and may be turned on by creating implicit value with another implementation.
  *
  * @see [[SLF4JLogger]]
  *
  * @tparam F the effect suspending logging operations
  */
sealed trait Logger[F[_]] {
  private[spata] def error(message: => String): F[Unit]
  private[spata] def error(cause: Throwable)(message: => String): F[Unit]
  private[spata] def errorS(message: => String): Stream[F, Unit] = Stream.eval(error(message))
  private[spata] def errorS(cause: Throwable)(message: => String): Stream[F, Unit] = Stream.eval(error(cause)(message))
  private[spata] def info(message: => String): F[Unit]
  private[spata] def infoS(message: => String): Stream[F, Unit] = Stream.eval(info(message))
  private[spata] def debug(message: => String): F[Unit]
  private[spata] def debugS(message: => String): Stream[F, Unit] = Stream.eval(debug(message))
}

/** Logger delegating spata logging operations to provided SLF4J logger.
  *
  * @param logger the underlying SLF4J logger
  * @tparam F the effect suspending logging operations
  */
class SLF4JLogger[F[_]: Sync](logger: JLogger) extends Logger[F] {
  override private[spata] def error(message: => String): F[Unit] = Sync[F].delay(logger.error(message))
  override private[spata] def error(cause: Throwable)(message: => String): F[Unit] =
    Sync[F].delay(logger.error(message, cause))
  override private[spata] def info(message: => String): F[Unit] = Sync[F].delay(logger.info(message))
  override private[spata] def debug(message: => String): F[Unit] = Sync[F].delay(logger.debug(message))
}

private[spata] class NOPLogger[F[_]: Sync] extends Logger[F] {
  override def error(message: => String): F[Unit] = Sync[F].unit
  override def error(cause: Throwable)(message: => String): F[Unit] = Sync[F].unit
  override def info(message: => String): F[Unit] = Sync[F].unit
  override def debug(message: => String): F[Unit] = Sync[F].unit
}

private[spata] object Logger {
  def apply[F[_]](implicit logger: Logger[F]): Logger[F] = logger
  implicit def logger[F[_]: Sync]: Logger[F] = new NOPLogger[F]
}
