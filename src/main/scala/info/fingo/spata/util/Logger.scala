/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.util

import cats.effect.Sync
import fs2.Stream
import org.slf4j.Logger as JLogger
import org.slf4j.helpers.NOPLogger

/** Logger delegating spata logging operations to provided SLF4J logger.
  * It defaults to no-op logger and may be activated by creating a given instance with regular SLF4J logger.
  *
  * @param logger the underlying SLF4J logger
  * @tparam F the effect suspending logging operations
  */
final class Logger[F[_]: Sync](logger: JLogger):

  /* Suspend basic logger methods in Sync */
  private[spata] def error(message: => String): F[Unit] = Sync[F].delay(logger.error(message))
  private[spata] def info(message: => String): F[Unit] = Sync[F].delay(logger.info(message))
  private[spata] def debug(message: => String): F[Unit] = Sync[F].delay(logger.debug(message))

  /* Helper methods to easily integrate logging in FS2 stream operations */
  private[spata] def errorS(message: => String): Stream[F, Unit] = Stream.eval(error(message))
  private[spata] def infoS(message: => String): Stream[F, Unit] = Stream.eval(info(message))
  private[spata] def debugS(message: => String): Stream[F, Unit] = Stream.eval(debug(message))

  /* Check if debug level is active */
  private[spata] def isDebug: Boolean = logger.isDebugEnabled

/* Logger helper object providing default implementation and access to actual logger */
private[spata] object Logger:

  /* Provides access to active logger */
  def apply[F[_]](using logger: Logger[F]): Logger[F] = logger

  /* Gets default (no-op) logger instance */
  given logger[F[_]: Sync]: Logger[F] = new Logger[F](NOPLogger.NOP_LOGGER)
