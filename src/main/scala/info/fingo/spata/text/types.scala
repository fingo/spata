/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.text

/** Convenience type. */
type ParseResult[A] = Either[ParseError, A]
