/*
 * Copyright 2020 FINGO sp. z o.o.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package info.fingo.spata.parser

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import info.fingo.spata.error.ParsingErrorCode.*
import FieldParser.FieldResult
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import Config.*
import RawFields.*
import FieldFailures.*
import RecordResults.*

class RecordParserTS extends AnyFunSuite with TableDrivenPropertyChecks:

  private val parser = new RecordParser[IO]()

  test("Record parser should correctly parse provided input"):
    forAll(regularCases): (_, input, output) =>
      val result = parse(input)
      assert(result == output)

  test("Record parser should correctly report malformed input"):
    forAll(failureCases): (_, input, output) =>
      val result = parse(input)
      assert(result == output)

  private def parse(input: List[FieldResult]) =
    val stream = Stream(input*).through(parser.toRecords)
    stream.compile.toList.unsafeRunSync()

  private lazy val regularCases = Table(
    ("testCase", "input", "output"),
    ("oneField", List(rfe("abc", 3)), List(rr("abc")(3))),
    ("twoFields", List(rf("abc", 3), rfe("xyz", 7)), List(rr("abc", "xyz")(7))),
    ("twoRecords", List(rfe("abc", 3), rfe("xyz", 3, 2)), List(rr("abc")(3), rr("xyz")(3, 2, 2))),
    ("twoAndOne", List(rf("abc", 3), rfe("def", 7), rfe("xyz", 3, 2)), List(rr("abc", "def")(7), rr("xyz")(3, 2, 2))),
    (
      "spacesIn",
      List(rf("abc ", 4), rfe(" def", 9), rfe("x yz", 4, 2)),
      List(rr("abc ", " def")(9), rr("x yz")(4, 2, 2))
    ),
    ("spacesOut", List(rf("abc", 4), rfe("def", 9), rfe("xyz", 4, 2)), List(rr("abc", "def")(9), rr("xyz")(4, 2, 2))),
    (
      "newLies",
      List(rf(s"abc$nl", 1, 2), rfe(s"${nl}def", 3, 3), rfe(s"x${nl}yz", 2, 5)),
      List(rr(s"abc$nl", s"${nl}def")(3, 3), rr(s"x${nl}yz")(2, 5, 2))
    ),
    ("noContent", List(rf("", 0), rfe("", 1), rfe("", 0, 2)), List(rr("", "")(1))),
    ("noRecordEnd", List(rf("abc", 4, 3)), List()),
    ("empty", List(), List())
  )

  private lazy val failureCases = Table(
    ("testCase", "input", "output"),
    ("errorOnly", List(ffcq(3)), List(rfl(UnclosedQuotation, 3))),
    ("fieldAndError", List(rf("abc", 3), ffeq(7)), List(rfl(UnescapedQuotation, 7, 1, 1, 2))),
    ("recordAndError", List(rfe("abc", 3), ffeq(3, 2)), List(rr("abc")(3), rfl(UnescapedQuotation, 3, 2, 2)))
  )
