package com.alefeducation.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class OffsetTest extends AnyFunSuite {
  test("should return false when offset start and end has not changed") {
    val oldOffset = new Offset(0, 1)
    val newOffset = new Offset(0, 1)
    newOffset.hasMoved(oldOffset) shouldBe false
  }

  test("should return false if when new offset is starting from old") {
    val oldOffset = new Offset(0, 1)
    val newOffset = new Offset(1, 5)
    newOffset.hasMoved(oldOffset) shouldBe false
  }

  test("should return true when new offset has only one partition") {
    val oldOffset = new Offset(0, 0)
    val newOffset = new Offset(1, 1)
    newOffset.hasMoved(oldOffset) shouldBe true
  }

  test("should return true when has moved exclusively") {
    val oldOffset = new Offset(0, 1)
    val newOffset = new Offset(2, 3)
    newOffset.hasMoved(oldOffset) shouldBe true
  }
}
