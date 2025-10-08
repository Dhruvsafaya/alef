package com.alefeducation.bigdata.batch.delta

import com.alefeducation.util.Constants.{ActiveState, Detached}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DeltaIWHTests extends AnyFunSuite with Matchers {

  test("buildMatchColumns should return correct SQL when isActiveUntilVersion is true") {
    val result = DeltaIWH.buildMatchColumns("user", isActiveUntilVersion = true)

    val expected =
      s"""
         | delta.user_id = events.user_id
         | AND
         |delta.user_status = $ActiveState
         | AND
         | delta.user_created_time < events.user_created_time
      """.stripMargin.trim

    result.trim shouldBe expected
  }

  test("buildMatchColumns should return correct SQL when isActiveUntilVersion is false") {
    val result = DeltaIWH.buildMatchColumns("user", isActiveUntilVersion = false)

    val expected =
      s"""
         | delta.user_id = events.user_id
         | AND
         |delta.user_status != $Detached
         | AND
         | delta.user_created_time < events.user_created_time
      """.stripMargin.trim

    result.trim shouldBe expected
  }

  test("buildInsertConditions should return correct SQL") {
    val result = DeltaIWH.buildInsertConditions("user")

    val expected =
      s"""
         |delta.user_id = events.user_id
         | AND
         |delta.user_status = $ActiveState
         | AND
         |delta.user_created_time = events.user_created_time
      """.stripMargin.trim

    result.trim shouldBe expected
  }

  test("buildUpdateColumns should return correct map when isActiveUntilVersion is true") {
    val result = DeltaIWH.buildUpdateColumns("user", isActiveUntilVersion = true)

    val expected = Map(
      "user_status" -> s"$Detached",
      "user_active_until" -> "events.user_created_time"
    )

    result shouldBe expected
  }

  test("buildUpdateColumns should return correct map when isActiveUntilVersion is false") {
    val result = DeltaIWH.buildUpdateColumns("user", isActiveUntilVersion = false)

    val expected = Map(
      "user_status" -> s"$Detached",
      "user_updated_time" -> "events.user_created_time",
      "user_dw_updated_time" -> "current_timestamp"
    )

    result shouldBe expected
  }
}

