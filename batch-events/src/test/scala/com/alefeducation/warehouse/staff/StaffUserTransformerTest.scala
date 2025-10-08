package com.alefeducation.warehouse.staff

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class StaffUserTransformerTest extends AnyFunSuite with Matchers {

  val transformer: StaffUserTransformer.type = StaffUserTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")

    val expectedQuery =
      s"""
         |SELECT rel_staff_user_dw_id
         |FROM alefdw_stage.rel_staff_user staging
         | JOIN alefdw_stage.rel_user u ON u.user_id = staging.staff_user_id
         |ORDER BY rel_staff_user_dw_id
         |LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO alefdw.dim_staff_user
        |(
        |   rel_staff_user_dw_id,
        |   staff_user_created_time,
        |   staff_user_active_until,
        |   staff_user_dw_created_time,
        |   staff_user_status,
        |   staff_user_id,
        |   staff_user_dw_id,
        |   staff_user_onboarded,
        |   staff_user_expirable,
        |   staff_user_exclude_from_report,
        |   staff_user_avatar,
        |   staff_user_event_type,
        |   staff_user_enabled
        |) SELECT
        |   rel_staff_user_dw_id,
        |   staff_user_created_time,
        |   staff_user_active_until,
        |   staff_user_dw_created_time,
        |   staff_user_status,
        |   staff_user_id,
        |   u.user_dw_id AS staff_user_dw_id,
        |   staff_user_onboarded,
        |   staff_user_expirable,
        |   staff_user_exclude_from_report,
        |   staff_user_avatar,
        |   staff_user_event_type,
        |   staff_user_enabled
        |FROM alefdw_stage.rel_staff_user staging
        |   JOIN alefdw_stage.rel_user u ON u.user_id = staging.staff_user_id
        |WHERE staging.rel_staff_user_dw_id IN (10,12,13)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
