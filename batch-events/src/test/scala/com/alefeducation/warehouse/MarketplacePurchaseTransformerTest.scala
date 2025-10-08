package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MarketplacePurchaseTransformerTest extends AnyFunSuite with Matchers {

  val transformer: MarketplacePurchaseTransformer.type = MarketplacePurchaseTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")

    val expectedQuery =
      s"""
         |SELECT fip_dw_id
         |FROM alefdw_stage.staging_item_purchase staging
         |  JOIN alefdw.dim_tenant t ON t.tenant_id = staging.fip_tenant_id
         |  JOIN alefdw.dim_school s ON s.school_id = staging.fip_school_id
         |  LEFT JOIN alefdw.dim_grade g ON g.grade_id = staging.fip_grade_id
         |  LEFT JOIN alefdw.dim_section sec ON sec.section_id = staging.fip_section_id
         |  JOIN alefdw.dim_academic_year ay ON ay.academic_year_id = staging.fip_academic_year_id
         |  JOIN alefdw_stage.rel_user u ON u.user_id = staging.fip_student_id
         |  LEFT JOIN alefdw.dim_avatar a ON a.avatar_id = staging.fip_item_id AND staging.fip_item_type = 'AVATAR'
         |  LEFT JOIN alefdw.dim_avatar_layer al ON al.avatar_layer_id = staging.fip_item_id AND staging.fip_item_type = 'AVATAR_LAYER'
         |  LEFT JOIN alefdw.dim_marketplace_config mc ON mc.id = staging.fip_item_id AND staging.fip_item_type = 'PLANT_A_TREE'
         | ORDER BY fip_dw_id LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO alefdw.fact_item_purchase (
        |    fip_dw_id,
        |    fip_created_time,
        |    fip_dw_created_time,
        |    fip_date_dw_id,
        |    fip_id,
        |    fip_item_id,
        |    fip_item_dw_id,
        |    fip_item_type,
        |    fip_item_title,
        |    fip_item_description,
        |    fip_transaction_id,
        |    fip_school_id,
        |    fip_school_dw_id,
        |    fip_grade_id,
        |    fip_grade_dw_id,
        |    fip_section_id,
        |    fip_section_dw_id,
        |    fip_academic_year_id,
        |    fip_academic_year_dw_id,
        |    fip_academic_year,
        |    fip_student_id,
        |    fip_student_dw_id,
        |    fip_tenant_id,
        |    fip_tenant_dw_id,
        |    fip_redeemed_stars
        |)
        |SELECT
        |    fip_dw_id,
        |    fip_created_time,
        |    fip_dw_created_time,
        |    fip_date_dw_id,
        |    fip_id,
        |    fip_item_id,
        |    CASE WHEN a.avatar_dw_id is not NULL
        |               THEN a.avatar_dw_id
        |         WHEN al.avatar_layer_dw_id is not NULL
        |               THEN al.avatar_layer_dw_id
        |               ELSE mc.dw_id END AS fip_item_dw_id,
        |    fip_item_type,
        |    fip_item_title,
        |    fip_item_description,
        |    fip_transaction_id,
        |    fip_school_id,
        |    s.school_dw_id AS fip_school_dw_id,
        |    fip_grade_id,
        |    g.grade_dw_id AS fip_grade_dw_id,
        |    fip_section_id,
        |    sec.section_dw_id AS fip_section_dw_id,
        |    fip_academic_year_id,
        |    ay.academic_year_dw_id AS fip_academic_year_dw_id,
        |    fip_academic_year,
        |    fip_student_id,
        |    u.user_dw_id AS fip_student_dw_id,
        |    fip_tenant_id,
        |    t.tenant_dw_id AS fip_tenant_dw_id,
        |    fip_redeemed_stars
        |FROM alefdw_stage.staging_item_purchase staging
        |JOIN alefdw.dim_tenant t
        |    ON t.tenant_id = staging.fip_tenant_id
        |JOIN alefdw.dim_school s
        |    ON s.school_id = staging.fip_school_id
        |LEFT JOIN alefdw.dim_grade g
        |    ON g.grade_id = staging.fip_grade_id
        |LEFT JOIN alefdw.dim_section sec
        |    ON sec.section_id = staging.fip_section_id
        |JOIN alefdw.dim_academic_year ay
        |    ON ay.academic_year_id = staging.fip_academic_year_id
        |JOIN alefdw_stage.rel_user u
        |    ON u.user_id = staging.fip_student_id
        |LEFT JOIN alefdw.dim_avatar a
        |    ON a.avatar_id = staging.fip_item_id AND staging.fip_item_type = 'AVATAR'
        |LEFT JOIN alefdw.dim_avatar_layer al
        |    ON al.avatar_layer_id = staging.fip_item_id AND staging.fip_item_type = 'AVATAR_LAYER'
        |LEFT JOIN alefdw.dim_marketplace_config mc
        |    ON mc.id = staging.fip_item_id AND staging.fip_item_type = 'PLANT_A_TREE'
        |WHERE staging.fip_dw_id IN (10,12,13)
        |
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
