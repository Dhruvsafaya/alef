package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MarketplaceTransactionTransformerTest extends AnyFunSuite with Matchers {

  val transformer: MarketplaceTransactionTransformer.type = MarketplaceTransactionTransformer


  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")

    val expectedQuery =
      s"""
         |SELECT fit_dw_id
         |FROM alefdw_stage.staging_item_transaction staging
         |  JOIN alefdw.dim_tenant t ON t.tenant_id = staging.fit_tenant_id
         |  JOIN alefdw.dim_school s ON s.school_id = staging.fit_school_id
         |  LEFT JOIN alefdw.dim_grade g ON g.grade_id = staging.fit_grade_id
         |  LEFT JOIN alefdw.dim_section sec ON sec.section_id = staging.fit_section_id
         |  JOIN alefdw.dim_academic_year ay ON ay.academic_year_id = staging.fit_academic_year_id
         |  JOIN alefdw_stage.rel_user u ON u.user_id = staging.fit_student_id
         |  LEFT JOIN alefdw.dim_avatar a ON a.avatar_id = staging.fit_item_id AND staging.fit_item_type = 'AVATAR'
         |  LEFT JOIN alefdw.dim_avatar_layer al ON al.avatar_layer_id = staging.fit_item_id AND staging.fit_item_type = 'AVATAR_LAYER'
         | ORDER BY fit_dw_id LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO alefdw.fact_item_transaction (
        |    fit_dw_id,
        |    fit_created_time,
        |    fit_dw_created_time,
        |    fit_date_dw_id,
        |    fit_id,
        |    fit_item_id,
        |    fit_item_dw_id,
        |    fit_item_type,
        |    fit_school_id,
        |    fit_school_dw_id,
        |    fit_grade_id,
        |    fit_grade_dw_id,
        |    fit_section_id,
        |    fit_section_dw_id,
        |    fit_academic_year_id,
        |    fit_academic_year_dw_id,
        |    fit_academic_year,
        |    fit_student_id,
        |    fit_student_dw_id,
        |    fit_tenant_id,
        |    fit_tenant_dw_id,
        |    fit_available_stars,
        |    fit_item_cost,
        |    fit_star_balance
        |)
        |SELECT
        |    fit_dw_id,
        |    fit_created_time,
        |    fit_dw_created_time,
        |    fit_date_dw_id,
        |    fit_id,
        |    fit_item_id,
        |    CASE WHEN a.avatar_dw_id is not NULL
        |              THEN a.avatar_dw_id
        |              ELSE al.avatar_layer_dw_id END AS fit_item_dw_id,
        |    fit_item_type,
        |    fit_school_id,
        |    s.school_dw_id AS fit_school_dw_id,
        |    fit_grade_id,
        |    g.grade_dw_id AS fit_grade_dw_id,
        |    fit_section_id,
        |    sec.section_dw_id AS fit_section_dw_id,
        |    fit_academic_year_id,
        |    ay.academic_year_dw_id AS fit_academic_year_dw_id,
        |    fit_academic_year,
        |    fit_student_id,
        |    u.user_dw_id AS fit_student_dw_id,
        |    fit_tenant_id,
        |    t.tenant_dw_id AS fit_tenant_dw_id,
        |    fit_available_stars,
        |    fit_item_cost,
        |    fit_star_balance
        |FROM alefdw_stage.staging_item_transaction staging
        |JOIN alefdw.dim_tenant t
        |    ON t.tenant_id = staging.fit_tenant_id
        |JOIN alefdw.dim_school s
        |    ON s.school_id = staging.fit_school_id
        |LEFT JOIN alefdw.dim_grade g
        |    ON g.grade_id = staging.fit_grade_id
        |LEFT JOIN alefdw.dim_section sec
        |    ON sec.section_id = staging.fit_section_id
        |JOIN alefdw.dim_academic_year ay
        |    ON ay.academic_year_id = staging.fit_academic_year_id
        |JOIN alefdw_stage.rel_user u
        |    ON u.user_id = staging.fit_student_id
        |LEFT JOIN alefdw.dim_avatar a
        |    ON a.avatar_id = staging.fit_item_id AND staging.fit_item_type = 'AVATAR'
        |LEFT JOIN alefdw.dim_avatar_layer al
        |    ON al.avatar_layer_id = staging.fit_item_id AND staging.fit_item_type = 'AVATAR_LAYER'
        |WHERE staging.fit_dw_id IN (10,12,13)
        |
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
