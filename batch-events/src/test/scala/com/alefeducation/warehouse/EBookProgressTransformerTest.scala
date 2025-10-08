package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EBookProgressTransformerTest extends AnyFunSuite with Matchers {

  val transformer: EBookProgressTransformer.type = EBookProgressTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("testalefdw")

    val expectedQuery =
      s"""
         |SELECT fep_dw_id
         |FROM testalefdw_stage.staging_ebook_progress staging
         |  JOIN testalefdw.dim_tenant t ON t.tenant_id = staging.fep_tenant_id
         |  JOIN testalefdw.dim_school s ON s.school_id = staging.fep_school_id
         |  JOIN testalefdw.dim_grade g ON g.grade_id = staging.fep_grade_id
         |  JOIN testalefdw_stage.rel_dw_id_mappings c ON c.id = staging.fep_class_id and c.entity_type = 'class'
         |  JOIN testalefdw_stage.rel_user u ON u.user_id = staging.fep_student_id and u.user_type = 'STUDENT'
         |  JOIN testalefdw.dim_learning_objective lo ON lo.lo_id = staging.fep_lo_id
         | ORDER BY fep_dw_id LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("testalefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO testalefdw.fact_ebook_progress (
        |     fep_dw_id,
        |     fep_id,
        |     fep_session_id,
        |     fep_exp_id,
        |     fep_tenant_id,
        |     fep_tenant_dw_id,
        |     fep_student_id,
        |     fep_student_dw_id,
        |     feb_ay_tag,
        |     fep_school_id,
        |     fep_school_dw_id,
        |     fep_grade_id,
        |     fep_grade_dw_id,
        |     fep_class_id,
        |     fep_class_dw_id,
        |     fep_lo_id,
        |     fep_lo_dw_id,
        |     fep_step_instance_step_id,
        |     fep_material_type,
        |     fep_title,
        |     fep_total_pages,
        |     fep_has_audio,
        |     fep_action,
        |     fep_is_last_page,
        |     fep_location,
        |     fep_state,
        |     fep_time_spent,
        |     fep_bookmark_location,
        |     fep_highlight_location,
        |     fep_created_time,
        |     fep_dw_created_time,
        |     fep_date_dw_id
        |)
        |SELECT
        |    fep_dw_id,
        |     fep_id,
        |     fep_session_id,
        |     fep_exp_id,
        |     fep_tenant_id,
        |     t.tenant_dw_id AS fep_tenant_dw_id,
        |     fep_student_id,
        |     u.user_dw_id AS fep_student_dw_id,
        |     feb_ay_tag,
        |     fep_school_id,
        |     s.school_dw_id AS fep_school_dw_id,
        |     fep_grade_id,
        |     g.grade_dw_id AS fep_grade_dw_id,
        |     fep_class_id,
        |     c.dw_id AS fep_class_dw_id,
        |     fep_lo_id,
        |     lo.lo_dw_id AS fep_lo_dw_id,
        |     fep_step_instance_step_id,
        |     fep_material_type,
        |     fep_title,
        |     fep_total_pages,
        |     fep_has_audio,
        |     fep_action,
        |     fep_is_last_page,
        |     fep_location,
        |     fep_state,
        |     fep_time_spent,
        |     fep_bookmark_location,
        |     fep_highlight_location,
        |     fep_created_time,
        |     fep_dw_created_time,
        |     fep_date_dw_id
        |FROM testalefdw_stage.staging_ebook_progress staging
        |JOIN testalefdw.dim_tenant t ON t.tenant_id = staging.fep_tenant_id
        |JOIN testalefdw.dim_school s ON s.school_id = staging.fep_school_id
        |JOIN testalefdw.dim_grade g ON g.grade_id = staging.fep_grade_id
        |JOIN testalefdw_stage.rel_dw_id_mappings c ON c.id = staging.fep_class_id and c.entity_type = 'class'
        |JOIN testalefdw_stage.rel_user u ON u.user_id = staging.fep_student_id and u.user_type = 'STUDENT'
        |JOIN testalefdw.dim_learning_objective lo ON lo.lo_id = staging.fep_lo_id
        |WHERE staging.fep_dw_id IN (10,12,13)
        |
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
