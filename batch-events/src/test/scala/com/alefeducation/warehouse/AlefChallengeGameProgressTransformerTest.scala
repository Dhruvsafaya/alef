package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.alefeducation.warehouse.pathway.PathwayTargetProgressTransformer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class AlefChallengeGameProgressTransformerTest extends AnyFunSuite with Matchers {

  val transformer: AlefChallengeGameProgressTransformer.type = AlefChallengeGameProgressTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("testalefdw")

    val expectedQuery =
      s"""
         |SELECT fgc_dw_id
         |FROM testalefdw_stage.staging_challenge_game_progress staging
         |  JOIN testalefdw.dim_tenant t ON t.tenant_id = staging.fgc_tenant_id
         |  JOIN testalefdw_stage.rel_user st ON st.user_id = staging.fgc_student_id and st.user_type = 'STUDENT'
         |  LEFT JOIN testalefdw.dim_school s ON s.school_id = staging.fgc_school_id
         |  LEFT JOIN testalefdw.dim_academic_year ay ON ay.academic_year_id = staging.fgc_academic_year_id
         |ORDER BY fgc_dw_id
         |LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("testalefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO testalefdw.fact_challenge_game_progress (
        |fgc_dw_id,
        |    fgc_id,
        |    fgc_game_id,
        |    fgc_state,
        |    fgc_tenant_dw_id,
        |    fgc_tenant_id,
        |    fgc_student_dw_id,
        |    fgc_student_id,
        |    fgc_academic_year_dw_id,
        |    fgc_academic_year_id,
        |    fgc_academic_year_tag,
        |    fgc_school_dw_id,
        |    fgc_school_id,
        |    fgc_grade,
        |    fgc_organization,
        |    fgc_score,
        |    fgc_created_time,
        |    fgc_dw_created_time,
        |    fgc_date_dw_id
        |)
        |SELECT fgc_dw_id,
        |    fgc_id,
        |    fgc_game_id,
        |    fgc_state,
        |    t.tenant_dw_id AS fgc_tenant_dw_id,
        |    fgc_tenant_id,
        |    st.user_dw_id AS fgc_student_dw_id,
        |    fgc_student_id,
        |    ay.academic_year_dw_id AS fgc_academic_year_dw_id,
        |    fgc_academic_year_id,
        |    fgc_academic_year_tag,
        |    s.school_dw_id AS fgc_school_dw_id,
        |    fgc_school_id,
        |    fgc_grade,
        |    fgc_organization,
        |    fgc_score,
        |    fgc_created_time,
        |    fgc_dw_created_time,
        |    fgc_date_dw_id
        |FROM testalefdw_stage.staging_challenge_game_progress staging
        |  JOIN testalefdw.dim_tenant t ON t.tenant_id = staging.fgc_tenant_id
        |  JOIN testalefdw_stage.rel_user st ON st.user_id = staging.fgc_student_id and st.user_type = 'STUDENT'
        |  LEFT JOIN testalefdw.dim_school s ON s.school_id = staging.fgc_school_id
        |  LEFT JOIN testalefdw.dim_academic_year ay ON ay.academic_year_id = staging.fgc_academic_year_id
        |WHERE staging.fgc_dw_id IN (10,12,13)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
