package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.{AnyFunSuite, AnyFunSuiteLike}
import org.scalatest.matchers.should.Matchers

class SkillGapTransformerTest extends AnyFunSuite with Matchers {
  val transformer: SkillGapTransformer.type = SkillGapTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")

    val expectedQuery =
      s"""
         |SELECT staging.dw_id FROM alefdw_stage.staging_skill_gap_tracker staging
         | JOIN alefdw.dim_tenant t ON t.tenant_id = staging.tenant_id
         |  JOIN alefdw.dim_school s ON s.school_id = staging.school_id
         |  JOIN alefdw_stage.rel_dw_id_mappings c ON c.id = staging.class_id AND c.entity_type='class'
         |  JOIN alefdw_stage.rel_dw_id_mappings p ON p.id = staging.pathway_id AND p.entity_type='course'
         |  JOIN alefdw_stage.rel_dw_id_mappings l ON l.id = staging.level_id AND l.entity_type='course_activity_container'
         |  JOIN alefdw_stage.rel_user st ON st.user_id = staging.student_id
         |  JOIN alefdw.dim_learning_objective dlo ON dlo.lo_id = staging.skill_id
         |  ORDER BY staging.dw_id LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO alefdw.fact_skill_gap_tracker (
        |dw_id,
        |event_type,
        |_trace_id,
        |tenant_id,
        |tenant_dw_id,
        |date_dw_id,
        |uuid,
        |created_time,
        |dw_created_time,
        |student_id,
        |student_dw_id,
        |class_id,
        |class_dw_id,
        |school_id,
        |school_dw_id,
        |pathway_id,
        |pathway_dw_id,
        |level_id,
        |level_dw_id,
        |academic_year_tag,
        |session_id,
        |ml_session_id,
        |level_proficiency_score,
        |level_proficiency_tier,
        |assessment_id,
        |session_attempt,
        |stars,
        |time_spent,
        |skill_proficiency_tier,
        |skill_proficiency_score,
        |skill_id,
        |skill_dw_id,
        |status )
        |SELECT staging.dw_id,
        |staging.event_type,
        |staging._trace_id,
        |staging.tenant_id,
        |t.tenant_dw_id AS tenant_dw_id,
        |staging.date_dw_id,
        |staging.uuid,
        |staging.created_time,
        |staging.dw_created_time,
        |staging.student_id,
        |st.user_dw_id AS student_dw_id,
        |staging.class_id,
        |c.dw_id AS class_dw_id,
        |staging.school_id,
        |s.school_dw_id AS school_dw_id,
        |staging.pathway_id,
        |p.dw_id AS pathway_dw_id,
        |staging.level_id,
        |l.dw_id AS level_dw_id,
        |staging.academic_year_tag,
        |staging.session_id,
        |staging.ml_session_id,
        |staging.level_proficiency_score,
        |staging.level_proficiency_tier,
        |staging.assessment_id,
        |staging.session_attempt,
        |staging.stars,
        |staging.time_spent,
        |staging.skill_proficiency_tier,
        |staging.skill_proficiency_score,
        |staging.skill_id,
        |dlo.lo_dw_id AS skill_dw_id,
        |staging.status
        |FROM alefdw_stage.staging_skill_gap_tracker staging
        |JOIN alefdw.dim_tenant t ON t.tenant_id = staging.tenant_id
        |JOIN alefdw.dim_school s ON s.school_id = staging.school_id
        |JOIN alefdw_stage.rel_dw_id_mappings c ON c.id = staging.class_id AND c.entity_type='class'
        |JOIN alefdw_stage.rel_dw_id_mappings p ON p.id = staging.pathway_id AND p.entity_type='course'
        |JOIN alefdw_stage.rel_dw_id_mappings l ON l.id = staging.level_id AND l.entity_type='course_activity_container'
        |JOIN alefdw_stage.rel_user st ON st.user_id = staging.student_id
        |JOIN alefdw.dim_learning_objective dlo ON dlo.lo_id = staging.skill_id
        |WHERE staging.dw_id IN (10,12,13)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

}
