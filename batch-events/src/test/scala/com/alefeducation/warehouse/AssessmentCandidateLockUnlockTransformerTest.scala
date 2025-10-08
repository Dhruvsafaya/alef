package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AssessmentCandidateLockUnlockTransformerTest extends AnyFunSuite with Matchers {

  val transformer: AssessmentCandidateLockUnlockTransformer.type = AssessmentCandidateLockUnlockTransformer


  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")

    val expectedQuery =
      s"""
         |SELECT staging.dw_id
         |FROM alefdw_stage.staging_assessment_lock_action staging
         |  JOIN alefdw.dim_tenant t ON t.tenant_id = staging.tenant_id
         |  JOIN alefdw.dim_school s ON s.school_id = staging.school_id
         |  JOIN alefdw_stage.rel_dw_id_mappings c ON c.id = staging.class_id
         |  JOIN alefdw_stage.rel_user st ON st.user_id = staging.candidate_id
         |  JOIN alefdw_stage.rel_user tc ON tc.user_id = staging.teacher_id
         |  JOIN alefdw.dim_testpart tp ON tp.id = staging.test_level_id and tp.version = staging.test_level_version
         | ORDER BY staging.dw_id LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO alefdw.fact_assessment_lock_action (
        |	dw_id,
        |	event_type,
        |	_trace_id,
        |	created_time,
        |	dw_created_time,
        | date_dw_id,
        |	id,
        |	tenant_id,
        |	tenant_dw_id,
        |	academic_year_tag,
        |	attempt,
        |	teacher_id,
        |	teacher_dw_id,
        |	candidate_id,
        |	candidate_dw_id,
        |	school_id,
        |	school_dw_id,
        |	class_id,
        |	class_dw_id,
        |	test_part_session_id,
        |	test_level_name,
        |	test_level_dw_id,
        |	test_level_id,
        |	test_level_version,
        |	skill,
        |	subject
        |)
        |SELECT 	staging.dw_id,
        |	staging.event_type,
        |	staging._trace_id,
        |	staging.created_time,
        |	staging.dw_created_time,
        |	staging.date_dw_id,
        |	staging.id,
        |	staging.tenant_id,
        |	t.tenant_dw_id AS tenant_dw_id,
        |	staging.academic_year_tag,
        |	staging.attempt,
        |	staging.teacher_id,
        |	tc.user_dw_id AS teacher_dw_id,
        |	staging.candidate_id,
        |	st.user_dw_id AS candidate_dw_id,
        |	staging.school_id,
        |	s.school_dw_id AS school_dw_id,
        |	staging.class_id,
        |	c.dw_id AS class_dw_id,
        |	staging.test_part_session_id,
        |	staging.test_level_name,
        |	tp.dw_id AS test_level_dw_id,
        |	staging.test_level_id,
        |	staging.test_level_version,
        |	staging.skill,
        |	staging.subject
        |
        |FROM alefdw_stage.staging_assessment_lock_action staging
        |  JOIN alefdw.dim_tenant t ON t.tenant_id = staging.tenant_id
        |  JOIN alefdw.dim_school s ON s.school_id = staging.school_id
        |  JOIN alefdw_stage.rel_dw_id_mappings c ON c.id = staging.class_id
        |  JOIN alefdw_stage.rel_user st ON st.user_id = staging.candidate_id
        |  JOIN alefdw_stage.rel_user tc ON tc.user_id = staging.teacher_id
        |  JOIN alefdw.dim_testpart tp ON tp.id = staging.test_level_id and tp.version = staging.test_level_version
        |
        |WHERE staging.dw_id IN (10,12,13)
        |
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
