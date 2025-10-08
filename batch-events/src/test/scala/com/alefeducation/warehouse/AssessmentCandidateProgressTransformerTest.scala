package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AssessmentCandidateProgressTransformerTest extends AnyFunSuite with Matchers {

  val transformer: AssessmentCandidateProgressTransformer.type = AssessmentCandidateProgressTransformer


  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")

    val expectedQuery =
      s"""
         |SELECT 	staging.dw_id
         |FROM alefdw_stage.staging_candidate_assessment_progress staging
         |  JOIN alefdw.dim_tenant t ON t.tenant_id = staging.tenant_id
         |  JOIN alefdw.dim_school s ON s.school_id = staging.school_id
         |  JOIN alefdw.dim_grade g ON g.grade_id = staging.grade_id
         |  JOIN alefdw_stage.rel_dw_id_mappings ids ON ids.id = staging.class_id
         |  JOIN alefdw_stage.rel_user u ON u.user_id = staging.candidate_id
         |  JOIN alefdw.dim_testpart tp ON tp.id = staging.test_level_id and tp.version = staging.test_level_version
         |
         |ORDER BY staging.dw_id LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(10, 12, 13))
    val expectedQuery =
      """INSERT INTO alefdw.fact_candidate_assessment_progress (
        |	dw_id,
        |	_trace_id,
        |	event_type,
        |	created_time,
        |	dw_created_time,
        |	date_dw_id,
        |	assessment_id,
        |	academic_year_tag,
        |	tenant_id,
        |	tenant_dw_id,
        |	school_id,
        |	school_dw_id,
        |	grade_id,
        |	grade_dw_id,
        |	grade,
        |	class_id,
        |	class_dw_id,
        |	candidate_id,
        |	candidate_dw_id,
        |	test_level_session_id,
        |	test_level,
        |	test_id,
        |	test_version,
        |	test_level_id,
        |	test_level_version,
        |	test_level_version_dw_id,
        |	test_level_section_id,
        |	attempt_number,
        |	material_type,
        |	skill,
        |	subject,
        |	language,
        |	status,
        | report_id,
        |	total_timespent,
        |	final_score,
        |	final_grade,
        |	final_category,
        |	final_uncertainty,
        |	framework,
        |	time_to_return
        |)
        |SELECT 	staging.dw_id,
        |	staging._trace_id,
        |	staging.event_type,
        |	staging.created_time,
        |	staging.dw_created_time,
        |	staging.date_dw_id,
        |	staging.assessment_id,
        |	staging.academic_year_tag,
        |	staging.tenant_id,
        |	t.tenant_dw_id AS tenant_dw_id,
        |	staging.school_id,
        |	s.school_dw_id AS school_dw_id,
        |	staging.grade_id,
        |	g.grade_dw_id AS grade_dw_id,
        |	staging.grade,
        |	staging.class_id,
        |	ids.dw_id AS class_dw_id,
        |	staging.candidate_id,
        |	u.user_dw_id AS candidate_dw_id,
        |	staging.test_level_session_id,
        |	staging.test_level,
        |	staging.test_id,
        |	staging.test_version,
        |	staging.test_level_id,
        |	staging.test_level_version,
        |	tp.dw_id AS test_level_version_dw_id,
        |	staging.test_level_section_id,
        |	staging.attempt_number,
        |	staging.material_type,
        |	staging.skill,
        |	staging.subject,
        |	staging.language,
        |	staging.status,
        |	staging.report_id,
        |	staging.total_timespent,
        |	staging.final_score,
        |	staging.final_grade,
        |	staging.final_category,
        |	staging.final_uncertainty,
        |	staging.framework,
        |	staging.time_to_return
        |
        |FROM alefdw_stage.staging_candidate_assessment_progress staging
        |  JOIN alefdw.dim_tenant t ON t.tenant_id = staging.tenant_id
        |  JOIN alefdw.dim_school s ON s.school_id = staging.school_id
        |  JOIN alefdw.dim_grade g ON g.grade_id = staging.grade_id
        |  JOIN alefdw_stage.rel_dw_id_mappings ids ON ids.id = staging.class_id
        |  JOIN alefdw_stage.rel_user u ON u.user_id = staging.candidate_id
        |  JOIN alefdw.dim_testpart tp ON tp.id = staging.test_level_id and tp.version = staging.test_level_version
        |
        |WHERE staging.dw_id IN (10,12,13)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
