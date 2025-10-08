package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StudentQueriesTransformerTest extends AnyFunSuite with Matchers {

  val transformer: StudentQueriesTransformer.type = StudentQueriesTransformer


  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")

    val expectedQuery =
      s"""
         |SELECT staging.dw_id
         |FROM alefdw_stage.staging_student_queries staging
         |  JOIN alefdw.dim_tenant t ON t.tenant_id = staging.tenant_id
         |  JOIN alefdw.dim_school s ON s.school_id = staging.school_id
         |  JOIN alefdw.dim_grade g ON g.grade_id = staging.grade_id
         |  JOIN alefdw_stage.rel_dw_id_mappings c ON c.id = staging.class_id and c.entity_type = 'class'
         |  JOIN alefdw.dim_section sc ON sc.section_id = staging.section_id
         |  LEFT JOIN alefdw_stage.rel_user u ON u.user_id = staging.teacher_id and u.user_type = 'TEACHER'
         |  JOIN alefdw.dim_academic_year ay ON ay.academic_year_id = staging.academic_year_id
         |  JOIN alefdw_stage.rel_user st ON st.user_id = staging.student_id and st.user_type = 'STUDENT'
         |  LEFT JOIN alefdw.dim_learning_objective lo ON lo.lo_id = staging.activity_id
         | ORDER BY staging.dw_id LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO alefdw.fact_student_queries (
        |  dw_id,
        |  created_time,
        |  dw_created_time,
        |  date_dw_id,
        |  _trace_id,
        |  event_type,
        |  message_id,
        |  query_id,
        |  tenant_id,
        |  tenant_dw_id,
        |  school_id,
        |  school_dw_id,
        |  grade_id,
        |  grade_dw_id,
        |  class_id,
        |  class_dw_id,
        |  section_id,
        |  section_dw_id,
        |  teacher_id,
        |  teacher_dw_id,
        |  academic_year_id,
        |  academic_year_dw_id,
        |  student_id,
        |  student_dw_id,
        |  activity_id,
        |  activity_dw_id,
        |  activity_type,
        |  can_student_reply,
        |  with_audio,
        |  with_text,
        |  material_type,
        |  gen_subject,
        |  lang_code,
        |  is_follow_up,
        |  has_screenshot
        |)
        |SELECT
        |  staging.dw_id,
        |  staging.created_time,
        |  getdate(),
        |  staging.date_dw_id,
        |  staging._trace_id,
        |  staging.event_type,
        |  staging.message_id,
        |  staging.query_id,
        |  staging.tenant_id,
        |  t.tenant_dw_id AS tenant_dw_id,
        |  staging.school_id,
        |  s.school_dw_id AS school_dw_id,
        |  staging.grade_id,
        |  g.grade_dw_id AS grade_dw_id,
        |  staging.class_id,
        |  c.dw_id AS class_dw_id,
        |  staging.section_id,
        |  sc.section_dw_id AS section_dw_id,
        |  staging.teacher_id,
        |  u.user_dw_id AS teacher_dw_id,
        |  staging.academic_year_id,
        |  ay.academic_year_dw_id AS academic_year_dw_id,
        |  staging.student_id,
        |  st.user_dw_id AS student_dw_id,
        |  staging.activity_id,
        |  lo.lo_dw_id AS activity_dw_id,
        |  staging.activity_type,
        |  staging.can_student_reply,
        |  staging.with_audio,
        |  staging.with_text,
        |  staging.material_type,
        |  staging.gen_subject,
        |  staging.lang_code,
        |  staging.is_follow_up,
        |  staging.has_screenshot
        |FROM alefdw_stage.staging_student_queries staging
        |  JOIN alefdw.dim_tenant t ON t.tenant_id = staging.tenant_id
        |  JOIN alefdw.dim_school s ON s.school_id = staging.school_id
        |  JOIN alefdw.dim_grade g ON g.grade_id = staging.grade_id
        |  JOIN alefdw_stage.rel_dw_id_mappings c ON c.id = staging.class_id and c.entity_type = 'class'
        |  JOIN alefdw.dim_section sc ON sc.section_id = staging.section_id
        |  LEFT JOIN alefdw_stage.rel_user u ON u.user_id = staging.teacher_id and u.user_type = 'TEACHER'
        |  JOIN alefdw.dim_academic_year ay ON ay.academic_year_id = staging.academic_year_id
        |  JOIN alefdw_stage.rel_user st ON st.user_id = staging.student_id and st.user_type = 'STUDENT'
        |  LEFT JOIN alefdw.dim_learning_objective lo ON lo.lo_id = staging.activity_id
        |WHERE staging.dw_id IN (10,12,13)
        |
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
