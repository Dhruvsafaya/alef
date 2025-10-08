package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TeacherTaskCenterTransformerTest extends AnyFunSuite with Matchers {

  val transformer: TeacherTaskCenterTransformer.type = TeacherTaskCenterTransformer


  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")

    val expectedQuery =
      s"""
         |SELECT staging.dw_id
         |FROM alefdw_stage.staging_teacher_task_center staging
         |  JOIN alefdw.dim_tenant t ON t.tenant_id = staging.tenant_id
         |  JOIN alefdw.dim_school s ON s.school_id = staging.school_id
         |  JOIN alefdw_stage.rel_dw_id_mappings c ON c.id = staging.class_id and c.entity_type = 'class'
         |  JOIN alefdw_stage.rel_user u ON u.user_id = staging.teacher_id and u.user_type = 'TEACHER'
         | ORDER BY staging.dw_id LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO alefdw.fact_teacher_task_center (
        |   dw_id,
        |   created_time,
        |   dw_created_time,
        |   date_dw_id,
        |   event_type,
        |   _trace_id,
        |   event_id,
        |   task_id,
        |   task_type,
        |   school_id,
        |   school_dw_id,
        |   class_id,
        |   class_dw_id,
        |   teacher_id,
        |   teacher_dw_id,
        |   tenant_id,
        |   tenant_dw_id
        |)
        |SELECT
        |    staging.dw_id,
        |    staging.created_time,
        |    getdate(),
        |    staging.date_dw_id,
        |    staging.event_type,
        |    staging._trace_id,
        |    staging.event_id,
        |    staging.task_id,
        |    staging.task_type,
        |    staging.school_id,
        |    s.school_dw_id AS school_dw_id,
        |    staging.class_id,
        |    c.dw_id AS class_dw_id,
        |    staging.teacher_id,
        |    u.user_dw_id AS teacher_dw_id,
        |    staging.tenant_id,
        |    t.tenant_dw_id AS tenant_dw_id
        |FROM alefdw_stage.staging_teacher_task_center staging
        |  JOIN alefdw.dim_tenant t ON t.tenant_id = staging.tenant_id
        |  JOIN alefdw.dim_school s ON s.school_id = staging.school_id
        |  JOIN alefdw_stage.rel_dw_id_mappings c ON c.id = staging.class_id and c.entity_type = 'class'
        |  JOIN alefdw_stage.rel_user u ON u.user_id = staging.teacher_id and u.user_type = 'TEACHER'
        |WHERE staging.dw_id IN (10,12,13)
        |
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
