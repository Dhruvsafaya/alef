package com.alefeducation.warehouse.course

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class CoreActivityAssignTransformerTest extends AnyFunSuite with Matchers {

  val transformer: CoreActivityAssignTransformer.type = CoreActivityAssignTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("testalefdw")

    val expectedQuery =
      s"""
         |SELECT cta_dw_id
         |FROM testalefdw_stage.rel_core_activity_assign rel
         |  JOIN testalefdw.dim_tenant t ON t.tenant_id = rel.cta_tenant_id
         |  JOIN testalefdw.dim_learning_objective lo ON lo.lo_id = rel.cta_activity_id
         |  JOIN testalefdw_stage.rel_dw_id_mappings c ON c.id = rel.cta_class_id and c.entity_type = 'class'
         |  JOIN testalefdw_stage.rel_dw_id_mappings cr ON cr.id = rel.cta_course_id and cr.entity_type = 'course'
         |  JOIN testalefdw_stage.rel_user u ON u.user_id = rel.cta_student_id and u.user_type = 'STUDENT'
         |  JOIN testalefdw_stage.rel_user teacher ON teacher.user_id = rel.cta_teacher_id and teacher.user_type = 'TEACHER'
         |ORDER BY cta_dw_id LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("testalefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO testalefdw.dim_core_activity_assign (
        |	cta_dw_id,
        |	cta_created_time,
        |	cta_dw_created_time,
        |	cta_active_until,
        |	cta_status,
        |	cta_event_type,
        |	cta_id,
        |	cta_action_time,
        |	cta_ay_tag,
        |	cta_tenant_dw_id,
        |	cta_tenant_id,
        |	cta_student_dw_id,
        |	cta_student_id,
        |	cta_course_dw_id,
        |	cta_course_id,
        |	cta_class_dw_id,
        |	cta_class_id,
        |	cta_teacher_dw_id,
        |	cta_teacher_id,
        |	cta_activity_dw_id,
        |	cta_activity_id,
        |	cta_activity_type,
        |	cta_progress_status,
        |	cta_start_date,
        |	cta_end_date
        |)
        |SELECT 	cta_dw_id,
        |	cta_created_time,
        |	cta_dw_created_time,
        |	cta_active_until,
        |	cta_status,
        |	cta_event_type,
        |	cta_id,
        |	cta_action_time,
        |	cta_ay_tag,
        |	t.tenant_dw_id AS cta_tenant_dw_id,
        |	cta_tenant_id,
        |	u.user_dw_id AS cta_student_dw_id,
        |	cta_student_id,
        |	cr.dw_id AS cta_course_dw_id,
        |	cta_course_id,
        |	c.dw_id AS cta_class_dw_id,
        |	cta_class_id,
        |	teacher.user_dw_id AS cta_teacher_dw_id,
        |	cta_teacher_id,
        |	lo.lo_dw_id AS cta_activity_dw_id,
        |	cta_activity_id,
        |	cta_activity_type,
        |	cta_progress_status,
        |	cta_start_date,
        |	cta_end_date
        |
        |FROM testalefdw_stage.rel_core_activity_assign rel
        |  JOIN testalefdw.dim_tenant t ON t.tenant_id = rel.cta_tenant_id
        |  JOIN testalefdw.dim_learning_objective lo ON lo.lo_id = rel.cta_activity_id
        |  JOIN testalefdw_stage.rel_dw_id_mappings c ON c.id = rel.cta_class_id and c.entity_type = 'class'
        |  JOIN testalefdw_stage.rel_dw_id_mappings cr ON cr.id = rel.cta_course_id and cr.entity_type = 'course'
        |  JOIN testalefdw_stage.rel_user u ON u.user_id = rel.cta_student_id and u.user_type = 'STUDENT'
        |  JOIN testalefdw_stage.rel_user teacher ON teacher.user_id = rel.cta_teacher_id and teacher.user_type = 'TEACHER'
        |
        |WHERE rel.cta_dw_id IN (10,12,13)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
