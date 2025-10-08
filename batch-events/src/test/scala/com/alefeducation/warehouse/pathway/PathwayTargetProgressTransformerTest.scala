package com.alefeducation.warehouse.pathway

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class PathwayTargetProgressTransformerTest extends AnyFunSuite with Matchers {

  val transformer: PathwayTargetProgressTransformer.type = PathwayTargetProgressTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("testalefdw")

    val expectedQuery =
      s"""
         |SELECT fptp_dw_id
         |FROM testalefdw_stage.staging_pathway_target_progress staging
         |  JOIN testalefdw.dim_tenant t ON t.tenant_id = staging.fptp_tenant_id
         |  JOIN testalefdw.dim_school s ON s.school_id = staging.fptp_school_id
         |  JOIN testalefdw.dim_grade g ON g.grade_id = staging.fptp_grade_id
         |  JOIN testalefdw_stage.rel_dw_id_mappings c ON c.id = staging.fptp_class_id and c.entity_type = 'class'
         |  JOIN testalefdw_stage.rel_dw_id_mappings p ON p.id = staging.fptp_pathway_id and p.entity_type = 'course'
         |  JOIN testalefdw_stage.rel_dw_id_mappings target ON target.id = staging.fptp_target_id and target.entity_type = 'pathway_target'
         |  JOIN testalefdw_stage.rel_user te ON te.user_id = staging.fptp_teacher_id and te.user_type = 'TEACHER'
         |  JOIN testalefdw_stage.rel_user st ON st.user_id = staging.fptp_student_id and st.user_type = 'STUDENT'
         |ORDER BY fptp_dw_id
         |LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("testalefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO testalefdw.fact_pathway_target_progress (
        |	fptp_dw_id,
        |	fptp_id,
        |	fptp_student_target_id,
        |	fptp_target_id,
        |	fptp_target_dw_id,
        |	fptp_student_id,
        |	fptp_student_dw_id,
        |	fptp_tenant_id,
        |	fptp_tenant_dw_id,
        |	fptp_school_id,
        |	fptp_school_dw_id,
        |	fptp_grade_id,
        |	fptp_grade_dw_id,
        |	fptp_pathway_id,
        |	fptp_pathway_dw_id,
        |	fptp_teacher_id,
        |	fptp_teacher_dw_id,
        |	fptp_class_id,
        |	fptp_class_dw_id,
        |	fptp_target_state,
        |	fptp_recommended_target_level,
        |	fptp_finalized_target_level,
        |	fptp_levels_completed,
        |	fptp_created_time,
        |	fptp_dw_created_time,
        |	fptp_date_dw_id
        |)
        |SELECT 	fptp_dw_id,
        |	fptp_id,
        |	fptp_student_target_id,
        |	fptp_target_id,
        |	target.dw_id AS fptp_target_dw_id,
        |	fptp_student_id,
        |	st.user_dw_id AS fptp_student_dw_id,
        |	fptp_tenant_id,
        |	t.tenant_dw_id AS fptp_tenant_dw_id,
        |	fptp_school_id,
        |	s.school_dw_id AS fptp_school_dw_id,
        |	fptp_grade_id,
        |	g.grade_dw_id AS fptp_grade_dw_id,
        |	fptp_pathway_id,
        |	p.dw_id AS fptp_pathway_dw_id,
        |	fptp_teacher_id,
        |	te.user_dw_id AS fptp_teacher_dw_id,
        |	fptp_class_id,
        |	c.dw_id AS fptp_class_dw_id,
        |	fptp_target_state,
        |	fptp_recommended_target_level,
        |	fptp_finalized_target_level,
        |	fptp_levels_completed,
        |	fptp_created_time,
        |	fptp_dw_created_time,
        |	fptp_date_dw_id
        |FROM testalefdw_stage.staging_pathway_target_progress staging
        |  JOIN testalefdw.dim_tenant t ON t.tenant_id = staging.fptp_tenant_id
        |  JOIN testalefdw.dim_school s ON s.school_id = staging.fptp_school_id
        |  JOIN testalefdw.dim_grade g ON g.grade_id = staging.fptp_grade_id
        |  JOIN testalefdw_stage.rel_dw_id_mappings c ON c.id = staging.fptp_class_id and c.entity_type = 'class'
        |  JOIN testalefdw_stage.rel_dw_id_mappings p ON p.id = staging.fptp_pathway_id and p.entity_type = 'course'
        |  JOIN testalefdw_stage.rel_dw_id_mappings target ON target.id = staging.fptp_target_id and target.entity_type = 'pathway_target'
        |  JOIN testalefdw_stage.rel_user te ON te.user_id = staging.fptp_teacher_id and te.user_type = 'TEACHER'
        |  JOIN testalefdw_stage.rel_user st ON st.user_id = staging.fptp_student_id and st.user_type = 'STUDENT'
        |WHERE staging.fptp_dw_id IN (10,12,13)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
