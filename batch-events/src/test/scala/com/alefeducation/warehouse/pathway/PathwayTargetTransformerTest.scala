package com.alefeducation.warehouse.pathway

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class PathwayTargetTransformerTest extends AnyFunSuite with Matchers {

  val transformer: PathwayTargetTransformer.type = PathwayTargetTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("testalefdw")

    val expectedQuery =
      s"""
         |SELECT pt_dw_id
         |FROM testalefdw_stage.rel_pathway_target rel
         |  JOIN testalefdw.dim_tenant t ON t.tenant_id = rel.pt_tenant_id
         |  JOIN testalefdw.dim_school s ON s.school_id = rel.pt_school_id
         |  JOIN testalefdw.dim_grade g ON g.grade_id = rel.pt_grade_id
         |  JOIN testalefdw_stage.rel_dw_id_mappings c ON c.id = rel.pt_class_id and c.entity_type = 'class'
         |  JOIN testalefdw_stage.rel_dw_id_mappings p ON p.id = rel.pt_pathway_id and p.entity_type = 'course'
         |  JOIN testalefdw_stage.rel_dw_id_mappings pt ON pt.id = rel.pt_target_id and pt.entity_type = 'pathway_target'
         |  JOIN testalefdw_stage.rel_user u ON u.user_id = rel.pt_teacher_id and u.user_type = 'TEACHER'
         |ORDER BY pt_dw_id LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("testalefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO testalefdw.dim_pathway_target (
        |	pt_dw_id,
        |	pt_id,
        |	pt_target_id,
        | pt_target_dw_id,
        | pt_status,
        | pt_active_until,
        |	pt_target_state,
        |	pt_teacher_id,
        |	pt_teacher_dw_id,
        |	pt_pathway_id,
        |	pt_pathway_dw_id,
        |	pt_class_id,
        |	pt_class_dw_id,
        |	pt_school_id,
        |	pt_school_dw_id,
        |	pt_tenant_id,
        |	pt_tenant_dw_id,
        |	pt_grade_id,
        |	pt_grade_dw_id,
        |	pt_start_date,
        |	pt_end_date,
        |	pt_created_time,
        |	pt_dw_created_time
        |)
        |SELECT
        |	pt_dw_id,
        |	pt_id,
        |	pt_target_id,
        |	pt.dw_id AS pt_target_dw_id,
        | pt_status,
        | pt_active_until,
        |	pt_target_state,
        |	pt_teacher_id,
        |	u.user_dw_id AS pt_teacher_dw_id,
        |	pt_pathway_id,
        |	p.dw_id AS pt_pathway_dw_id,
        |	pt_class_id,
        |	c.dw_id AS pt_class_dw_id,
        |	pt_school_id,
        |	s.school_dw_id AS pt_school_dw_id,
        |	pt_tenant_id,
        |	t.tenant_dw_id AS pt_tenant_dw_id,
        |	pt_grade_id,
        |	g.grade_dw_id AS pt_grade_dw_id,
        |	pt_start_date,
        |	pt_end_date,
        |	pt_created_time,
        |	pt_dw_created_time
        |FROM testalefdw_stage.rel_pathway_target rel
        |  JOIN testalefdw.dim_tenant t ON t.tenant_id = rel.pt_tenant_id
        |  JOIN testalefdw.dim_school s ON s.school_id = rel.pt_school_id
        |  JOIN testalefdw.dim_grade g ON g.grade_id = rel.pt_grade_id
        |  JOIN testalefdw_stage.rel_dw_id_mappings c ON c.id = rel.pt_class_id and c.entity_type = 'class'
        |  JOIN testalefdw_stage.rel_dw_id_mappings p ON p.id = rel.pt_pathway_id and p.entity_type = 'course'
        |  JOIN testalefdw_stage.rel_dw_id_mappings pt ON pt.id = rel.pt_target_id and pt.entity_type = 'pathway_target'
        |  JOIN testalefdw_stage.rel_user u ON u.user_id = rel.pt_teacher_id and u.user_type = 'TEACHER'
        |WHERE rel.pt_dw_id IN (10,12,13)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
