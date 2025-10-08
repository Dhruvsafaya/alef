package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExperienceSubmittedTransformerTest extends AnyFunSuite with Matchers {

  val transformer: ExperienceSubmittedTransformer.type = ExperienceSubmittedTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("testalefdw")

    val expectedQuery =
      s"""
         |WITH
         |fact_learning_experience AS ( select fle_dw_id, fle_ls_id, fle_exp_id from
         |    (SELECT
         |        t2.fle_dw_id,
         |        t2.fle_ls_id,
         |        t2.fle_exp_id,
         |        t2.fle_created_time,
         |    ROW_NUMBER()
         |        OVER
         |        (PARTITION BY t2.fle_ls_id, t2.fle_exp_id ORDER BY t2.fle_dw_id)
         |        AS rnk
         |    FROM testalefdw.fact_learning_experience t2
         |    where
         |      t2.fle_date_dw_id >= TO_CHAR(CURRENT_DATE - INTERVAL '7 DAY', 'YYYYMMDD')
         |      AND t2.fle_exp_ls_flag = true
         |    ) exp where exp.rnk = 1
         |)
         |SELECT fes_staging_id
         |FROM testalefdw_stage.staging_experience_submitted staging
         |  LEFT JOIN fact_learning_experience ON fact_learning_experience.fle_ls_id = staging.fes_ls_id and
         |		fact_learning_experience.fle_exp_id = staging.exp_uuid
         |  INNER JOIN testalefdw.dim_learning_objective on dim_learning_objective.lo_id = staging.lo_uuid
         |  INNER JOIN testalefdw_stage.rel_user  student on student.user_id = staging.student_uuid and student.user_type = 'STUDENT'
         |  LEFT JOIN testalefdw.dim_section on dim_section.section_id = staging.section_uuid
         |  INNER JOIN testalefdw_stage.rel_dw_id_mappings class on class.id = staging.class_uuid and class.entity_type = 'class'
         |  LEFT JOIN testalefdw.dim_subject on dim_subject.subject_id = staging.subject_uuid
         |  INNER JOIN testalefdw.dim_grade on dim_grade.grade_id = staging.grade_uuid
         |  INNER JOIN testalefdw.dim_tenant on dim_tenant.tenant_id = staging.tenant_uuid
         |  INNER JOIN testalefdw.dim_school on dim_school.school_id = staging.school_uuid
         |  LEFT JOIN testalefdw.dim_learning_path on dim_learning_path.learning_path_id =
         |					staging.lp_uuid||staging.school_uuid||nvl(staging.subject_uuid, staging.class_uuid)
         |					and dim_learning_path.learning_path_status = 1
         |  LEFT JOIN testalefdw.dim_academic_year on dim_academic_year.academic_year_id = staging.academic_year_uuid
         |          and dim_academic_year.academic_year_status = 1
         |WHERE
         |(staging.academic_year_uuid IS NOT NULL AND dim_academic_year.academic_year_dw_id IS NOT NULL) OR
         |(staging.academic_year_uuid IS NULL AND dim_academic_year.academic_year_dw_id IS NULL)
         | ORDER BY fes_staging_id LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("testalefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |
        |INSERT INTO testalefdw.fact_experience_submitted (
        |	fes_created_time,
        |	fes_dw_created_time,
        |	fes_date_dw_id,
        |	fes_id,
        |	fes_exp_dw_id,
        |	fes_ls_dw_id,
        |	fes_step_id,
        |	fes_lo_dw_id,
        |	fes_student_dw_id,
        |	fes_section_dw_id,
        |	fes_class_dw_id,
        |	fes_subject_dw_id,
        |	fes_grade_dw_id,
        |	fes_tenant_dw_id,
        |	fes_school_dw_id,
        |	fes_lp_dw_id,
        |	fes_instructional_plan_id,
        |	fes_content_package_id,
        |	fes_content_title,
        |	fes_content_type,
        |	fes_start_time,
        |	fes_lesson_type,
        |	fes_is_retry,
        |	fes_outside_of_school,
        |	fes_attempt,
        |	fes_academic_period_order,
        |	fes_academic_year_dw_id,
        |	fes_content_academic_year,
        |	fes_lesson_category,
        |	fes_suid,
        |	fes_abbreviation,
        |	fes_activity_type,
        |	fes_activity_component_type,
        |	fes_completion_node,
        |	fes_main_component,
        |	fes_exit_ticket,
        |	fes_ls_id,
        |	exp_uuid,
        |	fes_material_id,
        |	fes_material_type,
        |	fes_open_path_enabled
        |)
        |WITH
        |fact_learning_experience AS ( select fle_dw_id, fle_ls_id, fle_exp_id from
        |    (SELECT
        |        t2.fle_dw_id,
        |        t2.fle_ls_id,
        |        t2.fle_exp_id,
        |        t2.fle_created_time,
        |    ROW_NUMBER()
        |        OVER
        |        (PARTITION BY t2.fle_ls_id, t2.fle_exp_id ORDER BY t2.fle_dw_id)
        |        AS rnk
        |    FROM testalefdw.fact_learning_experience t2
        |    where
        |      t2.fle_date_dw_id >= TO_CHAR(CURRENT_DATE - INTERVAL '7 DAY', 'YYYYMMDD')
        |      AND t2.fle_exp_ls_flag = true
        |    ) exp where exp.rnk = 1
        |)
        |SELECT fes_created_time,
        |	fes_dw_created_time,
        |	fes_date_dw_id,
        |	fes_id,
        |	fact_learning_experience.fle_dw_id  AS fes_exp_dw_id,
        |	fact_learning_experience.fle_dw_id AS fes_ls_dw_id,
        |	fes_step_id,
        |	dim_learning_objective.lo_dw_id AS fes_lo_dw_id,
        |	student.user_dw_id AS fes_student_dw_id,
        |	dim_section.section_dw_id AS fes_section_dw_id,
        |	class.dw_id AS fes_class_dw_id,
        |	dim_subject.subject_dw_id AS fes_subject_dw_id,
        |	dim_grade.grade_dw_id AS fes_grade_dw_id,
        |	dim_tenant.tenant_dw_id AS fes_tenant_dw_id,
        |	dim_school.school_dw_id AS fes_school_dw_id,
        |	dim_learning_path.learning_path_dw_id AS fes_lp_dw_id,
        |	fes_instructional_plan_id,
        |	fes_content_package_id,
        |	fes_content_title,
        |	fes_content_type,
        |	fes_start_time,
        |	fes_lesson_type,
        |	fes_is_retry,
        |	fes_outside_of_school,
        |	fes_attempt,
        |	fes_academic_period_order,
        |	dim_academic_year.academic_year_dw_id AS fes_academic_year_dw_id,
        |	fes_content_academic_year,
        |	fes_lesson_category,
        |	fes_suid,
        |	fes_abbreviation,
        |	fes_activity_type,
        |	fes_activity_component_type,
        |	fes_completion_node,
        |	fes_main_component,
        |	fes_exit_ticket,
        |	fes_ls_id,
        |	exp_uuid,
        |	fes_material_id,
        |	fes_material_type,
        |	fes_open_path_enabled
        |FROM testalefdw_stage.staging_experience_submitted staging
        |  LEFT JOIN fact_learning_experience ON fact_learning_experience.fle_ls_id = staging.fes_ls_id and
        |		fact_learning_experience.fle_exp_id = staging.exp_uuid
        |  INNER JOIN testalefdw.dim_learning_objective on dim_learning_objective.lo_id = staging.lo_uuid
        |  INNER JOIN testalefdw_stage.rel_user  student on student.user_id = staging.student_uuid and student.user_type = 'STUDENT'
        |  LEFT JOIN testalefdw.dim_section on dim_section.section_id = staging.section_uuid
        |  INNER JOIN testalefdw_stage.rel_dw_id_mappings class on class.id = staging.class_uuid and class.entity_type = 'class'
        |  LEFT JOIN testalefdw.dim_subject on dim_subject.subject_id = staging.subject_uuid
        |  INNER JOIN testalefdw.dim_grade on dim_grade.grade_id = staging.grade_uuid
        |  INNER JOIN testalefdw.dim_tenant on dim_tenant.tenant_id = staging.tenant_uuid
        |  INNER JOIN testalefdw.dim_school on dim_school.school_id = staging.school_uuid
        |  LEFT JOIN testalefdw.dim_learning_path on dim_learning_path.learning_path_id =
        |					staging.lp_uuid||staging.school_uuid||nvl(staging.subject_uuid, staging.class_uuid)
        |					and dim_learning_path.learning_path_status = 1
        |  LEFT JOIN testalefdw.dim_academic_year on dim_academic_year.academic_year_id = staging.academic_year_uuid
        |          and dim_academic_year.academic_year_status = 1
        |WHERE
        |(staging.academic_year_uuid IS NOT NULL AND dim_academic_year.academic_year_dw_id IS NOT NULL) OR
        |(staging.academic_year_uuid IS NULL AND dim_academic_year.academic_year_dw_id IS NULL)
        |
        |AND staging.fes_staging_id IN (10,12,13)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
