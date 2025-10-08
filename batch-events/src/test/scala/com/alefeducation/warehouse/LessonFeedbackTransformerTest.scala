package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LessonFeedbackTransformerTest extends AnyFunSuite with Matchers {

  val transformer = LessonFeedbackTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")
    val expectedQuery =
      s"""
         |WITH
         |fact_learning_experience AS ( select fle_dw_id, fle_ls_id from
         |    (SELECT
         |        t2.fle_dw_id,
         |        t2.fle_ls_id,
         |        t2.fle_created_time,
         |        ROW_NUMBER() OVER (PARTITION BY t2.fle_ls_id ORDER BY t2.fle_dw_id) AS rnk
         |    FROM alefdw.fact_learning_experience t2 where t2.fle_date_dw_id >= TO_CHAR(CURRENT_DATE - INTERVAL '7 DAY', 'YYYYMMDD') AND t2.fle_exp_ls_flag = false
         |    ) exp where exp.rnk = 1
         |),
         |dlo AS (select lo_id, lo_dw_id from
         |    (select
         |          lo_id,
         |          lo_dw_id,
         |          row_number() over (partition by lo_id order by lo_created_time desc) as rank
         |         from alefdw.dim_learning_objective
         |     ) t where t.rank = 1
         |)
         |SELECT
         |   staging.lesson_feedback_staging_id
         |FROM alefdw_stage.staging_lesson_feedback staging
         |   LEFT JOIN fact_learning_experience ON fact_learning_experience.fle_ls_id = staging.fle_ls_uuid
         |   JOIN alefdw.dim_tenant ON dim_tenant.tenant_id = staging.tenant_uuid
         |   JOIN alefdw.dim_school ON dim_school.school_id = staging.school_uuid
         |   LEFT JOIN alefdw.dim_academic_year ON dim_academic_year.academic_year_id = staging.academic_year_uuid
         |                                        AND dim_academic_year.academic_year_status = 1
         |   JOIN alefdw.dim_grade ON dim_grade.grade_id = staging.grade_uuid
         |   LEFT JOIN alefdw.dim_section ON dim_section.section_id = staging.section_uuid
         |   LEFT JOIN alefdw.dim_subject ON dim_subject.subject_id = staging.subject_uuid
         |   JOIN alefdw_stage.rel_user student1 ON student1.user_id = staging.student_uuid
         |   JOIN dlo ON dlo.lo_id = staging.lo_uuid
         |   LEFT JOIN alefdw.dim_curriculum_grade ON dim_curriculum_grade.curr_grade_id = staging.curr_grade_uuid
         |   LEFT JOIN alefdw.dim_curriculum_subject ON dim_curriculum_subject.curr_subject_id = staging.curr_subject_uuid AND dim_curriculum_subject.curr_subject_status = 1
         |   LEFT JOIN alefdw_stage.rel_dw_id_mappings dim_class ON dim_class.id = staging.class_uuid
         |   LEFT JOIN alefdw.dim_term ON dim_term.term_id = staging.lesson_feedback_trimester_id
         |WHERE ((section_uuid IS NULL AND dim_section.section_dw_id IS NULL) OR (section_uuid IS NOT NULL AND dim_section.section_dw_id IS NOT NULL))
         |  AND ((subject_uuid IS NULL AND dim_subject.subject_dw_id IS NULL) OR (subject_uuid IS NOT NULL AND dim_subject.subject_dw_id IS NOT NULL))
         |  AND ((class_uuid IS NULL AND dim_class.dw_id IS NULL) OR (class_uuid IS NOT NULL AND dim_class.dw_id IS NOT NULL))
         |  AND ((curr_grade_uuid IS NULL AND curr_grade_dw_id IS NULL) OR (curr_grade_uuid IS NOT NULL AND curr_grade_dw_id IS NOT NULL))
         |  AND ((curr_subject_uuid IS NULL AND curr_subject_dw_id IS NULL) OR (curr_subject_uuid IS NOT NULL AND curr_subject_dw_id IS NOT NULL))
         |  AND ((lesson_feedback_trimester_id IS NULL AND term_dw_id IS NULL) OR (lesson_feedback_trimester_id IS NOT NULL AND term_dw_id IS NOT NULL))
         |  AND ((staging.academic_year_uuid IS NULL AND dim_academic_year.academic_year_dw_id IS NULL) OR (staging.academic_year_uuid IS NOT NULL AND dim_academic_year.academic_year_dw_id IS NOT NULL))
         |ORDER BY staging.lesson_feedback_staging_id
         |LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert from select query") {

    val query = transformer.getInsertFromSelectQuery("alefdw", List(1001, 1002, 1003))

    val expectedQuery =
      """INSERT INTO alefdw.fact_lesson_feedback (
        |	lesson_feedback_id,
        |	lesson_feedback_created_time,
        |	lesson_feedback_dw_created_time,
        |	lesson_feedback_date_dw_id,
        |	lesson_feedback_tenant_dw_id,
        |	lesson_feedback_school_dw_id,
        |	lesson_feedback_academic_year_dw_id,
        |	lesson_feedback_grade_dw_id,
        |	lesson_feedback_section_dw_id,
        |	lesson_feedback_subject_dw_id,
        |	lesson_feedback_student_dw_id,
        |	lesson_feedback_lo_dw_id,
        |	lesson_feedback_term_dw_id,
        |	lesson_feedback_curr_grade_dw_id,
        |	lesson_feedback_curr_subject_dw_id,
        |	lesson_feedback_fle_ls_dw_id,
        |	lesson_feedback_trimester_id,
        |	lesson_feedback_trimester_order,
        |	lesson_feedback_content_academic_year,
        |	lesson_feedback_rating,
        |	lesson_feedback_rating_text,
        |	lesson_feedback_has_comment,
        |	lesson_feedback_is_cancelled,
        |	lesson_feedback_instructional_plan_id,
        |	lesson_feedback_learning_path_id,
        |	lesson_feedback_class_dw_id,
        |	lesson_feedback_fle_ls_uuid,
        | lesson_feedback_teaching_period_id,
        |	lesson_feedback_teaching_period_title
        |)
        |WITH
        |fact_learning_experience AS ( select fle_dw_id, fle_ls_id from
        |    (SELECT
        |        t2.fle_dw_id,
        |        t2.fle_ls_id,
        |        t2.fle_created_time,
        |        ROW_NUMBER() OVER (PARTITION BY t2.fle_ls_id ORDER BY t2.fle_dw_id) AS rnk
        |    FROM alefdw.fact_learning_experience t2 where t2.fle_date_dw_id >= TO_CHAR(CURRENT_DATE - INTERVAL '7 DAY', 'YYYYMMDD') AND t2.fle_exp_ls_flag = false
        |    ) exp where exp.rnk = 1
        |),
        |dlo AS (select lo_id, lo_dw_id from
        |    (select
        |          lo_id,
        |          lo_dw_id,
        |          row_number() over (partition by lo_id order by lo_created_time desc) as rank
        |         from alefdw.dim_learning_objective
        |     ) t where t.rank = 1
        |)
        |SELECT lesson_feedback_id,
        |	lesson_feedback_created_time,
        |	lesson_feedback_dw_created_time,
        |	lesson_feedback_date_dw_id,
        |	dim_tenant.tenant_dw_id AS lesson_feedback_tenant_dw_id,
        |	dim_school.school_dw_id AS lesson_feedback_school_dw_id,
        |	dim_academic_year.academic_year_dw_id AS lesson_feedback_academic_year_dw_id,
        |	dim_grade.grade_dw_id AS lesson_feedback_grade_dw_id,
        |	dim_section.section_dw_id AS lesson_feedback_section_dw_id,
        |	dim_subject.subject_dw_id AS lesson_feedback_subject_dw_id,
        |	student1.user_dw_id AS lesson_feedback_student_dw_id,
        |	dlo.lo_dw_id AS lesson_feedback_lo_dw_id,
        |	dim_term.term_dw_id AS lesson_feedback_term_dw_id,
        |	dim_curriculum_grade.curr_grade_dw_id AS lesson_feedback_curr_grade_dw_id,
        |	dim_curriculum_subject.curr_subject_dw_id AS lesson_feedback_curr_subject_dw_id,
        |	fact_learning_experience.fle_dw_id AS lesson_feedback_fle_ls_dw_id,
        |	lesson_feedback_trimester_id,
        |	lesson_feedback_trimester_order,
        |	lesson_feedback_content_academic_year,
        |	lesson_feedback_rating,
        |	lesson_feedback_rating_text,
        |	lesson_feedback_has_comment,
        |	lesson_feedback_is_cancelled,
        |	lesson_feedback_instructional_plan_id,
        |	lesson_feedback_learning_path_id,
        |	dim_class.dw_id AS lesson_feedback_class_dw_id,
        |	fle_ls_uuid AS lesson_feedback_fle_ls_uuid,
        | lesson_feedback_teaching_period_id,
        |	lesson_feedback_teaching_period_title
        |FROM alefdw_stage.staging_lesson_feedback staging
        |   LEFT JOIN fact_learning_experience ON fact_learning_experience.fle_ls_id = staging.fle_ls_uuid
        |   JOIN alefdw.dim_tenant ON dim_tenant.tenant_id = staging.tenant_uuid
        |   JOIN alefdw.dim_school ON dim_school.school_id = staging.school_uuid
        |   LEFT JOIN alefdw.dim_academic_year ON dim_academic_year.academic_year_id = staging.academic_year_uuid
        |                                        AND dim_academic_year.academic_year_status = 1
        |   JOIN alefdw.dim_grade ON dim_grade.grade_id = staging.grade_uuid
        |   LEFT JOIN alefdw.dim_section ON dim_section.section_id = staging.section_uuid
        |   LEFT JOIN alefdw.dim_subject ON dim_subject.subject_id = staging.subject_uuid
        |   JOIN alefdw_stage.rel_user student1 ON student1.user_id = staging.student_uuid
        |   JOIN dlo ON dlo.lo_id = staging.lo_uuid
        |   LEFT JOIN alefdw.dim_curriculum_grade ON dim_curriculum_grade.curr_grade_id = staging.curr_grade_uuid
        |   LEFT JOIN alefdw.dim_curriculum_subject ON dim_curriculum_subject.curr_subject_id = staging.curr_subject_uuid AND dim_curriculum_subject.curr_subject_status = 1
        |   LEFT JOIN alefdw_stage.rel_dw_id_mappings dim_class ON dim_class.id = staging.class_uuid
        |   LEFT JOIN alefdw.dim_term ON dim_term.term_id = staging.lesson_feedback_trimester_id
        |WHERE ((section_uuid IS NULL AND dim_section.section_dw_id IS NULL) OR (section_uuid IS NOT NULL AND dim_section.section_dw_id IS NOT NULL))
        |  AND ((subject_uuid IS NULL AND dim_subject.subject_dw_id IS NULL) OR (subject_uuid IS NOT NULL AND dim_subject.subject_dw_id IS NOT NULL))
        |  AND ((class_uuid IS NULL AND dim_class.dw_id IS NULL) OR (class_uuid IS NOT NULL AND dim_class.dw_id IS NOT NULL))
        |  AND ((curr_grade_uuid IS NULL AND curr_grade_dw_id IS NULL) OR (curr_grade_uuid IS NOT NULL AND curr_grade_dw_id IS NOT NULL))
        |  AND ((curr_subject_uuid IS NULL AND curr_subject_dw_id IS NULL) OR (curr_subject_uuid IS NOT NULL AND curr_subject_dw_id IS NOT NULL))
        |  AND ((lesson_feedback_trimester_id IS NULL AND term_dw_id IS NULL) OR (lesson_feedback_trimester_id IS NOT NULL AND term_dw_id IS NOT NULL))
        |  AND ((staging.academic_year_uuid IS NULL AND dim_academic_year.academic_year_dw_id IS NULL) OR (staging.academic_year_uuid IS NOT NULL AND dim_academic_year.academic_year_dw_id IS NOT NULL))
        |  AND staging.lesson_feedback_staging_id IN (1001,1002,1003)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare select statement") {
    val cols = List(
      "lesson_feedback_id",
      "lesson_feedback_created_time",
      "lesson_feedback_dw_created_time",
      "lesson_feedback_date_dw_id",
      "lesson_feedback_tenant_dw_id",
      "lesson_feedback_school_dw_id",
      "lesson_feedback_academic_year_dw_id",
      "lesson_feedback_grade_dw_id",
      "lesson_feedback_section_dw_id",
      "lesson_feedback_subject_dw_id",
      "lesson_feedback_student_dw_id",
      "lesson_feedback_lo_dw_id",
      "lesson_feedback_term_dw_id",
      "lesson_feedback_curr_grade_dw_id",
      "lesson_feedback_curr_subject_dw_id",
      "lesson_feedback_fle_ls_dw_id",
      "lesson_feedback_trimester_id",
      "lesson_feedback_trimester_order",
      "lesson_feedback_content_academic_year",
      "lesson_feedback_rating",
      "lesson_feedback_rating_text",
      "lesson_feedback_has_comment",
      "lesson_feedback_is_cancelled",
      "lesson_feedback_instructional_plan_id",
      "lesson_feedback_learning_path_id",
      "lesson_feedback_teaching_period_id",
      "lesson_feedback_teaching_period_title",
      "lesson_feedback_class_dw_id",
      "lesson_feedback_fle_ls_uuid",
    )
    val expRes =
      s"""lesson_feedback_id,
        |\tlesson_feedback_created_time,
        |\tlesson_feedback_dw_created_time,
        |\tlesson_feedback_date_dw_id,
        |\tdim_tenant.tenant_dw_id AS lesson_feedback_tenant_dw_id,
        |\tdim_school.school_dw_id AS lesson_feedback_school_dw_id,
        |\tdim_academic_year.academic_year_dw_id AS lesson_feedback_academic_year_dw_id,
        |\tdim_grade.grade_dw_id AS lesson_feedback_grade_dw_id,
        |\tdim_section.section_dw_id AS lesson_feedback_section_dw_id,
        |\tdim_subject.subject_dw_id AS lesson_feedback_subject_dw_id,
        |\tstudent1.user_dw_id AS lesson_feedback_student_dw_id,
        |\tdlo.lo_dw_id AS lesson_feedback_lo_dw_id,
        |\tdim_term.term_dw_id AS lesson_feedback_term_dw_id,
        |\tdim_curriculum_grade.curr_grade_dw_id AS lesson_feedback_curr_grade_dw_id,
        |\tdim_curriculum_subject.curr_subject_dw_id AS lesson_feedback_curr_subject_dw_id,
        |\tfact_learning_experience.fle_dw_id AS lesson_feedback_fle_ls_dw_id,
        |\tlesson_feedback_trimester_id,
        |\tlesson_feedback_trimester_order,
        |\tlesson_feedback_content_academic_year,
        |\tlesson_feedback_rating,
        |\tlesson_feedback_rating_text,
        |\tlesson_feedback_has_comment,
        |\tlesson_feedback_is_cancelled,
        |\tlesson_feedback_instructional_plan_id,
        |\tlesson_feedback_learning_path_id,
        |\tlesson_feedback_teaching_period_id,
        |\tlesson_feedback_teaching_period_title,
        |\tdim_class.dw_id AS lesson_feedback_class_dw_id,
        |\tfle_ls_uuid AS lesson_feedback_fle_ls_uuid""".stripMargin

    val actual = transformer.makeColumnNames(cols)

    actual.trim should be(expRes)
  }
}
