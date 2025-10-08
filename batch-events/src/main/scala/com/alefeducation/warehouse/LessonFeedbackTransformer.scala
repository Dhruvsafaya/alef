package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object LessonFeedbackTransformer extends CommonTransformer {

  val mainColumnNames: List[String] = List(
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
    "lesson_feedback_class_dw_id",
    "lesson_feedback_fle_ls_uuid",
    "lesson_feedback_teaching_period_id",
    "lesson_feedback_teaching_period_title"
  )

  override def getPkColumn(): String = "lesson_feedback_staging_id"

  override def getStagingTableName(): String = "staging_lesson_feedback"

  def cteForFirstRecord(schema: String) = s"""WITH
                            |fact_learning_experience AS ( select fle_dw_id, fle_ls_id from
                            |    (SELECT
                            |        t2.fle_dw_id,
                            |        t2.fle_ls_id,
                            |        t2.fle_created_time,
                            |        ROW_NUMBER() OVER (PARTITION BY t2.fle_ls_id ORDER BY t2.fle_dw_id) AS rnk
                            |    FROM $schema.fact_learning_experience t2 where t2.fle_date_dw_id >= TO_CHAR(CURRENT_DATE - INTERVAL '7 DAY', 'YYYYMMDD') AND t2.fle_exp_ls_flag = false
                            |    ) exp where exp.rnk = 1
                            |),
                            |dlo AS (select lo_id, lo_dw_id from
                            |    (select
                            |          lo_id,
                            |          lo_dw_id,
                            |          row_number() over (partition by lo_id order by lo_created_time desc) as rank
                            |         from $schema.dim_learning_objective
                            |     ) t where t.rank = 1
                            |)""".stripMargin

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idClause = ids.mkString(",")
    val insCols = mainColumnNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColumnNames(mainColumnNames)
    s"""
       |INSERT INTO $schema.fact_lesson_feedback ($insCols)
       |${cteForFirstRecord(schema)}
       |SELECT $selCols
       |${fromStatement(schema)}
       |  AND staging.$getPkColumn IN ($idClause)
       |""".stripMargin
  }

  override def getSelectQuery(schema: String): String =
    s"""
       |${cteForFirstRecord(schema)}
       |SELECT
       |   staging.$getPkColumn
       |${fromStatement(schema)}
       |ORDER BY staging.lesson_feedback_staging_id
       |LIMIT $QUERY_LIMIT
       |""".stripMargin


  private def fromStatement(schema: String): String = {
    s"""FROM ${schema}_stage.$getStagingTableName staging
      |   LEFT JOIN fact_learning_experience ON fact_learning_experience.fle_ls_id = staging.fle_ls_uuid
      |   JOIN $schema.dim_tenant ON dim_tenant.tenant_id = staging.tenant_uuid
      |   JOIN $schema.dim_school ON dim_school.school_id = staging.school_uuid
      |   LEFT JOIN $schema.dim_academic_year ON dim_academic_year.academic_year_id = staging.academic_year_uuid AND dim_academic_year.academic_year_status = 1
      |   JOIN $schema.dim_grade ON dim_grade.grade_id = staging.grade_uuid
      |   LEFT JOIN $schema.dim_section ON dim_section.section_id = staging.section_uuid
      |   LEFT JOIN $schema.dim_subject ON dim_subject.subject_id = staging.subject_uuid
      |   JOIN ${schema}_stage.rel_user student1 ON student1.user_id = staging.student_uuid
      |   JOIN dlo ON dlo.lo_id = staging.lo_uuid
      |   LEFT JOIN $schema.dim_curriculum_grade ON dim_curriculum_grade.curr_grade_id = staging.curr_grade_uuid
      |   LEFT JOIN $schema.dim_curriculum_subject ON dim_curriculum_subject.curr_subject_id = staging.curr_subject_uuid AND dim_curriculum_subject.curr_subject_status = 1
      |   LEFT JOIN ${schema}_stage.rel_dw_id_mappings dim_class ON dim_class.id = staging.class_uuid
      |   LEFT JOIN $schema.dim_term ON dim_term.term_id = staging.lesson_feedback_trimester_id
      |WHERE ((section_uuid IS NULL AND dim_section.section_dw_id IS NULL) OR (section_uuid IS NOT NULL AND dim_section.section_dw_id IS NOT NULL))
      |  AND ((subject_uuid IS NULL AND dim_subject.subject_dw_id IS NULL) OR (subject_uuid IS NOT NULL AND dim_subject.subject_dw_id IS NOT NULL))
      |  AND ((class_uuid IS NULL AND dim_class.dw_id IS NULL) OR (class_uuid IS NOT NULL AND dim_class.dw_id IS NOT NULL))
      |  AND ((curr_grade_uuid IS NULL AND curr_grade_dw_id IS NULL) OR (curr_grade_uuid IS NOT NULL AND curr_grade_dw_id IS NOT NULL))
      |  AND ((curr_subject_uuid IS NULL AND curr_subject_dw_id IS NULL) OR (curr_subject_uuid IS NOT NULL AND curr_subject_dw_id IS NOT NULL))
      |  AND ((lesson_feedback_trimester_id IS NULL AND term_dw_id IS NULL) OR (lesson_feedback_trimester_id IS NOT NULL AND term_dw_id IS NOT NULL))
      |  AND ((staging.academic_year_uuid IS NULL AND dim_academic_year.academic_year_dw_id IS NULL) OR (staging.academic_year_uuid IS NOT NULL AND dim_academic_year.academic_year_dw_id IS NOT NULL))
      |""".stripMargin
  }

  def makeColumnNames(cols: List[String]): String =
    cols
      .map {
        case "lesson_feedback_tenant_dw_id"        => s"\tdim_tenant.tenant_dw_id AS lesson_feedback_tenant_dw_id"
        case "lesson_feedback_school_dw_id"        => s"\tdim_school.school_dw_id AS lesson_feedback_school_dw_id"
        case "lesson_feedback_academic_year_dw_id" => s"\tdim_academic_year.academic_year_dw_id AS lesson_feedback_academic_year_dw_id"
        case "lesson_feedback_grade_dw_id"         => s"\tdim_grade.grade_dw_id AS lesson_feedback_grade_dw_id"
        case "lesson_feedback_section_dw_id"       => s"\tdim_section.section_dw_id AS lesson_feedback_section_dw_id"
        case "lesson_feedback_subject_dw_id"       => s"\tdim_subject.subject_dw_id AS lesson_feedback_subject_dw_id"
        case "lesson_feedback_student_dw_id"       => s"\tstudent1.user_dw_id AS lesson_feedback_student_dw_id"
        case "lesson_feedback_lo_dw_id"            => s"\tdlo.lo_dw_id AS lesson_feedback_lo_dw_id"
        case "lesson_feedback_term_dw_id"          => s"\tdim_term.term_dw_id AS lesson_feedback_term_dw_id"
        case "lesson_feedback_curr_grade_dw_id"    => s"\tdim_curriculum_grade.curr_grade_dw_id AS lesson_feedback_curr_grade_dw_id"
        case "lesson_feedback_curr_subject_dw_id"  => s"\tdim_curriculum_subject.curr_subject_dw_id AS lesson_feedback_curr_subject_dw_id"
        case "lesson_feedback_fle_ls_dw_id"        => s"\tfact_learning_experience.fle_dw_id AS lesson_feedback_fle_ls_dw_id"
        case "lesson_feedback_class_dw_id"         => s"\tdim_class.dw_id AS lesson_feedback_class_dw_id"
        case "lesson_feedback_fle_ls_uuid"         => s"\tfle_ls_uuid AS lesson_feedback_fle_ls_uuid"
        case col                                   => s"\t$col"
      }
      .mkString(",\n")

}
