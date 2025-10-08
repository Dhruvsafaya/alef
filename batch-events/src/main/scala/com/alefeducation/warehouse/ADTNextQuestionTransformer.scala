package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object ADTNextQuestionTransformer extends CommonRedshiftTransformer {

  override val pkCol: List[String] = List("fanq_dw_id")
  override val stagingTableName: String = "staging_adt_next_question"
  override val mainTableName: String = "fact_adt_next_question"

  override val mainColumnNames: List[String] = List(
    "fanq_created_time",
    "fanq_dw_created_time",
    "fanq_date_dw_id",
    "fanq_id",
    "fanq_fle_ls_dw_id",
    "fanq_student_dw_id",
    "fanq_question_pool_id",
    "fanq_tenant_dw_id",
    "fanq_response",
    "fanq_proficiency",
    "fanq_next_question_id",
    "fanq_time_spent",
    "fanq_current_question_id",
    "fanq_intest_progress",
    "fanq_status",
    "fanq_curriculum_subject_id",
    "fanq_curriculum_subject_name",
    "fanq_fle_ls_uuid",
    "fanq_language",
    "fanq_standard_error",
    "fanq_attempt",
    "fanq_grade",
    "fanq_grade_id",
    "fanq_grade_dw_id",
    "fanq_academic_year",
    "fanq_academic_year_id",
    "fanq_academic_year_dw_id",
    "fanq_academic_term",
    "fanq_class_subject_name",
  )

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
    |SELECT
    |fanq_created_time,
    |fanq_dw_created_time,
    |fanq_date_dw_id,
    |fanq_id,
    |fact_learning_experience.fle_dw_id,
    |student1.user_dw_id,
    |fanq_question_pool_id,
    |dim_tenant.tenant_dw_id,
    |fanq_response,
    |fanq_proficiency,
    |fanq_next_question_id,
    |fanq_time_spent,
    |fanq_current_question_id,
    |fanq_intest_progress,
    |fanq_status,
    |fanq_curriculum_subject_id,
    |fanq_curriculum_subject_name,
    |fle_ls_uuid,
    |fanq_language,
    |fanq_standard_error,
    |fanq_attempt,
    |fanq_grade,
    |fanq_grade_id,
    |dim_grade.grade_dw_id,
    |fanq_academic_year,
    |fanq_academic_year_id,
    |dim_academic_year.academic_year_dw_id,
    |fanq_academic_term,
    |fanq_class_subject_name
    |FROM ${connection.schema}_stage.staging_adt_next_question
    |LEFT JOIN ${connection.schema}.fact_learning_experience on fact_learning_experience.fle_ls_id = staging_adt_next_question.fle_ls_uuid and datediff(days, fact_learning_experience.fle_created_time, staging_adt_next_question.fanq_created_time) <= 175 and fact_learning_experience.fle_exp_ls_flag = false
    |INNER JOIN ${connection.schema}_stage.rel_user  student1 on student1.user_id = staging_adt_next_question.student_uuid
    |INNER JOIN ${connection.schema}.dim_tenant on dim_tenant.tenant_id = staging_adt_next_question.tenant_uuid
    |LEFT JOIN ${connection.schema}.dim_grade on dim_grade.grade_id = staging_adt_next_question.fanq_grade_id
    |LEFT JOIN ${connection.schema}.dim_academic_year on dim_academic_year.academic_year_id = staging_adt_next_question.fanq_academic_year_id
    |WHERE ((staging_adt_next_question.fanq_grade_id is null AND dim_grade.grade_id is null)
    |OR (staging_adt_next_question.fanq_grade_id is not null AND dim_grade.grade_id is not null))
    |AND ((staging_adt_next_question.fanq_academic_year_id is null AND dim_academic_year.academic_year_id is null)
    |OR (staging_adt_next_question.fanq_academic_year_id is not null AND dim_academic_year.academic_year_id is not null))
    |ORDER BY staging_adt_next_question.fanq_created_time
    |LIMIT $QUERY_LIMIT
    |""".stripMargin

  override def pkNotation: Map[String, String] = Map("staging_adt_next_question" -> "fanq_created_time")
}
