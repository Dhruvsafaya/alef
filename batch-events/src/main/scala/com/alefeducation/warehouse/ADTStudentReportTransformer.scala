package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object ADTStudentReportTransformer extends CommonRedshiftTransformer {
  override val pkCol: List[String] = List("fasr_dw_id")
  override val stagingTableName: String = "staging_adt_student_report"
  override val mainTableName: String = "fact_adt_student_report"

  override val mainColumnNames: List[String] = List(
    "fasr_created_time",
    "fasr_dw_created_time",
    "fasr_date_dw_id",
    "fasr_tenant_dw_id",
    "fasr_student_dw_id",
    "fasr_question_pool_id",
    "fasr_fle_ls_dw_id",
    "fasr_id",
    "fasr_final_score",
    "fasr_final_proficiency",
    "fasr_total_time_spent",
    "fasr_academic_year",
    "fasr_academic_term",
    "fasr_test_id",
    "fasr_curriculum_subject_id",
    "fasr_curriculum_subject_name",
    "fasr_status",
    "fasr_final_result",
    "fasr_final_uncertainty",
    "fasr_framework",
    "fasr_fle_ls_uuid",
    "fasr_final_standard_error",
    "fasr_language",
    "fasr_school_dw_id",
    "fasr_attempt",
    "fasr_final_grade",
    "fasr_forecast_score",
    "fasr_final_category",
    "fasr_grade",
    "fasr_grade_id",
    "fasr_grade_dw_id",
    "fasr_academic_year_id",
    "fasr_academic_year_dw_id",
    "fasr_secondary_result",
    "fasr_class_subject_name",
    "fasr_skill",
  )

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
       | select fasr_created_time,
       |	fasr_dw_created_time,
       |	fasr_date_dw_id,
       |	dim_tenant.tenant_dw_id,
       |	student1.user_dw_id,
       |	fasr_question_pool_id,
       |	fact_learning_experience.fle_dw_id,
       |	fasr_id,
       |	fasr_final_score,
       |	fasr_final_proficiency,
       |	fasr_total_time_spent,
       |	fasr_academic_year,
       |	fasr_academic_term,
       |	fasr_test_id,
       |	fasr_curriculum_subject_id,
       |	fasr_curriculum_subject_name,
       |	fasr_status,
       | 	fasr_final_result,
       |	fasr_final_uncertainty,
       |	fasr_framework,
       |	fasr_fle_ls_id,
       |	fasr_final_standard_error,
       |	fasr_language,
       |	dim_school.school_dw_id,
       |	fasr_attempt,
       |	fasr_final_grade,
       |  fasr_forecast_score,
       |  fasr_final_category,
       |  fasr_grade,
       |  fasr_grade_id,
       |  dim_grade.grade_dw_id,
       |  fasr_academic_year_id,
       |  dim_academic_year.academic_year_dw_id,
       |  fasr_secondary_result,
       |  fasr_class_subject_name,
       |  fasr_skill
       | from ${connection.schema}_stage.staging_adt_student_report
       | left join ${connection.schema}.fact_learning_experience on fact_learning_experience.fle_ls_id = staging_adt_student_report.fasr_fle_ls_id and datediff(days, fact_learning_experience.fle_created_time, staging_adt_student_report.fasr_created_time) <= 175 and fact_learning_experience.fle_exp_ls_flag = false
       | inner join ${connection.schema}.dim_tenant on dim_tenant.tenant_id = staging_adt_student_report.fasr_tenant_id
       | inner join ${connection.schema}_stage.rel_user  student1 on student1.user_id = staging_adt_student_report.fasr_student_id
       | inner join ${connection.schema}.dim_school on dim_school.school_id = staging_adt_student_report.fasr_school_id
       | left join ${connection.schema}.dim_grade on dim_grade.grade_id = staging_adt_student_report.fasr_grade_id
       | left join ${connection.schema}.dim_academic_year on dim_academic_year.academic_year_id = staging_adt_student_report.fasr_academic_year_id
       | order by fasr_staging_id
       | LIMIT $QUERY_LIMIT
       |""".stripMargin

  override def pkNotation: Map[String, String] = Map("staging_adt_student_report" -> "fasr_created_time")
}
