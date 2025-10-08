package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object StudentV2XAPITransformer extends RedshiftTransformer {

  override def tableNotation: Map[String, String] = Map.empty

  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation: Map[String, String] = Map(
    "staging_student_activities" -> "fsta_staging_id"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val pkSelectQuery =
      s"""
         |select fsta_staging_id
         |  from ${connection.schema}_stage.staging_student_activities
         |    inner join ${connection.schema}.dim_tenant on dim_tenant.tenant_id = staging_student_activities.tenant_uuid
         |    inner join ${connection.schema}.dim_school on dim_school.school_id = staging_student_activities.school_uuid
         |    inner join ${connection.schema}.dim_grade on dim_grade.grade_id = staging_student_activities.grade_uuid
         |    inner join ${connection.schema}.dim_section on dim_section.section_id = staging_student_activities.section_uuid
         |    inner join ${connection.schema}.dim_subject on dim_subject.subject_id = staging_student_activities.subject_uuid
         |    inner join ${connection.schema}_stage.rel_user  student1 on student1.user_id = staging_student_activities.student_uuid
         |    left join ${connection.schema}.dim_academic_year on dim_academic_year.academic_year_id = staging_student_activities.academic_year_uuid
         |  WHERE
         |    (staging_student_activities.academic_year_uuid IS NULL AND dim_academic_year.academic_year_dw_id IS NULL)
         |    OR (staging_student_activities.academic_year_uuid IS NOT NULL AND dim_academic_year.academic_year_dw_id IS NOT NULL)
         |  order by fsta_staging_id limit $QUERY_LIMIT
       """.stripMargin

    val insertQuery =
      s"""
         |insert into ${connection.schema}.fact_student_activities (
         |  fsta_created_time,
         |	fsta_dw_created_time,
         |	fsta_actor_object_type,
         |	fsta_actor_account_homepage,
         |	fsta_verb_display,
         |	fsta_verb_id,
         |	fsta_object_id,
         |	fsta_object_type,
         |	fsta_object_definition_type,
         |	fsta_object_definition_name,
         |	fsta_from_time,
         |	fsta_to_time,
         |	fsta_outside_of_school,
         |	fsta_event_type,
         |	fsta_prev_event_type,
         |	fsta_next_event_type,
         |	fsta_date_dw_id,
         |	fsta_attempt,
         |	fsta_score_raw,
         |	fsta_score_scaled,
         |	fsta_score_max,
         |	fsta_score_min,
         |	fsta_lesson_position,
         |	fsta_exp_id,
         |	fsta_ls_id,
         |	fsta_tenant_dw_id,
         |	fsta_school_dw_id,
         |	fsta_grade_dw_id,
         |	fsta_section_dw_id,
         |	fsta_subject_dw_id,
         |	fsta_student_dw_id,
         |	fsta_academic_year_dw_id,
         |	fsta_timestamp_local,
         |	fsta_start_time,
         |	fsta_end_time,
         |	fsta_time_spent,
         |  fsta_is_completion_node,
         |  fsta_is_flexible_lesson,
         |  fsta_academic_calendar_id,
         |  fsta_teaching_period_id,
         |  fsta_teaching_period_title)
         |(select
         |  fsta_created_time,
         |	getdate(),
         |	fsta_actor_object_type,
         |	fsta_actor_account_homepage,
         |	fsta_verb_display,
         |	fsta_verb_id,
         |	fsta_object_id,
         |	fsta_object_type,
         |	fsta_object_definition_type,
         |	fsta_object_definition_name,
         |	fsta_from_time,
         |	fsta_to_time,
         |	fsta_outside_of_school,
         |	fsta_event_type,
         |	fsta_prev_event_type,
         |	fsta_next_event_type,
         |	fsta_date_dw_id,
         |	fsta_attempt,
         |	fsta_score_raw,
         |	fsta_score_scaled,
         |	fsta_score_max,
         |	fsta_score_min,
         |	fsta_lesson_position,
         |	fsta_exp_id,
         |	fsta_ls_id,
         |	dim_tenant.tenant_dw_id,
         |	dim_school.school_dw_id,
         |	dim_grade.grade_dw_id,
         |	dim_section.section_dw_id,
         |	dim_subject.subject_dw_id,
         |	student1.user_dw_id,
         |	dim_academic_year.academic_year_dw_id,
         |	fsta_timestamp_local,
         |	fsta_start_time,
         |	fsta_end_time,
         |	fsta_time_spent,
         |  fsta_is_completion_node,
         |  fsta_is_flexible_lesson,
         |  fsta_academic_calendar_id,
         |  fsta_teaching_period_id,
         |  fsta_teaching_period_title
         |from ${connection.schema}_stage.staging_student_activities
         |  inner join ${connection.schema}.dim_tenant on dim_tenant.tenant_id = staging_student_activities.tenant_uuid
         |  inner join ${connection.schema}.dim_school on dim_school.school_id = staging_student_activities.school_uuid
         |  inner join ${connection.schema}.dim_grade on dim_grade.grade_id = staging_student_activities.grade_uuid
         |  inner join ${connection.schema}.dim_section on dim_section.section_id = staging_student_activities.section_uuid
         |  inner join ${connection.schema}.dim_subject on dim_subject.subject_id = staging_student_activities.subject_uuid
         |  inner join ${connection.schema}_stage.rel_user  student1 on student1.user_id = staging_student_activities.student_uuid
         |  left join ${connection.schema}.dim_academic_year on dim_academic_year.academic_year_id = staging_student_activities.academic_year_uuid
         |  where
         |    (staging_student_activities.academic_year_uuid IS NOT NULL AND dim_academic_year.academic_year_dw_id IS NOT NULL)
         |    OR (staging_student_activities.academic_year_uuid IS NULL AND dim_academic_year.academic_year_dw_id IS NULL)
         |  order by fsta_staging_id limit $QUERY_LIMIT);
    """.stripMargin

    log.info(s"Prepare queries: $pkSelectQuery\n$insertQuery")

    List(
      QueryMeta(
        "staging_student_activities",
        pkSelectQuery,
        insertQuery
      )
    )
  }
}
