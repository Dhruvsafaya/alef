package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object TeacherV2XAPITransformer extends RedshiftTransformer {
  override def tableNotation: Map[String, String] = Map.empty

  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation: Map[String, String] = Map(
    "staging_teacher_activities" -> "fta_staging_id"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {

    val pkSelectQuery =
      s"""
         |select fta_staging_id
         |from ${connection.schema}_stage.staging_teacher_activities
         |  inner join ${connection.schema}.dim_tenant on dim_tenant.tenant_id = staging_teacher_activities.tenant_uuid
         |  inner join ${connection.schema}.dim_school on dim_school.school_id = staging_teacher_activities.school_uuid
         |  inner join ${connection.schema}.dim_grade on dim_grade.grade_id = staging_teacher_activities.grade_uuid
         |  inner join ${connection.schema}.dim_section on dim_section.section_id = staging_teacher_activities.section_uuid
         |  inner join ${connection.schema}.dim_subject on dim_subject.subject_id = staging_teacher_activities.subject_uuid
         |  inner join ${connection.schema}_stage.rel_user teacher1 on teacher1.user_id = staging_teacher_activities.teacher_uuid
         |  order by fta_staging_id limit $QUERY_LIMIT
       """.stripMargin

    val insertQuery =
      s"""
         |insert into ${connection.schema}.fact_teacher_activities (
         |  fta_created_time,
         |	fta_dw_created_time,
         |	fta_actor_object_type,
         |	fta_actor_account_homepage,
         |	fta_verb_id,
         |	fta_verb_display,
         |	fta_object_id,
         |	fta_object_type,
         |	fta_object_definition_type,
         |	fta_object_definition_name,
         |	fta_context_category,
         |	fta_outside_of_school,
         |	fta_event_type,
         |	fta_prev_event_type,
         |	fta_next_event_type,
         |	fta_date_dw_id,
         |	fta_tenant_dw_id,
         |	fta_school_dw_id,
         |	fta_grade_dw_id,
         |	fta_section_dw_id,
         |	fta_subject_dw_id,
         |	fta_teacher_dw_id,
         |	fta_start_time,
         |	fta_end_time,
         |	fta_timestamp_local,
         |	fta_time_spent)
         |(select
         |  fta_created_time,
         |	getdate(),
         |	fta_actor_object_type,
         |	fta_actor_account_homepage,
         |	fta_verb_id,
         |	fta_verb_display,
         |	fta_object_id,
         |	fta_object_type,
         |	fta_object_definition_type,
         |	fta_object_definition_name,
         |	fta_context_category,
         |	fta_outside_of_school,
         |	fta_event_type,
         |	fta_prev_event_type,
         |	fta_next_event_type,
         |	fta_date_dw_id,
         |	dim_tenant.tenant_dw_id,
         |	dim_school.school_dw_id,
         |	dim_grade.grade_dw_id,
         |	dim_section.section_dw_id,
         |	dim_subject.subject_dw_id,
         |	teacher1.user_dw_id,
         |	fta_start_time,
         |	fta_end_time,
         |	fta_timestamp_local,
         |	fta_time_spent
         |from ${connection.schema}_stage.staging_teacher_activities
         |  inner join ${connection.schema}.dim_tenant on dim_tenant.tenant_id = staging_teacher_activities.tenant_uuid
         |  inner join ${connection.schema}.dim_school on dim_school.school_id = staging_teacher_activities.school_uuid
         |  inner join ${connection.schema}.dim_grade on dim_grade.grade_id = staging_teacher_activities.grade_uuid
         |  inner join ${connection.schema}.dim_section on dim_section.section_id = staging_teacher_activities.section_uuid
         |  inner join ${connection.schema}.dim_subject on dim_subject.subject_id = staging_teacher_activities.subject_uuid
         |  inner join ${connection.schema}_stage.rel_user  teacher1 on teacher1.user_id = staging_teacher_activities.teacher_uuid
         |  order by fta_staging_id limit $QUERY_LIMIT);
    """.stripMargin

    log.info(s"Prepare queries: $pkSelectQuery\n$insertQuery")

    List(
      QueryMeta(
        "staging_teacher_activities",
        pkSelectQuery,
        insertQuery
      )
    )
  }
}
