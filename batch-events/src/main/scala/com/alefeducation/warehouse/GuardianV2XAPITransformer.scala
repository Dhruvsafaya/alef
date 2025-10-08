package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object GuardianV2XAPITransformer extends RedshiftTransformer {

  override def tableNotation: Map[String, String] = Map.empty

  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation: Map[String, String] = Map(
    "staging_guardian_app_activities" -> "fgaa_staging_id"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val pkSelectQuery =
      s"""
         |select fgaa_staging_id
         |from ${connection.schema}_stage.staging_guardian_app_activities
         |  inner join ${connection.schema}.dim_tenant on dim_tenant.tenant_id = staging_guardian_app_activities.tenant_uuid
         |  inner join ${connection.schema}_stage.rel_user  guardian1 on guardian1.user_id = staging_guardian_app_activities.guardian_uuid
         |  inner join ${connection.schema}_stage.rel_user  student1 on student1.user_id = staging_guardian_app_activities.student_uuid
         |  inner join ${connection.schema}.dim_school on dim_school.school_id = staging_guardian_app_activities.school_uuid
         |  order by fgaa_staging_id limit $QUERY_LIMIT
       """.stripMargin

    val insertQuery =
      s"""
         |insert into ${connection.schema}.fact_guardian_app_activities (
         |  fgaa_created_time,
         |	fgaa_actor_object_type,
         |	fgaa_actor_account_homepage,
         |	fgaa_verb_display,
         |	fgaa_verb_id,
         |	fgaa_object_id,
         |	fgaa_object_type,
         |	fgaa_object_account_homepage,
         |	fgaa_dw_created_time,
         |	fgaa_event_type,
         |	fgaa_date_dw_id,
         |	fgaa_device,
         |	fgaa_tenant_dw_id,
         |	fgaa_guardian_dw_id,
         |	fgaa_student_dw_id,
         |	fgaa_school_dw_id,
         |	fgaa_timestamp_local)
         |(select
         |  fgaa_created_time,
         |	fgaa_actor_object_type,
         |	fgaa_actor_account_homepage,
         |	fgaa_verb_display,
         |	fgaa_verb_id,
         |	fgaa_object_id,
         |	fgaa_object_type,
         |	fgaa_object_account_homepage,
         |	getdate(),
         |	fgaa_event_type,
         |	fgaa_date_dw_id,
         |	fgaa_device,
         |	dim_tenant.tenant_dw_id,
         |	guardian1.user_dw_id,
         |	student1.user_dw_id,
         |	dim_school.school_dw_id,
         |	fgaa_timestamp_local
         |from ${connection.schema}_stage.staging_guardian_app_activities
         |  inner join ${connection.schema}.dim_tenant on dim_tenant.tenant_id = staging_guardian_app_activities.tenant_uuid
         |  inner join ${connection.schema}_stage.rel_user  guardian1 on guardian1.user_id = staging_guardian_app_activities.guardian_uuid
         |  inner join ${connection.schema}_stage.rel_user  student1 on student1.user_id = staging_guardian_app_activities.student_uuid
         |  inner join ${connection.schema}.dim_school on dim_school.school_id = staging_guardian_app_activities.school_uuid
         |  order by fgaa_staging_id limit $QUERY_LIMIT);
    """.stripMargin

    log.info(s"Prepare queries: $pkSelectQuery\n$insertQuery")

    List(
      QueryMeta(
        "staging_guardian_app_activities",
        pkSelectQuery,
        insertQuery
      )
    )
  }

}