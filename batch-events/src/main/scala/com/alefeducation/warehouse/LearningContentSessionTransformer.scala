package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object LearningContentSessionTransformer extends RedshiftTransformer {

  override def tableNotation: Map[String, String] = Map.empty
  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation = Map(
    "staging_content_session" -> "fcs_staging_id"
  )

  val factTableName = "fact_content_session"

  val pkCol = List(
    "fcs_staging_id"
  )

  val factTableColumns = List(
    "fcs_content_id",
    "fcs_created_time",
    "fcs_id",
    "fcs_event_type",
    "fcs_is_start",
    "fcs_ip_id",
    "fcs_ls_id",
    "fcs_outside_of_school",
    "fcs_content_academic_year",
    "fcs_app_timespent",
    "fcs_app_score",
    "fcs_dw_created_time",
    "fcs_date_dw_id"
  )

  val dw_ids = List(
    "fcs_lo_dw_id",
    "fcs_student_dw_id",
    "fcs_class_dw_id",
    "fcs_grade_dw_id",
    "fcs_tenant_dw_id",
    "fcs_school_dw_id",
    "fcs_ay_dw_id",
    "fcs_section_dw_id",
    "fcs_lp_dw_id"
  )

  val uuids = List(
    "dlo.lo_dw_id as fcs_lo_dw_id",
    "ru.user_dw_id as fcs_student_dw_id",
    "dm.dw_id as fcs_class_dw_id",
    "dg.grade_dw_id as fcs_grade_dw_id",
    "dt.tenant_dw_id as fcs_tenant_dw_id",
    "dsc.school_dw_id as fcs_school_dw_id",
    "day.academic_year_dw_id as fcs_ay_dw_id",
    "dse.section_dw_id as fcs_section_dw_id",
    "dlp.learning_path_dw_id as fcs_lp_dw_id"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val pkSelectQuery = getSelectQuery(pkCol, connection)

    val selectQuery = getSelectQuery(factTableColumns ++ uuids, connection)

    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.$factTableName (${(factTableColumns ++ dw_ids).mkString(", ")})
         | (
         |  ${selectQuery}
         | )
         |""".stripMargin

    List(
      QueryMeta(
        "staging_content_session",
        pkSelectQuery,
        insertStatement
      )
    )
  }

  private def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       |     ${cols.mkString(",")}
       |from ${connection.schema}_stage.staging_content_session cs
       | inner join ${connection.schema}.dim_learning_objective dlo on dlo.lo_id=cs.fcs_lo_id
       | inner join ${connection.schema}_stage.rel_user ru on ru.user_id = cs.fcs_student_id
       | left join ${connection.schema}_stage.rel_dw_id_mappings dm on dm.id = cs.fcs_class_id and dm.entity_type = 'class'
       | inner join ${connection.schema}.dim_grade dg on dg.grade_id = cs.fcs_grade_id
       | inner join ${connection.schema}.dim_tenant dt on dt.tenant_id = cs.fcs_tenant_id
       | inner join ${connection.schema}.dim_school dsc on dsc.school_id = cs.fcs_school_id
       | left join ${connection.schema}.dim_academic_year day on day.academic_year_id=cs.fcs_ay_id
       | left join ${connection.schema}.dim_section dse on dse.section_id = cs.fcs_section_id
       | left join ${connection.schema}.dim_learning_path dlp on dlp.learning_path_id=cs.fcs_lp_id || cs.fcs_school_id || cs.fcs_class_id
       |  and dlp.learning_path_status = 1
       |where
       | (cs.fcs_ay_id IS NULL AND day.academic_year_dw_id IS NULL) OR
       | (cs.fcs_ay_id IS NOT NULL AND day.academic_year_dw_id IS NOT NULL)
       |order by ${pkCol.mkString(",")}
       |limit $QUERY_LIMIT
       |""".stripMargin
  }

}
