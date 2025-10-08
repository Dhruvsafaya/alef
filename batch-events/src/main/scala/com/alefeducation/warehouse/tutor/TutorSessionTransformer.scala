package com.alefeducation.warehouse.tutor

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection


object TutorSessionTransformer extends CommonRedshiftTransformer{
  override val pkCol: List[String] = List("fts_dw_id")
  override val mainColumnNames: List[String] = List(
    "fts_role",
    "fts_context_id",
    "fts_school_dw_id",
    "fts_grade_dw_id",
    "fts_grade",
    "fts_subject_dw_id",
    "fts_subject",
    "fts_language",
    "fts_tenant_dw_id",
    "fts_created_time",
    "fts_dw_created_time",
    "fts_date_dw_id",
    "fts_user_dw_id",
    "fts_session_id",
    "fts_session_state",
    "fts_activity_dw_id",
    "fts_activity_status",
    "fts_material_id",
    "fts_material_type",
    "fts_course_activity_container_dw_id",
    "fts_outcome_dw_id",
    "fts_session_message_limit_reached",
    "fts_learning_session_id"
  )
  override val stagingTableName: String = "staging_tutor_session"
  override val mainTableName: String = "fact_tutor_session"

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
       |SELECT
       |   ${makeColumnNames(cols)}
       |FROM ${connection.schema}_stage.staging_tutor_session ts
       |INNER JOIN ${connection.schema}.dim_grade g ON g.grade_id = ts.fts_grade_id
       |INNER JOIN ${connection.schema}.dim_tenant t ON t.tenant_id = ts.fts_tenant_id
       |INNER JOIN ${connection.schema}.dim_school s ON s.school_id = ts.fts_school_id
       |INNER JOIN ${connection.schema}.dim_curriculum_subject csu ON csu.curr_subject_id = ts.fts_subject_id and csu.curr_subject_status=1
       |INNER JOIN ${connection.schema}_stage.rel_user u ON u.user_id = ts.fts_user_id
       |INNER JOIN ${connection.schema}.dim_learning_objective lo ON lo.lo_id = ts.fts_activity_id
       |LEFT JOIN ${connection.schema}_stage.rel_dw_id_mappings plc ON plc.id = ts.fts_level_id and plc.entity_type = 'course_activity_container'
       |LEFT JOIN ${connection.schema}.dim_outcome oc on oc.outcome_id = ts.fts_outcome_id
       |ORDER BY ${pkNotation(stagingTableName)}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin

  override def pkNotation: Map[String, String] = Map("staging_tutor_session" -> "fts_dw_id")

  def makeColumnNames(cols: List[String]): String = cols.map {
    case "fts_user_dw_id" => "\tu.user_dw_id AS fts_user_dw_id"
    case "fts_tenant_dw_id" => "\tt.tenant_dw_id AS fts_tenant_dw_id"
    case "fts_grade_dw_id" => "\tg.grade_dw_id AS fts_grade_dw_id"
    case "fts_school_dw_id" => "\ts.school_dw_id AS fts_school_dw_id"
    case "fts_subject_dw_id" => "\tcsu.curr_subject_dw_id AS fts_subject_dw_id"
    case "fts_activity_dw_id" => "\tlo.lo_dw_id AS fts_activity_dw_id"
    case "fts_course_activity_container_dw_id" => "\tplc.dw_id AS fts_course_activity_container_dw_id"
    case "fts_outcome_dw_id" => "\toc.outcome_dw_id AS fts_outcome_dw_id"
    case col => s"\t$col"
  }.mkString(",\n")
}

