package com.alefeducation.warehouse.tutor

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object TutorConversationTransformer extends CommonRedshiftTransformer{
  override val pkCol: List[String] = List("ftc_dw_id")
  override val mainColumnNames: List[String] = List(
    "ftc_role",
    "ftc_context_id",
    "ftc_school_dw_id",
    "ftc_grade_dw_id",
    "ftc_grade",
    "ftc_subject_dw_id",
    "ftc_subject",
    "ftc_language",
    "ftc_tenant_dw_id",
    "ftc_created_time",
    "ftc_dw_created_time",
    "ftc_date_dw_id",
    "ftc_user_dw_id",
    "ftc_session_id",
    "ftc_activity_dw_id",
    "ftc_activity_status",
    "ftc_material_id",
    "ftc_material_type",
    "ftc_course_activity_container_dw_id",
    "ftc_outcome_dw_id",
    "ftc_conversation_max_tokens",
    "ftc_conversation_token_count",
    "ftc_system_prompt_tokens",
    "ftc_message_language",
    "ftc_message_feedback",
    "ftc_user_message_source",
    "ftc_user_message_tokens",
    "ftc_user_message_timestamp",
    "ftc_bot_message_source",
    "ftc_bot_message_tokens",
    "ftc_bot_message_timestamp",
    "ftc_bot_message_confidence",
    "ftc_bot_message_response_time",
    "ftc_session_state",
    "ftc_conversation_id",
    "ftc_message_id",
    "ftc_message_tokens",
    "ftc_suggestions_prompt_tokens",
    "ftc_activity_page_context_id",
    "ftc_student_location",
    "ftc_suggestion_clicked",
    "ftc_clicked_suggestion_id"
  )
  override val stagingTableName: String = "staging_tutor_conversation"
  override val mainTableName: String = "fact_tutor_conversation"

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
       |SELECT
       |   ${makeColumnNames(cols)}
       |FROM ${connection.schema}_stage.staging_tutor_conversation tc
       |INNER JOIN ${connection.schema}.dim_grade g ON g.grade_id = tc.ftc_grade_id
       |INNER JOIN ${connection.schema}.dim_tenant t ON t.tenant_id = tc.ftc_tenant_id
       |INNER JOIN ${connection.schema}.dim_school s ON s.school_id = tc.ftc_school_id
       |LEFT JOIN ${connection.schema}.dim_curriculum_subject csu ON csu.curr_subject_id = tc.ftc_subject_id and csu.curr_subject_status=1
       |INNER JOIN ${connection.schema}_stage.rel_user u ON u.user_id = tc.ftc_user_id
       |INNER JOIN ${connection.schema}.dim_learning_objective lo ON lo.lo_id = tc.ftc_activity_id
       |LEFT JOIN ${connection.schema}_stage.rel_dw_id_mappings plc ON plc.id = tc.ftc_level_id and plc.entity_type = 'course_activity_container'
       |LEFT JOIN ${connection.schema}.dim_outcome oc on oc.outcome_id = tc.ftc_outcome_id
       |ORDER BY ${pkNotation(stagingTableName)}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin

  override def pkNotation: Map[String, String] = Map("staging_tutor_conversation" -> "ftc_dw_id")

  def makeColumnNames(cols: List[String]): String = cols.map {
    case "ftc_user_dw_id" => "\tu.user_dw_id AS ftc_user_dw_id"
    case "ftc_tenant_dw_id" => "\tt.tenant_dw_id AS ftc_tenant_dw_id"
    case "ftc_grade_dw_id" => "\tg.grade_dw_id AS ftc_grade_dw_id"
    case "ftc_school_dw_id" => "\ts.school_dw_id AS ftc_school_dw_id"
    case "ftc_subject_dw_id" => "\tcsu.curr_subject_dw_id AS ftc_subject_dw_id"
    case "ftc_activity_dw_id" => "\tlo.lo_dw_id AS ftc_activity_dw_id"
    case "ftc_course_activity_container_dw_id" => "\tplc.dw_id AS ftc_course_activity_container_dw_id"
    case "ftc_outcome_dw_id" => "\toc.outcome_dw_id AS ftc_outcome_dw_id"
    case col => s"\t$col"
  }.mkString(",\n")
}
