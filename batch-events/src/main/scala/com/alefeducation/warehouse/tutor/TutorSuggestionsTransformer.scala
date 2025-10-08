package com.alefeducation.warehouse.tutor

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object TutorSuggestionsTransformer extends CommonRedshiftTransformer{
  override val pkCol: List[String] = List("fts_dw_id")
  override val mainColumnNames: List[String] = List(
    "fts_dw_id",
    "fts_message_id",
    "fts_suggestion_id",
    "fts_created_time",
    "fts_dw_created_time",
    "fts_user_id",
    "fts_user_dw_id",
    "fts_session_id",
    "fts_conversation_id",
    "fts_response_time",
    "fts_success_parser_tokens",
    "fts_failure_parser_tokens",
    "fts_suggestion_clicked",
    "fts_date_dw_id",
    "fts_tenant_id",
    "fts_tenant_dw_id"
  )
    override val stagingTableName: String = "staging_tutor_suggestions"
  override val mainTableName: String = "fact_tutor_suggestions"

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
       |SELECT
       |   ${makeColumnNames(cols)}
       |FROM ${connection.schema}_stage.staging_tutor_suggestions fts
       |INNER JOIN ${connection.schema}_stage.rel_user u ON u.user_id = fts.fts_user_id
       |INNER JOIN ${connection.schema}.dim_tenant t ON t.tenant_id = fts.fts_tenant_id
       |ORDER BY ${pkNotation(stagingTableName)}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin

  override def pkNotation: Map[String, String] = Map("staging_tutor_suggestions" -> "fts_dw_id")

  def makeColumnNames(cols: List[String]): String = cols.map {
    case "fts_user_dw_id" => "\tu.user_dw_id AS fts_user_dw_id"
    case "fts_tenant_dw_id" => "\tt.tenant_dw_id AS fts_tenant_dw_id"
    case col => s"\t$col"
  }.mkString(",\n")
}
