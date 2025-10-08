package com.alefeducation.warehouse

import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.warehouse.core.UpsertRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object HeartbeatAggregationTransformer extends UpsertRedshiftTransformer {

  override val mainTableName: String = "fact_user_heartbeat_hourly_aggregated"
  override val stagingTableName: String = "staging_user_heartbeat_hourly_aggregated"

  override def pkNotation: Map[String, String] = Map(stagingTableName -> "fuhha_staging_id")
  override val primaryKeyColumns: List[String] = List("fuhha_staging_id")

  override val mainColumnNames: List[String] = List(
    "fuhha_created_time",
    "fuhha_dw_created_time",
    "fuhha_date_dw_id",
    "fuhha_user_dw_id",
    "fuhha_role_dw_id",
    "fuhha_school_dw_id",
    "fuhha_content_repository_dw_id",
    "fuhha_tenant_dw_id",
    "fuhha_channel",
    "fuhha_activity_date_hour"
  )

  override val matchConditions: String =
    s"""
       |$mainTableName.fuhha_user_dw_id = $TempTableAlias.fuhha_user_dw_id
       | AND $mainTableName.fuhha_activity_date_hour = $TempTableAlias.fuhha_activity_date_hour
       | """.stripMargin

  override val columnsToInsert: Map[String, String] = mainColumnNames.map { column =>
    column -> s"$TempTableAlias.$column"
  }.toMap

  override val columnsToUpdate: Map[String, String] = columnsToInsert -
    "fuhha_dw_created_time" +
    ("fuhha_dw_updated_time" -> s"$TempTableAlias.fuhha_dw_created_time")

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    val pk = if (cols.contains(pkNotation(stagingTableName))) {
      ""
    } else {
      s"${pkNotation(stagingTableName)},\n"
    }
    s"""
       |SELECT
       |  ${cols.mkString(",\n")}
       |FROM (
       |  SELECT
       |    $pk ${makeColumnNames(cols)}
       |
       |  FROM ${connection.schema}_stage.$stagingTableName staging
       |  INNER JOIN ${connection.schema}.dim_role r ON UPPER(staging.fuhha_role) = UPPER(r.role_name)
       |  INNER JOIN ${connection.schema}.dim_tenant t ON staging.fuhha_tenant_id = t.tenant_id
       |  INNER JOIN ${connection.schema}_stage.rel_user u ON staging.fuhha_user_id = u.user_id
       |  INNER JOIN ${connection.schema}.dim_school s ON staging.fuhha_school_id = s.school_id
       |
       |  ORDER BY ${pkNotation(stagingTableName)}
       |  LIMIT $QUERY_LIMIT
       |)
       |""".stripMargin
  }

  def makeColumnNames(cols: List[String]): String =
    cols
      .map {
        case "fuhha_role_dw_id"                             => "r.role_dw_id AS fuhha_role_dw_id"
        case "fuhha_tenant_dw_id"                           => "t.tenant_dw_id AS fuhha_tenant_dw_id"
        case "fuhha_school_dw_id"                           => "s.school_dw_id AS fuhha_school_dw_id"
        case "fuhha_content_repository_dw_id"               => "s.school_content_repository_dw_id AS fuhha_content_repository_dw_id"
        case "fuhha_user_dw_id"                             => "u.user_dw_id AS fuhha_user_dw_id"
        case col                                            => s"$col"
      }
      .mkString(",\n")
}
