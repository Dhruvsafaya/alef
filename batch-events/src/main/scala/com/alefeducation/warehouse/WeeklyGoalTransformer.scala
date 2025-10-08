package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}
import scalikejdbc.{AutoSession, SQL}

object WeeklyGoalTransformer extends RedshiftTransformer {
  override def tableNotation: Map[String, String] = Map.empty

  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation = Map(
    "staging_weekly_goal" -> "fwg_dw_id"
  )

  val pkCol = List("fwg_dw_id")

  val factColumnNames = List(
    "fwg_id",
    "fwg_student_dw_id",
    "fwg_type_dw_id",
    "fwg_class_dw_id",
    "fwg_action_status",
    "fwg_tenant_dw_id",
    "fwg_star_earned",
    "fwg_created_time",
    "fwg_dw_created_time"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val selectPkQuery = getSelectQuery(pkCol, connection)
    val selectQuery = getSelectQuery(factColumnNames, connection)
    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.fact_weekly_goal (${factColumnNames.mkString(", ")})
         | (
         |  $selectQuery
         | )
         |""".stripMargin
    log.info(s"pkSelectQuery :  $selectPkQuery")
    log.info(s"insertQuery : $insertStatement")
    List(QueryMeta("staging_weekly_goal", selectPkQuery, insertStatement))
  }

  private def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       |     ${makeColumnNames(cols)}
       |from ${connection.schema}_stage.staging_weekly_goal g
       |inner join ${connection.schema}.dim_weekly_goal_type wgt on wgt.weekly_goal_type_id = g.fwg_type_id
       |inner join ${connection.schema}.dim_tenant t on t.tenant_id = g.fwg_tenant_id
       |inner join ${connection.schema}_stage.rel_dw_id_mappings c on c.id = g.fwg_class_id and c.entity_type = 'class'
       |inner join ${connection.schema}_stage.rel_user s on s.user_id = g.fwg_student_id
       |
       |order by ${pkNotation("staging_weekly_goal")}
       |limit $QUERY_LIMIT
       |""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String = cols.map {
    case "fwg_tenant_dw_id" => "t.tenant_dw_id as fwg_tenant_dw_id"
    case "fwg_class_dw_id" => "c.dw_id as fwg_class_dw_id"
    case "fwg_student_dw_id" => "s.user_dw_id as fwg_student_dw_id"
    case "fwg_type_dw_id" => "wgt.weekly_goal_type_dw_id as fwg_type_dw_id"
    case col => col
  }.mkString(", ")

}
