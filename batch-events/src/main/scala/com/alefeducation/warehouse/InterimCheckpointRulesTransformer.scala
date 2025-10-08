package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.{AutoSession, SQL}
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object InterimCheckpointRulesTransformer extends RedshiftTransformer {

  override def tableNotation: Map[String, String] = Map.empty

  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation = Map(
    "rel_interim_checkpoint_rules" -> "rel_ic_rule_id"
  )

  val dwIds = List("ic_rule_ic_dw_id", "ic_rule_outcome_dw_id")

  val uuids = List(
    "ic.ic_dw_id as ic_rule_ic_dw_id",
    "oc.outcome_dw_id as ic_rule_outcome_dw_id"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val table = pkNotation.keys.headOption.getOrElse("").replace("rel", "dim")
    val tableColumns =
      SQL(
        s"select column_name from information_schema.columns where " +
          s"table_schema = '${connection.schema}' and table_name = '$table' and " +
          s"ordinal_position != 1 order by ordinal_position;")
        .map(_.string("column_name"))
        .list
        .apply

    val dimTableColumns = tableColumns.diff(dwIds)
    val pkSelectQuery = getSelectQuery(List(pkNotation("rel_interim_checkpoint_rules")), connection)
    val selectStatement = getSelectQuery(dimTableColumns ++ uuids, connection)

    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.dim_interim_checkpoint_rules (${(dimTableColumns ++ dwIds).mkString(", ")})
         | (
         |  ${selectStatement}
         | )
         |""".stripMargin

    log.info(s"pkSelectQuery :  ${pkSelectQuery}")
    log.info(s"insertQuery : ${insertStatement}")

    List(
      QueryMeta(
        "rel_interim_checkpoint_rules",
        pkSelectQuery,
        insertStatement
      )
    )
  }

  def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       |     ${cols.mkString(",")}
       |from ${connection.schema}_stage.rel_interim_checkpoint_rules icr
       |inner join ${connection.schema}.dim_interim_checkpoint ic on ic.ic_id = icr.ic_uuid
       |inner join ${connection.schema}.dim_outcome oc on oc.outcome_id = icr.outcome_uuid
       |
       |order by ${pkNotation("rel_interim_checkpoint_rules")}
       |limit $QUERY_LIMIT
       |""".stripMargin
  }

}
