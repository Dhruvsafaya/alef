package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}
import scalikejdbc.AutoSession

object SkillContentUnavailableTransformer extends RedshiftTransformer {

  val mainColumnNames: List[String] = List(
    "scu_created_time",
    "scu_dw_created_time",
    "scu_date_dw_id",
    "scu_tenant_dw_id",
    "scu_lo_dw_id",
    "scu_skill_dw_id"
  )


  def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       | ${selectedColNames(cols)}
       | from ${connection.schema}_stage.staging_skill_content_unavailable
       | inner join ${connection.schema}.dim_tenant on dim_tenant.tenant_id = staging_skill_content_unavailable.tenant_uuid
       | inner join ${connection.schema}.dim_learning_objective on dim_learning_objective.lo_id = staging_skill_content_unavailable.lo_uuid
       | inner join ${connection.schema}.dim_ccl_skill on dim_ccl_skill.ccl_skill_id = staging_skill_content_unavailable.skill_uuid  order by scu_staging_id limit $QUERY_LIMIT
       |""".stripMargin
  }

  override def pkNotation: Map[String, String] = Map("staging_skill_content_unavailable" -> "scu_staging_id")

  def selectedColNames(cols: List[String]): String =
    cols
      .map {
        case "scu_tenant_dw_id" => s"\tdim_tenant.tenant_dw_id"
        case "scu_skill_dw_id" => s"\tdim_ccl_skill.ccl_skill_dw_id"
        case "scu_lo_dw_id" => s"\tdim_learning_objective.lo_dw_id"
        case col => s"\t$col"
      }
      .mkString(",\n")



  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val pkSelectQuery = getSelectQuery(List("scu_staging_id"), connection)

    val selectStatement = getSelectQuery(mainColumnNames, connection)

    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.fact_skill_content_unavailable (${mainColumnNames.mkString(", ")})
         | (
         |  ${selectStatement}
         | )
         |""".stripMargin

    log.info(s"pkSelectQuery :  ${pkSelectQuery}")
    log.info(s"insertQuery : ${insertStatement}")

    List(
      QueryMeta(
        "staging_skill_content_unavailable",
        pkSelectQuery,
        insertStatement
      )
    )
  }
}