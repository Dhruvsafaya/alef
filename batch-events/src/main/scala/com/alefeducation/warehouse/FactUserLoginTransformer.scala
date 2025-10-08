package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object FactUserLoginTransformer extends CommonRedshiftTransformer {

  override def pkNotation = Map("staging_user_login" -> "ful_staging_id")

  override val pkCol = List("ful_staging_id")
  override val stagingTableName: String = "staging_user_login"
  override val mainTableName: String = "fact_user_login"

  override val mainColumnNames = List(
      "ful_created_time",
      "ful_dw_created_time",
      "ful_date_dw_id",
      "ful_id",
      "ful_user_dw_id",
      "ful_role_dw_id",
      "ful_tenant_dw_id",
      "ful_school_dw_id",
      "ful_outside_of_school",
      "ful_login_time"
  )

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       |     ${makeColumnNames(cols)}
       |from ${connection.schema}_stage.staging_user_login
       |inner join ${connection.schema}_stage.rel_user user1 on user1.user_id = staging_user_login.user_uuid
       |inner join ${connection.schema}.dim_role on dim_role.role_name = staging_user_login.role_uuid
       |left join ${connection.schema}.dim_tenant on dim_tenant.tenant_id = staging_user_login.tenant_uuid
       |left join ${connection.schema}.dim_school on dim_school.school_id = staging_user_login.school_uuid
       |
       |order by ${pkNotation("staging_user_login")}
       |limit $QUERY_LIMIT
       |""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String = cols.map {
    case "ful_dw_created_time" => "getdate() as ful_dw_created_time"
    case "ful_user_dw_id" => "user1.user_dw_id as ful_user_dw_id"
    case "ful_role_dw_id" => "dim_role.role_dw_id as ful_role_dw_id"
    case "ful_tenant_dw_id" => "dim_tenant.tenant_dw_id as ful_tenant_dw_id"
    case "ful_school_dw_id" => "dim_school.school_dw_id as ful_school_dw_id"
    case col => col
  }.mkString(",\n")

}
