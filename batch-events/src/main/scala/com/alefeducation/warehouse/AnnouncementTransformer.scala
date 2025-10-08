package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object AnnouncementTransformer extends CommonRedshiftTransformer {

  override val pkCol: List[String] = List("fa_dw_id")
  override val mainColumnNames: List[String] = List(
    "fa_created_time",
    "fa_dw_created_time",
    "fa_status",
    "fa_tenant_dw_id",
    "fa_id",
    "fa_admin_dw_id",
    "fa_role_dw_id",
    "fa_recipient_type",
    "fa_recipient_type_description",
    "fa_recipient_dw_id",
    "fa_has_attachment",
    "fa_type"
  )
  override val stagingTableName: String = "staging_announcement"
  override val mainTableName: String = "fact_announcement"

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
       |SELECT
       |  ${makeColumnNames(cols)}
       |FROM ${connection.schema}_stage.staging_announcement a
       |INNER JOIN ${connection.schema}.dim_role r ON r.role_name = a.fa_role_id
       |INNER JOIN ${connection.schema}.dim_tenant t ON t.tenant_id = a.fa_tenant_id
       |INNER JOIN ${connection.schema}_stage.rel_user u ON u.user_id = a.fa_admin_id
       |LEFT JOIN ${connection.schema}.dim_school s ON s.school_id = a.fa_recipient_id and a.fa_recipient_type = 1
       |LEFT JOIN ${connection.schema}.dim_grade g ON g.grade_id = a.fa_recipient_id and a.fa_recipient_type = 2
       |LEFT JOIN ${connection.schema}_stage.rel_dw_id_mappings c
       |          ON c.id = a.fa_recipient_id and a.fa_recipient_type = 3 and c.entity_type = 'class'
       |LEFT JOIN ${connection.schema}_stage.rel_user us ON us.user_id = a.fa_recipient_id and a.fa_recipient_type = 4
       |WHERE s.school_id IS NOT NULL OR g.grade_id IS NOT NULL OR c.id IS NOT NULL OR us.user_id IS NOT NULL
       |ORDER BY ${pkNotation("staging_announcement")}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin

  override def pkNotation: Map[String, String] = Map("staging_announcement" -> "fa_dw_id")

  def makeColumnNames(cols: List[String]): String = cols.map {
    case "fa_role_dw_id" => "\tr.role_dw_id AS fa_role_dw_id"
    case "fa_admin_dw_id" => "\tu.user_dw_id AS fa_admin_dw_id"
    case "fa_tenant_dw_id" => "\tt.tenant_dw_id AS fa_tenant_dw_id"
    case "fa_recipient_dw_id" => """DECODE(a.fa_recipient_type,
                                   |       1, s.school_dw_id,
                                   |       2, g.grade_dw_id,
                                   |       3, c.dw_id,
                                   |       4, us.user_dw_id
                                   |       ) AS fa_recipient_dw_id
                                   |""".stripMargin
    case col => s"\t$col"
  }.mkString(",\n")
}
