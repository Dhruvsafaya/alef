package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object MarketplacePurchaseTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
    "fip_dw_id",
    "fip_created_time",
    "fip_dw_created_time",
    "fip_date_dw_id",
    "fip_id",
    "fip_item_id",
    "fip_item_dw_id",
    "fip_item_type",
    "fip_item_title",
    "fip_item_description",
    "fip_transaction_id",
    "fip_school_id",
    "fip_school_dw_id",
    "fip_grade_id",
    "fip_grade_dw_id",
    "fip_section_id",
    "fip_section_dw_id",
    "fip_academic_year_id",
    "fip_academic_year_dw_id",
    "fip_academic_year",
    "fip_student_id",
    "fip_student_dw_id",
    "fip_tenant_id",
    "fip_tenant_dw_id",
    "fip_redeemed_stars"
  )

  override def getSelectQuery(schema: String): String = {
    val fipPkCol = getPkColumn()
    val from = fromStatement(schema)

    s"SELECT $fipPkCol $from ORDER BY $fipPkCol LIMIT $QUERY_LIMIT"
  }

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idsInStatement = ids.mkString(",")
    val insCols = mainColNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColNames(mainColNames)
    s"""
       |INSERT INTO $schema.fact_item_purchase ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "fip_dw_id"

  override def getStagingTableName(): String = "staging_item_purchase"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       |  JOIN $schema.dim_tenant t ON t.tenant_id = staging.fip_tenant_id
       |  JOIN $schema.dim_school s ON s.school_id = staging.fip_school_id
       |  LEFT JOIN $schema.dim_grade g ON g.grade_id = staging.fip_grade_id
       |  LEFT JOIN $schema.dim_section sec ON sec.section_id = staging.fip_section_id
       |  JOIN $schema.dim_academic_year ay ON ay.academic_year_id = staging.fip_academic_year_id
       |  JOIN ${schema}_stage.rel_user u ON u.user_id = staging.fip_student_id
       |  LEFT JOIN $schema.dim_avatar a ON a.avatar_id = staging.fip_item_id AND staging.fip_item_type = 'AVATAR'
       |  LEFT JOIN $schema.dim_avatar_layer al ON al.avatar_layer_id = staging.fip_item_id AND staging.fip_item_type = 'AVATAR_LAYER'
       |  LEFT JOIN $schema.dim_marketplace_config mc ON mc.id = staging.fip_item_id AND staging.fip_item_type = 'PLANT_A_TREE'
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "fip_tenant_dw_id"  => s"\tt.tenant_dw_id AS fip_tenant_dw_id"
        case "fip_school_dw_id"  => s"\ts.school_dw_id AS fip_school_dw_id"
        case "fip_grade_dw_id"  => s"\tg.grade_dw_id AS fip_grade_dw_id"
        case "fip_section_dw_id"  => s"\tsec.section_dw_id AS fip_section_dw_id"
        case "fip_academic_year_dw_id"  => s"\tay.academic_year_dw_id AS fip_academic_year_dw_id"
        case "fip_student_dw_id"  => s"\tu.user_dw_id AS fip_student_dw_id"
        case "fip_item_dw_id" => s"""\tCASE WHEN a.avatar_dw_id is not NULL
                                        THEN a.avatar_dw_id
                                        WHEN al.avatar_layer_dw_id is not NULL
                                        THEN al.avatar_layer_dw_id
                                        ELSE mc.dw_id END AS fip_item_dw_id""".stripMargin
        case col => s"\t$col"
      }.mkString(",\n")

}
