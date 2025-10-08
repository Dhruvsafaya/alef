package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object MarketplaceTransactionTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
    "fit_dw_id",
    "fit_created_time",
    "fit_dw_created_time",
    "fit_date_dw_id",
    "fit_id",
    "fit_item_id",
    "fit_item_dw_id",
    "fit_item_type",
    "fit_school_id",
    "fit_school_dw_id",
    "fit_grade_id",
    "fit_grade_dw_id",
    "fit_section_id",
    "fit_section_dw_id",
    "fit_academic_year_id",
    "fit_academic_year_dw_id",
    "fit_academic_year",
    "fit_student_id",
    "fit_student_dw_id",
    "fit_tenant_id",
    "fit_tenant_dw_id",
    "fit_available_stars",
    "fit_item_cost",
    "fit_star_balance"
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
       |INSERT INTO $schema.fact_item_transaction ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "fit_dw_id"

  override def getStagingTableName(): String = "staging_item_transaction"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       |  JOIN $schema.dim_tenant t ON t.tenant_id = staging.fit_tenant_id
       |  JOIN $schema.dim_school s ON s.school_id = staging.fit_school_id
       |  LEFT JOIN $schema.dim_grade g ON g.grade_id = staging.fit_grade_id
       |  LEFT JOIN $schema.dim_section sec ON sec.section_id = staging.fit_section_id
       |  JOIN $schema.dim_academic_year ay ON ay.academic_year_id = staging.fit_academic_year_id
       |  JOIN ${schema}_stage.rel_user u ON u.user_id = staging.fit_student_id
       |  LEFT JOIN $schema.dim_avatar a ON a.avatar_id = staging.fit_item_id AND staging.fit_item_type = 'AVATAR'
       |  LEFT JOIN $schema.dim_avatar_layer al ON al.avatar_layer_id = staging.fit_item_id AND staging.fit_item_type = 'AVATAR_LAYER'
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "fit_tenant_dw_id"  => s"\tt.tenant_dw_id AS fit_tenant_dw_id"
        case "fit_school_dw_id"  => s"\ts.school_dw_id AS fit_school_dw_id"
        case "fit_grade_dw_id"  => s"\tg.grade_dw_id AS fit_grade_dw_id"
        case "fit_section_dw_id"  => s"\tsec.section_dw_id AS fit_section_dw_id"
        case "fit_academic_year_dw_id"  => s"\tay.academic_year_dw_id AS fit_academic_year_dw_id"
        case "fit_student_dw_id"  => s"\tu.user_dw_id AS fit_student_dw_id"
        case "fit_item_dw_id" => s"""\tCASE WHEN a.avatar_dw_id is not NULL
                                        THEN a.avatar_dw_id
                                        ELSE al.avatar_layer_dw_id END AS fit_item_dw_id""".stripMargin
        case col => s"\t$col"
      }.mkString(",\n")
}
