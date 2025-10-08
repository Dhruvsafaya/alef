package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object ADTAttemptThresholdTransformer extends CommonTransformer{
  val mainColumnNames: List[String] = List(
    "aat_dw_id",
    "aat_created_time",
    "aat_dw_created_time",
    "aat_updated_time",
    "aat_dw_updated_time",
    "aat_id",
    "aat_status",
    "aat_attempt_start_time",
    "aat_state",
    "aat_attempt_number",
    "aat_attempt_title",
    "aat_total_attempts",
    "aat_attempt_end_time",
    "aat_tenant_id",
    "aat_tenant_dw_id",
    "aat_academic_year_id",
    "aat_academic_year_dw_id",
    "aat_school_id",
    "aat_school_dw_id"
  )

  override def getSelectQuery(schema: String): String =   s"""
                                                             |SELECT ${getPkColumn()}
                                                             |${fromStatement(schema)}
                                                             |ORDER BY ${getPkColumn()}
                                                             |LIMIT $QUERY_LIMIT
                                                             |""".stripMargin

  def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.${getStagingTableName()} staging
       |   JOIN $schema.dim_tenant t ON t.tenant_id = staging.aat_tenant_id
       |   JOIN $schema.dim_school s ON s.school_id = staging.aat_school_id
       |   JOIN $schema.dim_academic_year ay ON ay.academic_year_id = staging.aat_academic_year_id
       |""".stripMargin

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idClause = ids.mkString(",")
    val insCols = mainColumnNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColumnNames(mainColumnNames)
    s"""
       |INSERT INTO $schema.dim_adt_attempt_threshold ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |  AND staging.$getPkColumn IN ($idClause)
       |""".stripMargin
  }

  def makeColumnNames(cols: List[String]): String =
    cols
      .map {
        case "aat_tenant_dw_id"   => s"\tt.tenant_dw_id AS aat_tenant_dw_id"
        case "aat_school_dw_id"  => s"\ts.school_dw_id AS aat_school_dw_id"
        case "aat_academic_year_dw_id"  => s"\tay.academic_year_dw_id AS aat_academic_year_dw_id"
        case col                   => s"\t$col"
      }
      .mkString(",\n")

  override def getPkColumn(): String = "aat_dw_id"

  override def getStagingTableName(): String = "rel_adt_attempt_threshold"
}
