package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object AcademicCalendarTransformer extends CommonTransformer {
  val mainColumnNames: List[String] = List(
    "academic_calendar_dw_id",
    "academic_calendar_created_time",
    "academic_calendar_updated_time",
    "academic_calendar_deleted_time",
    "academic_calendar_dw_created_time",
    "academic_calendar_dw_updated_time",
    "academic_calendar_status",
    "academic_calendar_title",
    "academic_calendar_id",
    "academic_calendar_school_id",
    "academic_calendar_school_dw_id",
    "academic_calendar_is_default",
    "academic_calendar_type",
    "academic_calendar_academic_year_id",
    "academic_calendar_academic_year_dw_id",
    "academic_calendar_organization",
    "academic_calendar_tenant_id",
    "academic_calendar_organization_dw_id",
    "academic_calendar_created_by_id",
    "academic_calendar_created_by_dw_id",
    "academic_calendar_updated_by_id",
    "academic_calendar_updated_by_dw_id"
  )

  override def getSelectQuery(schema: String): String =
    s"""
       |SELECT
       |   staging.$getPkColumn
       |${fromStatement(schema)}
       |ORDER BY staging.$getPkColumn
       |LIMIT $QUERY_LIMIT
       |""".stripMargin

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idClause = ids.mkString(",")
    val insCols = mainColumnNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColumnNames(mainColumnNames)
     s"""
       |INSERT INTO $schema.dim_academic_calendar ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idClause)
       |""".stripMargin
  }

  override def getPkColumn(): String = "academic_calendar_dw_id"

  override def getStagingTableName(): String = "rel_academic_calendar"

  private def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.$getStagingTableName staging
       |   LEFT OUTER JOIN ${schema}_stage.rel_user cb ON cb.user_id = staging.academic_calendar_created_by_id
       |   LEFT OUTER JOIN ${schema}_stage.rel_user ub ON ub.user_id = staging.academic_calendar_updated_by_id
       |   JOIN $schema.dim_organization org ON org.organization_code = staging.academic_calendar_organization
       |   JOIN $schema.dim_academic_year ay ON ay.academic_year_id = staging.academic_calendar_academic_year_id and ay.academic_year_type = staging.academic_calendar_type
       |   LEFT OUTER JOIN $schema.dim_school ON dim_school.school_id = staging.academic_calendar_school_id
       |""".stripMargin


  def makeColumnNames(cols: List[String]): String =
    cols
      .map {
        case "academic_calendar_created_by_dw_id"    => s"\tcb.user_dw_id AS academic_calendar_created_by_dw_id"
        case "academic_calendar_updated_by_dw_id"    => s"\tub.user_dw_id AS academic_calendar_updated_by_dw_id"
        case "academic_calendar_organization_dw_id"  => s"\torg.organization_dw_id AS academic_calendar_organization_dw_id"
        case "academic_calendar_academic_year_dw_id"  => s"\tay.academic_year_dw_id AS academic_calendar_academic_year_dw_id"
        case "academic_calendar_school_dw_id"        => s"\tdim_school.school_dw_id AS academic_calendar_school_dw_id"
        case col                                 => s"\t$col"
      }
      .mkString(",\n")
}
