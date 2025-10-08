package com.alefeducation.warehouse

import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.warehouse.core.UpsertRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object AcademicYearTransformer extends UpsertRedshiftTransformer {

  override val primaryKeyColumns: List[String] = List(
    "academic_year_delta_dw_id"
  )
  override val mainColumnNames: List[String] = List(
    "academic_year_delta_dw_id",
    "academic_year_created_time",
    "academic_year_updated_time",
    "academic_year_deleted_time",
    "academic_year_dw_created_time",
    "academic_year_dw_updated_time",
    "academic_year_status",
    "academic_year_state",
    "academic_year_id",
    "academic_year_school_id",
    "academic_year_school_dw_id",
    "academic_year_start_date",
    "academic_year_end_date",
    "academic_year_is_roll_over_completed",
    "academic_year_organization_code",
    "academic_year_organization_dw_id",
    "academic_year_created_by",
    "academic_year_created_by_dw_id",
    "academic_year_updated_by",
    "academic_year_updated_by_dw_id",
    "academic_year_type"
  )
  override val stagingTableName: String = "rel_academic_year"
  override val mainTableName: String = "dim_academic_year"
  override val matchConditions: String =
    s"""$mainTableName.academic_year_id = $TempTableAlias.academic_year_id
        | AND NVL($mainTableName.academic_year_school_id, '') = NVL($TempTableAlias.academic_year_school_id, '')""".stripMargin

  override val columnsToInsert: Map[String, String] = mainColumnNames.map { column =>
    column -> s"$TempTableAlias.$column"
  }.toMap

  override val columnsToUpdate: Map[String, String] = columnsToInsert - "academic_year_dw_created_time" +
    ("academic_year_dw_updated_time" -> s"$TempTableAlias.academic_year_dw_created_time")

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
       |SELECT
       |   ${makeColumnNames(cols)}
       |${fromStatement(connection.schema)}
       |ORDER BY ${pkNotation(stagingTableName)}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin

  override def pkNotation: Map[String, String] = Map(stagingTableName -> "academic_year_delta_dw_id")

  private def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.$stagingTableName staging
       |   LEFT OUTER JOIN ${schema}_stage.rel_user cb ON cb.user_id = staging.academic_year_created_by
       |   LEFT OUTER JOIN ${schema}_stage.rel_user ub ON ub.user_id = staging.academic_year_updated_by
       |   LEFT OUTER JOIN $schema.dim_organization org ON org.organization_code = staging.academic_year_organization_code
       |   LEFT OUTER JOIN $schema.dim_school ON dim_school.school_id = staging.academic_year_school_id
       |WHERE ((staging.academic_year_organization_code IS NULL AND org.organization_code IS NULL) OR
       |        (staging.academic_year_organization_code IS NOT NULL AND org.organization_code IS NOT NULL))
       |      AND ((staging.academic_year_school_id IS NULL AND dim_school.school_id IS NULL) OR
       |        (staging.academic_year_school_id IS NOT NULL AND dim_school.school_id IS NOT NULL))
       |""".stripMargin

  def makeColumnNames(cols: List[String]): String =
    cols
      .map {
        case "academic_year_created_by_dw_id"    => s"\tcb.user_dw_id AS academic_year_created_by_dw_id"
        case "academic_year_updated_by_dw_id"    => s"\tub.user_dw_id AS academic_year_updated_by_dw_id"
        case "academic_year_organization_dw_id"  => s"\torg.organization_dw_id AS academic_year_organization_dw_id"
        case "academic_year_school_dw_id"        => s"\tdim_school.school_dw_id AS academic_year_school_dw_id"
        case col                                 => s"\t$col"
      }
      .mkString(",\n")
}
