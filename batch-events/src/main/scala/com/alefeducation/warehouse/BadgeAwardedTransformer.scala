package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object BadgeAwardedTransformer extends CommonRedshiftTransformer {

  override val pkCol: List[String] = List("fba_dw_id")

  override val mainColumnNames: List[String] = List(
    "fba_id",
    "fba_created_time",
    "fba_badge_dw_id",
    "fba_student_dw_id",
    "fba_school_dw_id",
    "fba_grade_dw_id",
    "fba_section_dw_id",
    "fba_tenant_dw_id",
    "fba_academic_year_dw_id",
    "fba_content_repository_dw_id",
    "fba_organization_dw_id",
    "fba_date_dw_id"
  )

  override val stagingTableName: String = "staging_badge_awarded"

  override val mainTableName: String = "fact_badge_awarded"

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
      |SELECT
      |   ${makeColumnNames(cols)}
      |FROM ${connection.schema}_stage.staging_badge_awarded ba
      | INNER JOIN ${connection.schema}.dim_grade g ON g.grade_id = ba.fba_grade_id
      | INNER JOIN ${connection.schema}.dim_badge b ON ba.fba_badge_type_id = b.bdg_id AND ba.fba_tier = b.bdg_tier
      |     AND g.grade_k12grade = b.bdg_grade AND b.bdg_status = 1
      | INNER JOIN ${connection.schema}.dim_tenant t ON ba.fba_tenant_id = t.tenant_id
      | INNER JOIN ${connection.schema}.dim_school s ON s.school_id = ba.fba_school_id
      | INNER JOIN ${connection.schema}_stage.rel_user u ON ba.fba_student_id = u.user_id
      | LEFT JOIN ${connection.schema}.dim_academic_year day ON day.academic_year_id = ba.fba_academic_year_id
      |     AND day.academic_year_status = 1 AND day.academic_year_is_roll_over_completed = false
      | LEFT OUTER JOIN ${connection.schema}.dim_section sec ON sec.section_id = ba.fba_section_id
      |WHERE
      | (ba.fba_academic_year_id IS NULL AND day.academic_year_dw_id IS NULL) OR
      | (ba.fba_academic_year_id IS NOT NULL AND day.academic_year_dw_id IS NOT NULL)
      |ORDER BY ${pkNotation(stagingTableName)}
      |LIMIT $QUERY_LIMIT
      |""".stripMargin

  override def pkNotation: Map[String, String] = Map("staging_badge_awarded" -> "fba_dw_id")

  def makeColumnNames(cols: List[String]): String = cols.map {
    case "fba_badge_dw_id" => "\tb.bdg_dw_id AS fba_badge_dw_id"
    case "fba_tenant_dw_id" => "\tt.tenant_dw_id AS fba_tenant_dw_id"
    case "fba_grade_dw_id" => "\tg.grade_dw_id AS fba_grade_dw_id"
    case "fba_school_dw_id" => "\ts.school_dw_id AS fba_school_dw_id"
    case "fba_student_dw_id" => "\tu.user_dw_id AS fba_student_dw_id"
    case "fba_academic_year_dw_id" => "\tday.academic_year_dw_id AS fba_academic_year_dw_id"
    case "fba_section_dw_id" => "\tsec.section_dw_id AS fba_section_dw_id"
    case "fba_content_repository_dw_id" => "\ts.school_content_repository_dw_id AS fba_content_repository_dw_id"
    case "fba_organization_dw_id" => "\ts.school_organization_dw_id AS fba_organization_dw_id"
    case col => s"\t$col"
  }.mkString(",\n")
}
