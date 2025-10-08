package com.alefeducation.warehouse.pathway

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object PathwayLeaderboardTransformer extends CommonRedshiftTransformer {

  override val pkCol: List[String] = List("fpl_staging_id")
  override val mainColumnNames: List[String] = List(
    "fpl_created_time",
    "fpl_dw_created_time",
    "fpl_date_dw_id",
    "fpl_id",
    "fpl_student_dw_id",
    "fpl_course_dw_id",
    "fpl_class_dw_id",
    "fpl_grade_dw_id",
    "fpl_academic_year_dw_id",
    "fpl_start_date",
    "fpl_end_date",
    "fpl_order",
    "fpl_level_competed_count",
    "fpl_average_score",
    "fpl_total_stars",
    "fpl_tenant_dw_id"
  )
  override val stagingTableName: String = "staging_pathway_leaderboard"
  override val mainTableName: String = "fact_pathway_leaderboard"

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
       |SELECT
       |     ${makeColumnNames(cols)}
       |FROM ${connection.schema}_stage.staging_pathway_leaderboard pl
       | INNER JOIN ${connection.schema}.dim_tenant t ON t.tenant_id = pl.fpl_tenant_id
       | INNER JOIN ${connection.schema}_stage.rel_user u ON u.user_id = pl.fpl_student_id
       | INNER JOIN ${connection.schema}_stage.rel_dw_id_mappings c ON c.id = pl.fpl_class_id and c.entity_type = 'class'
       | INNER JOIN ${connection.schema}_stage.rel_dw_id_mappings pc ON pc.id = pl.fpl_pathway_id and pc.entity_type = 'course'
       | INNER JOIN ${connection.schema}.dim_grade g ON g.grade_id = pl.fpl_grade_id
       | LEFT JOIN ${connection.schema}.dim_academic_year day ON day.academic_year_id = pl.fpl_academic_year_id AND day.academic_year_status = 1
       |WHERE
       |  (pl.fpl_academic_year_id IS NULL AND day.academic_year_dw_id IS NULL) OR
       |  (pl.fpl_academic_year_id IS NOT NULL AND day.academic_year_dw_id IS NOT NULL)
       |ORDER BY ${pkNotation("staging_pathway_leaderboard")}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin

  override def pkNotation: Map[String, String] = Map("staging_pathway_leaderboard" -> "fpl_staging_id")

  def makeColumnNames(cols: List[String]): String = cols.map {
    case "fpl_tenant_dw_id" => "\tt.tenant_dw_id AS fpl_tenant_dw_id"
    case "fpl_grade_dw_id" => "\tg.grade_dw_id AS fpl_grade_dw_id"
    case "fpl_student_dw_id" => "\tu.user_dw_id AS fpl_student_dw_id"
    case "fpl_academic_year_dw_id" => "\tday.academic_year_dw_id AS fba_academic_year_dw_id"
    case "fpl_course_dw_id" => "\tpc.dw_id AS fpl_course_dw_id"
    case "fpl_class_dw_id" => "\tc.dw_id AS fpl_class_dw_id"
    case col => s"\t$col"
  }.mkString(",\n")
}
