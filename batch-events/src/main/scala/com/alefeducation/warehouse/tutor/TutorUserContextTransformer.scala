package com.alefeducation.warehouse.tutor

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object TutorUserContextTransformer extends CommonRedshiftTransformer{
  override val pkCol: List[String] = List("ftc_dw_id")
  override val mainColumnNames: List[String] = List(
    "ftc_role",
    "ftc_context_id",
    "ftc_school_dw_id",
    "ftc_grade_dw_id",
    "ftc_grade",
    "ftc_subject_dw_id",
    "ftc_subject",
    "ftc_language",
    "ftc_tenant_dw_id",
    "ftc_created_time",
    "ftc_dw_created_time",
    "ftc_date_dw_id",
    "ftc_user_dw_id",
    "ftc_tutor_locked"
  )
  override val stagingTableName: String = "staging_tutor_user_context"
  override val mainTableName: String = "fact_tutor_user_context"

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
       |SELECT
       |   ${makeColumnNames(cols)}
       |FROM ${connection.schema}_stage.staging_tutor_user_context tuc
       |INNER JOIN ${connection.schema}.dim_grade g ON g.grade_id = tuc.ftc_grade_id
       |INNER JOIN ${connection.schema}.dim_tenant t ON t.tenant_id = tuc.ftc_tenant_id
       |INNER JOIN ${connection.schema}.dim_school s ON s.school_id = tuc.ftc_school_id
       |LEFT JOIN ${connection.schema}.dim_curriculum_subject csu ON csu.curr_subject_id = tuc.ftc_subject_id and csu.curr_subject_status=1
       |INNER JOIN ${connection.schema}_stage.rel_user u ON u.user_id = tuc.ftc_user_id
       |ORDER BY ${pkNotation(stagingTableName)}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin

  override def pkNotation: Map[String, String] = Map("staging_tutor_user_context" -> "ftc_dw_id")

  def makeColumnNames(cols: List[String]): String = cols.map {
    case "ftc_user_dw_id" => "\tu.user_dw_id AS ftc_user_dw_id"
    case "ftc_tenant_dw_id" => "\tt.tenant_dw_id AS ftc_tenant_dw_id"
    case "ftc_grade_dw_id" => "\tg.grade_dw_id AS ftc_grade_dw_id"
    case "ftc_school_dw_id" => "\ts.school_dw_id AS ftc_school_dw_id"
    case "ftc_subject_dw_id" => "\tcsu.curr_subject_dw_id AS ftc_subject_dw_id"
    case col => s"\t$col"
  }.mkString(",\n")
}
