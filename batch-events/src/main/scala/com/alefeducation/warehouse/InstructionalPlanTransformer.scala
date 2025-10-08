package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object InstructionalPlanTransformer extends CommonRedshiftTransformer {

  override val mainTableName: String = "dim_instructional_plan"

  override val stagingTableName: String = "rel_instructional_plan"
  override val pkCol: List[String] = List("rel_instructional_plan_id")
  override def pkNotation: Map[String, String] = Map(stagingTableName -> "rel_instructional_plan_id")

  override val mainColumnNames: List[String] = List(
    "instructional_plan_created_time",
    "instructional_plan_updated_time",
    "instructional_plan_deleted_time",
    "instructional_plan_dw_created_time",
    "instructional_plan_dw_updated_time",
    "instructional_plan_status",
    "instructional_plan_id",
    "instructional_plan_name",
    "instructional_plan_curriculum_id",
    "instructional_plan_curriculum_subject_id",
    "instructional_plan_curriculum_grade_id",
    "instructional_plan_content_academic_year_id",
    "instructional_plan_item_order",
    "instructional_plan_item_week_dw_id",
    "instructional_plan_item_lo_dw_id",
    "instructional_plan_item_ccl_lo_id",
    "instructional_plan_item_optional",
    "instructional_plan_item_instructor_led",
    "instructional_plan_item_default_locked",
    "instructional_plan_item_type",
    "instructional_plan_item_ic_dw_id",
    "instructional_plan_content_repository_id",
    "instructional_plan_content_repository_dw_id"
  )

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
     |SELECT
     |  ${makeColumnNames(cols)}
     |FROM ${connection.schema}_stage.$stagingTableName ip
     |  LEFT JOIN ${connection.schema}.dim_learning_objective lo ON lo.lo_id = ip.lo_uuid
     |  INNER JOIN ${connection.schema}.dim_week w ON w.week_id = ip.week_uuid
     |  LEFT JOIN ${connection.schema}.dim_interim_checkpoint ic ON ic.ic_id = ip.ic_uuid
     |  INNER JOIN ${connection.schema}.dim_content_repository cr ON cr.content_repository_id = ip.content_repository_uuid
     |WHERE ((ic_uuid IS NULL AND ic_dw_id IS NULL) OR (ic_uuid IS NOT NULL AND ic_dw_id IS NOT NULL))
     |  AND ((lo_uuid IS NULL AND lo_dw_id IS NULL) OR (lo_uuid IS NOT NULL AND lo_dw_id IS NOT NULL))
     |
     |ORDER BY ${pkNotation(stagingTableName)}
     |LIMIT $QUERY_LIMIT
     |""".stripMargin

  def makeColumnNames(cols: List[String]): String =
    cols
      .map {
        case "instructional_plan_item_week_dw_id"          => "w.week_dw_id AS instructional_plan_item_week_dw_id"
        case "instructional_plan_item_lo_dw_id"            => "lo.lo_dw_id AS instructional_plan_item_lo_dw_id"
        case "instructional_plan_item_ic_dw_id"            => "ic.ic_dw_id AS instructional_plan_item_ic_dw_id"
        case "instructional_plan_content_repository_dw_id" => "cr.content_repository_dw_id AS instructional_plan_content_repository_dw_id"
        case col                                           => s"$col"
      }
      .mkString(",\n")
}
