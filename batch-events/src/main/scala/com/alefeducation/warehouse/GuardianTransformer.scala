package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object GuardianTransformer extends CommonRedshiftTransformer {

  override val mainTableName: String = "dim_guardian"

  override val stagingTableName: String = "rel_guardian"
  override val pkCol: List[String] = List("rel_guardian_dw_id")
  override def pkNotation: Map[String, String] = Map(stagingTableName -> "rel_guardian_dw_id")

  override val mainColumnNames: List[String] = List(
    "rel_guardian_dw_id",
    "guardian_created_time",
    "guardian_updated_time",
    "guardian_deleted_time",
    "guardian_dw_created_time",
    "guardian_dw_updated_time",
    "guardian_active_until",
    "guardian_status",
    "guardian_id",
    "guardian_dw_id",
    "guardian_student_dw_id",
    "guardian_invitation_status",
  )

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
     |SELECT
     |  ${makeColumnNames(cols)}
     |FROM ${connection.schema}_stage.$stagingTableName
     |  JOIN ${connection.schema}_stage.rel_user ON rel_user.user_id = rel_guardian.guardian_id
     |  LEFT JOIN ${connection.schema}_stage.rel_user alias_student ON alias_student.user_id = rel_guardian.student_id
     |WHERE (student_id IS NULL AND alias_student.user_dw_id IS NULL)
     |  OR (student_id IS NOT NULL AND alias_student.user_dw_id IS NOT NULL)
     |
     |ORDER BY ${pkNotation(stagingTableName)}
     |LIMIT $QUERY_LIMIT
     |""".stripMargin

  def makeColumnNames(cols: List[String]): String =
    cols
      .map {
        case "guardian_dw_id"         => "rel_user.user_dw_id AS guardian_dw_id"
        case "guardian_student_dw_id" => "alias_student.user_dw_id AS guardian_student_dw_id"
        case col                      => s"$col"
      }
      .mkString(",\n")
}
