package com.alefeducation.warehouse

import com.alefeducation.warehouse.AssignmentTransformer.{insCols, selCols}
import com.alefeducation.warehouse.core.CommonTransformer

object AssignmentInstanceStudentTransformer extends CommonTransformer {

  val dimTableColumns = List(
    "ais_created_time",
    "ais_updated_time",
    "ais_deleted_time",
    "ais_dw_created_time",
    "ais_dw_updated_time",
    "ais_status"
  )

  val dw_ids = List(
    "ais_instance_dw_id",
    "ais_student_dw_id"
  )

  val uuids = List(
    "ins.assignment_instance_dw_id as ais_instance_dw_id",
    "std.user_dw_id as ais_student_dw_id"
  )

  val insCols = dimTableColumns ++ dw_ids
  val selCols = dimTableColumns ++ uuids

  override def getSelectQuery(schema: String): String =
    s"""
      |SELECT
      |     a.${getPkColumn()}
      |${fromStatement(schema)}
      |ORDER BY a.${getPkColumn()}
      |      LIMIT $QUERY_LIMIT
      |""".stripMargin

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idCols = ids.mkString(",")
    val insColsStr = insCols.mkString("\n\t", ",\n\t", "\n")
    val selColsStr = selCols.mkString("\n\t", ",\n\t", "\n")
    s"""
      |INSERT INTO $schema.dim_assignment_instance_student ($insColsStr)
      | SELECT $selColsStr ${fromStatement(schema)}
      | AND a.${getPkColumn()} IN ($idCols)
      |""".stripMargin
  }

  override def getPkColumn(): String = "rel_ais_id"

  override def getStagingTableName(): String = "rel_assignment_instance_student"

  private def fromStatement(schema: String): String =
    s"""
      |FROM ${schema}_stage.rel_assignment_instance_student a
      |INNER JOIN $schema.dim_assignment_instance ins on ins.assignment_instance_id=a.ais_instance_id
      |INNER JOIN ${schema}_stage.rel_user std on std.user_id=a.ais_student_id
      |""".stripMargin
}
