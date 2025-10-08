package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object CourseResourceActivityOutcomeAssociationTransformer extends CommonTransformer {
  override def getPkColumn(): String = "craoa_dw_id"

  override def getStagingTableName(): String = "rel_course_resource_activity_outcome_association"

  private val commonColumns = List(
    "craoa_dw_id",
    "craoa_created_time",
    "craoa_updated_time",
    "craoa_dw_created_time",
    "craoa_dw_updated_time",
    "craoa_status",
    "craoa_course_id",
    "craoa_activity_id",
    "craoa_outcome_id",
    "craoa_outcome_type",
    "craoa_curr_id",
    "craoa_curr_grade_id",
    "craoa_curr_subject_id",
  )

  private val dwIds = List(
    "craoa_course_dw_id",
    "craoa_activity_dw_id",
  )

  private val uuids = List(
    "course.dw_id AS craoa_course_dw_id",
    "activity.act_dw_id AS craoa_activity_dw_id",
  )

  private val insertColumns = commonColumns ++ dwIds
  private val selectColumns = commonColumns ++ uuids

  override def getSelectQuery(schema: String): String =
    s"""
       |SELECT
       |     ${getPkColumn()}
       |${fromStatement(schema)}
       |ORDER BY ${getPkColumn()}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idCols = ids.mkString(",")
    val insertColumnsStr = insertColumns.mkString("\n\t", ",\n\t", "\n")
    val selectColumnsStr = selectColumns.mkString("\n\t", ",\n\t", "\n")
    s"""
       |INSERT INTO $schema.dim_course_resource_activity_outcome_association ($insertColumnsStr)
       |SELECT $selectColumnsStr
       |${fromStatement(schema)}
       |WHERE ${getPkColumn()} IN ($idCols)
       |""".stripMargin
  }

  private def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.rel_course_resource_activity_outcome_association caoa
       |JOIN ${schema}_stage.rel_dw_id_mappings course ON course.id = caoa.craoa_course_id AND course.entity_type = 'course'
       |JOIN (
       |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
       |    FROM $schema.dim_learning_objective
       |    UNION
       |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
       |    FROM $schema.dim_interim_checkpoint
       |) AS activity ON activity.act_id = caoa.craoa_activity_id
       |""".stripMargin
}
