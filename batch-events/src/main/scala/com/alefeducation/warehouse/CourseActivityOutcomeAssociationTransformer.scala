package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object CourseActivityOutcomeAssociationTransformer extends CommonTransformer {
  override def getPkColumn(): String = "caoa_dw_id"

  override def getStagingTableName(): String = "rel_course_activity_outcome_association"

  private val commonColumns = List(
    "caoa_dw_id",
    "caoa_created_time",
    "caoa_updated_time",
    "caoa_dw_created_time",
    "caoa_dw_updated_time",
    "caoa_status",
    "caoa_course_id",
    "caoa_activity_id",
    "caoa_outcome_id",
    "caoa_outcome_type",
    "caoa_curr_id",
    "caoa_curr_grade_id",
    "caoa_curr_subject_id",
  )

  private val dwIds = List(
    "caoa_course_dw_id",
    "caoa_activity_dw_id",
  )

  private val uuids = List(
    "course.dw_id AS caoa_course_dw_id",
    "activity.act_dw_id AS caoa_activity_dw_id",
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
       |INSERT INTO $schema.dim_course_activity_outcome_association ($insertColumnsStr)
       |SELECT $selectColumnsStr
       |${fromStatement(schema)}
       |WHERE ${getPkColumn()} IN ($idCols)
       |""".stripMargin
  }

  private def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.rel_course_activity_outcome_association caoa
       |JOIN ${schema}_stage.rel_dw_id_mappings course ON course.id = caoa.caoa_course_id AND course.entity_type = 'course'
       |JOIN (
       |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
       |    FROM $schema.dim_learning_objective
       |    UNION
       |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
       |    FROM $schema.dim_interim_checkpoint
       |) AS activity ON activity.act_id = caoa.caoa_activity_id
       |""".stripMargin

}
