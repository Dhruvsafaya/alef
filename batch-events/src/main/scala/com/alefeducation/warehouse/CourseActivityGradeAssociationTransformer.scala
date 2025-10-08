package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object CourseActivityGradeAssociationTransformer extends CommonTransformer {
  override def getPkColumn(): String = "caga_dw_id"

  override def getStagingTableName(): String = "rel_course_activity_grade_association"

  private val commonColumns = List(
    "caga_dw_id",
    "caga_created_time",
    "caga_updated_time",
    "caga_deleted_time",
    "caga_dw_created_time",
    "caga_dw_updated_time",
    "caga_dw_deleted_time",
    "caga_status",
    "caga_course_id",
    "caga_activity_id",
    "caga_grade_id"
  )

  private val dwIds = List(
    "caga_activity_dw_id",
    "caga_course_dw_id"
  )

  private val uuids = List(
    "activity.act_dw_id AS caga_activity_dw_id",
    "course.dw_id AS caga_course_dw_id"
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
       |INSERT INTO $schema.dim_course_activity_grade_association ($insertColumnsStr)
       |SELECT $selectColumnsStr
       |${fromStatement(schema)}
       |WHERE ${getPkColumn()} IN ($idCols)
       |""".stripMargin
  }

  private def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.rel_course_activity_grade_association caga
       |JOIN ${schema}_stage.rel_dw_id_mappings course ON course.id = caga.caga_course_id AND course.entity_type = 'course'
       |JOIN (
       |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
       |    FROM $schema.dim_learning_objective
       |    UNION
       |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
       |    FROM $schema.dim_interim_checkpoint
       |) AS activity ON activity.act_id = caga.caga_activity_id
       |""".stripMargin

}
