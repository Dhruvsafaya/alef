package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object CourseResourceActivityGradeAssociationTransformer extends CommonTransformer  {
  override def getPkColumn(): String = "craga_dw_id"

  override def getStagingTableName(): String = "rel_course_resource_activity_grade_association"

  private val commonColumns = List(
    "craga_dw_id",
    "craga_created_time",
    "craga_updated_time",
    "craga_deleted_time",
    "craga_dw_created_time",
    "craga_dw_updated_time",
    "craga_dw_deleted_time",
    "craga_status",
    "craga_course_id",
    "craga_activity_id",
    "craga_grade_id"
  )

  private val dwIds = List(
    "craga_activity_dw_id",
    "craga_course_dw_id"
  )

  private val uuids = List(
    "activity.act_dw_id AS craga_activity_dw_id",
    "course.dw_id AS craga_course_dw_id"
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
       |INSERT INTO $schema.dim_course_resource_activity_grade_association ($insertColumnsStr)
       |SELECT $selectColumnsStr
       |${fromStatement(schema)}
       |WHERE ${getPkColumn()} IN ($idCols)
       |""".stripMargin
  }

  private def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.rel_course_resource_activity_grade_association caga
       |JOIN ${schema}_stage.rel_dw_id_mappings course ON course.id = caga.craga_course_id AND course.entity_type = 'course'
       |LEFT JOIN (
       |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
       |    FROM $schema.dim_learning_objective
       |    UNION
       |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
       |    FROM $schema.dim_interim_checkpoint
       |) AS activity ON activity.act_id = caga.craga_activity_id
       |""".stripMargin

}
