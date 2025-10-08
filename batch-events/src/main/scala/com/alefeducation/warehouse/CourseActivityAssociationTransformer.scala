package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object CourseActivityAssociationTransformer extends CommonTransformer {
  override def getPkColumn(): String = "caa_dw_id"

  override def getStagingTableName(): String = "rel_course_activity_association"

  private val commonColumns = List(
    "caa_dw_id",
    "caa_created_time",
    "caa_updated_time",
    "caa_deleted_time",
    "caa_dw_created_time",
    "caa_dw_updated_time",
    "caa_dw_deleted_time",
    "caa_status",
    "caa_attach_status",
    "caa_course_id",
    "caa_container_id",
    "caa_activity_id",
    "caa_activity_type",
    "caa_activity_pacing",
    "caa_activity_index",
    "caa_course_version",
    "caa_is_parent_deleted",
    "caa_grade",
    "caa_activity_is_optional",
    "caa_is_joint_parent_activity",
  )

  private val dwIds = List(
    "caa_course_dw_id",
    "caa_container_dw_id",
    "caa_activity_dw_id",
  )

  private val uuids = List(
    "course.dw_id AS caa_course_dw_id",
    "container.dw_id AS caa_container_dw_id",
    "activity.act_dw_id AS caa_activity_dw_id",
  )

  private val insertColumns = commonColumns ++ dwIds
  private val selectColumns = commonColumns ++ uuids

  override def getSelectQuery(schema: String): String =
    s"""
       |SELECT
       |     ${getPkColumn()}
       |${fromStatement(schema)}
       |WHERE $whereCondition
       |ORDER BY ${getPkColumn()}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idCols = ids.mkString(",")
    val insertColumnsStr = insertColumns.mkString("\n\t", ",\n\t", "\n")
    val selectColumnsStr = selectColumns.mkString("\n\t", ",\n\t", "\n")
    s"""
       |INSERT INTO $schema.dim_course_activity_association ($insertColumnsStr)
       |SELECT $selectColumnsStr
       |${fromStatement(schema)}
       |WHERE $whereCondition AND ${getPkColumn()} IN ($idCols)
       |""".stripMargin
  }

  private def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.rel_course_activity_association caa
       |JOIN ${schema}_stage.rel_dw_id_mappings course ON course.id = caa.caa_course_id AND course.entity_type = 'course'
       |LEFT JOIN ${schema}_stage.rel_dw_id_mappings container ON container.id = caa.caa_container_id AND container.entity_type = 'course_activity_container'
       |JOIN (
       |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
       |    FROM $schema.dim_learning_objective
       |    UNION
       |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
       |    FROM $schema.dim_interim_checkpoint
       |) AS activity ON activity.act_id = caa.caa_activity_id
       |""".stripMargin

  private def whereCondition: String =
    s"""
       |(
       |  caa.caa_container_id IS NULL AND container.id IS NULL
       |  OR caa.caa_container_id IS NOT NULL AND container.id IS NOT NULL
       |)
       |""".stripMargin

}
