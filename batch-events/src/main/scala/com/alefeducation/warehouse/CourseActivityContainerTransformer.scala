package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer


object CourseActivityContainerTransformer extends CommonTransformer {

  val columnNames = List(
    "course_activity_container_is_accelerated",
    "course_activity_container_pacing",
    "course_activity_container_sequence",
    "course_activity_container_course_version",
    "course_activity_container_attach_status",
    "course_activity_container_id",
    "course_activity_container_dw_id",
    "course_activity_container_longname",
    "course_activity_container_updated_time",
    "course_activity_container_dw_created_time",
    "course_activity_container_title",
    "course_activity_container_created_time",
    "course_activity_container_course_id",
    "course_activity_container_domain",
    "course_activity_container_status",
    "course_activity_container_dw_updated_time",
    "course_activity_container_index",
    "rel_course_activity_container_dw_id"
  )

  private def makeColumnNames(cols: List[String]): String =
    cols.map(col => if (col == "course_activity_container_dw_id") s"dw_id as course_activity_container_dw_id" else col).mkString(",\n\t")

  override def getSelectQuery(schema: String): String =
    s"""
       |SELECT
       |   pl.$getPkColumn
       |${fromStatement(schema)}
       |ORDER BY pl.$getPkColumn
       |LIMIT $QUERY_LIMIT
       |""".stripMargin

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idClause = ids.mkString(",")
    val insCols = columnNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColumnNames(columnNames)
    s"""
       |INSERT INTO $schema.dim_course_activity_container ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |  AND pl.$getPkColumn IN ($idClause)
       |""".stripMargin
  }

  override def getPkColumn(): String = "rel_course_activity_container_dw_id"

  override def getStagingTableName(): String = "rel_course_activity_container"

  private def fromStatement(schema: String): String =
    s"""
      |FROM ${schema}_stage.${getStagingTableName()} pl
      | JOIN ${schema}_stage.rel_dw_id_mappings dwid on pl.course_activity_container_id = dwid.id
      |WHERE dwid.entity_type = 'course_activity_container'
      |""".stripMargin
}
