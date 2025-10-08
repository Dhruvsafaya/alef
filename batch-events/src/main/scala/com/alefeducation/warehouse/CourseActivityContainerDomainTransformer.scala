package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer


object CourseActivityContainerDomainTransformer extends CommonTransformer {

  val mainColumnNames: List[String] = List(
    "cacd_dw_id",
    "cacd_created_time",
    "cacd_dw_created_time",
    "cacd_updated_time",
    "cacd_dw_updated_time",
    "cacd_container_id",
    "cacd_container_dw_id",
    "cacd_course_id",
    "cacd_course_dw_id",
    "cacd_domain",
    "cacd_sequence",
    "cacd_status"
  )

  override def getSelectQuery(schema: String): String = s"""
                                                           |SELECT
                                                           |   staging.$getPkColumn
                                                           |${fromStatement(schema)}
                                                           |ORDER BY staging.$getPkColumn
                                                           |LIMIT $QUERY_LIMIT
                                                           |""".stripMargin

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idClause = ids.mkString(",")
    val insCols = mainColumnNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColumnNames(mainColumnNames)
    s"""
       |INSERT INTO $schema.dim_course_activity_container_domain ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |  AND staging.$getPkColumn IN ($idClause)
       |""".stripMargin
  }


  override def getPkColumn(): String = "cacd_dw_id"

  override def getStagingTableName(): String = "rel_course_activity_container_domain"

  private def fromStatement(schema: String): String =
    s"""FROM ${schema}_stage.$getStagingTableName staging
       | JOIN ${schema}_stage.rel_dw_id_mappings dwid ON staging.cacd_container_id = dwid.id AND dwid.entity_type = 'course_activity_container'
       | JOIN ${schema}_stage.rel_dw_id_mappings cdwid ON staging.cacd_course_id = cdwid.id AND cdwid.entity_type = 'course'
       |""".stripMargin

  private def makeColumnNames(cols: List[String]): String =
    cols
      .map {
        case "cacd_container_dw_id" => s"\tdwid.dw_id AS cacd_container_dw_id"
        case "cacd_course_dw_id" => s"\tcdwid.dw_id AS cacd_course_dw_id"
        case col => s"\t$col"
      }
      .mkString(",\n")
}