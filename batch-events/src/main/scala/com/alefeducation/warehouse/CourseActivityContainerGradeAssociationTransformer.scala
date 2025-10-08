package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object CourseActivityContainerGradeAssociationTransformer extends CommonTransformer {

  val mainColumnNames: List[String] = List(
    "cacga_dw_id",
    "cacga_created_time",
    "cacga_dw_created_time",
    "cacga_updated_time",
    "cacga_dw_updated_time",
    "cacga_container_id",
    "cacga_container_dw_id",
    "cacga_course_id",
    "cacga_course_dw_id",
    "cacga_grade",
    "cacga_status"
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
       |INSERT INTO $schema.dim_course_activity_container_grade_association ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |  AND staging.$getPkColumn IN ($idClause)
       |""".stripMargin
  }


  override def getPkColumn(): String = "cacga_dw_id"

  override def getStagingTableName(): String = "rel_course_activity_container_grade_association"

  private def fromStatement(schema: String): String =
    s"""FROM ${schema}_stage.$getStagingTableName staging
        | JOIN ${schema}_stage.rel_dw_id_mappings dwid ON staging.cacga_container_id = dwid.id AND
        |   dwid.entity_type = 'course_activity_container'
        | JOIN ${schema}_stage.rel_dw_id_mappings cdwid ON staging.cacga_course_id = cdwid.id AND
        |   cdwid.entity_type = 'course'
        |""".stripMargin

  private def makeColumnNames(cols: List[String]): String =
    cols
      .map {
        case "cacga_container_dw_id" => s"\tdwid.dw_id AS cacga_container_dw_id"
        case "cacga_course_dw_id" => s"\tcdwid.dw_id AS cacga_course_dw_id"
        case col => s"\t$col"
      }
      .mkString(",\n")
}
