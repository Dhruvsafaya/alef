package com.alefeducation.warehouse.course

import com.alefeducation.warehouse.core.RedshiftTransformer
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}
import scalikejdbc.AutoSession

object CourseTransformer extends RedshiftTransformer {

  override def pkNotation = Map("rel_course" -> "rel_course_dw_id")
  val pkCol = List("rel_course_dw_id")

  val columnNames = List(
    "rel_course_dw_id",
    "course_dw_id",
    "course_id",
    "course_name",
    "course_code",
    "course_organization_dw_id",
    "course_status",
    "course_created_time",
    "course_dw_created_time",
    "course_updated_time",
    "course_dw_updated_time",
    "course_deleted_time",
    "course_dw_deleted_time",
    "course_lang_code",
    "course_type",
    "course_program_enabled",
    "course_resources_enabled",
    "course_placement_type"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val selectPkQuery = getSelectQuery(pkCol, connection)
    val selectQuery = getSelectQuery(columnNames, connection)
    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.dim_course (${columnNames.mkString(", ")})
         | (
         |  $selectQuery
         | )
         |""".stripMargin
    log.info(s"pkSelectQuery :  $selectPkQuery")
    log.info(s"insertQuery : $insertStatement")
    List(QueryMeta("rel_course", selectPkQuery, insertStatement))
  }

  private def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |SELECT
       |     ${makeColumnNames(cols)}
       |FROM ${connection.schema}_stage.rel_course p
       |  JOIN ${connection.schema}.dim_organization o on p.course_organization = o.organization_code and o.organization_status = 1
       |  JOIN ${connection.schema}_stage.rel_dw_id_mappings pdw on p.course_id = pdw.id and pdw.entity_type = 'course'
       |
       |ORDER BY ${pkCol.mkString(",")}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String =
    cols.map(col =>
      if (col == "course_organization_dw_id")
        "o.organization_dw_id as course_organization_dw_id"
      else if (col == "course_dw_id")
        "pdw.dw_id as course_dw_id"
      else col
    ).mkString(", ")

}
