package com.alefeducation.warehouse.course

import com.alefeducation.warehouse.core.RedshiftTransformer
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}
import scalikejdbc.AutoSession


object CourseContentRepositoryTransformer extends RedshiftTransformer {

  override def pkNotation = Map("rel_course_content_repository_association" -> "ccr_dw_id")
  val pkCol = List("ccr_dw_id")

  val columnNames = List(
    "ccr_dw_id",
    "ccr_course_dw_id",
    "ccr_course_id",
    "ccr_repository_dw_id",
    "ccr_repository_id",
    "ccr_status",
    "ccr_created_time",
    "ccr_dw_created_time",
    "ccr_updated_time",
    "ccr_dw_updated_time",
    "ccr_deleted_time",
    "ccr_course_type",
    "ccr_attach_status"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val selectPkQuery = getSelectQuery(pkCol, connection)
    val selectQuery = getSelectQuery(columnNames, connection)
    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.dim_course_content_repository_association (${columnNames.mkString(", ")})
         | (
         |  $selectQuery
         | )
         |""".stripMargin
    log.info(s"pkSelectQuery :  $selectPkQuery")
    log.info(s"insertQuery : $insertStatement")
    List(QueryMeta("rel_course_content_repository_association", selectPkQuery, insertStatement))
  }

  private def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |SELECT
       |     ${makeColumnNames(cols)}
       |FROM ${connection.schema}_stage.rel_course_content_repository_association p
       |  LEFT JOIN ${connection.schema}_stage.rel_dw_id_mappings pdw on p.ccr_course_id = pdw.id and pdw.entity_type = 'course'
       |  LEFT JOIN ${connection.schema}.dim_content_repository dcr on p.ccr_repository_id = dcr.content_repository_id
       |WHERE (p.ccr_repository_id is null) or (dcr.content_repository_id is not null)
       |
       |ORDER BY ${pkCol.mkString(",")}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String =
    cols.map(col =>
      if (col == "ccr_course_dw_id")
        "pdw.dw_id as ccr_course_dw_id"
      else if (col == "ccr_repository_dw_id")
        "dcr.content_repository_dw_id as ccr_repository_dw_id"
      else col
    ).mkString(", ")

}
