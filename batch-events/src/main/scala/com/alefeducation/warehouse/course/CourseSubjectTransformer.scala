package com.alefeducation.warehouse.course

import com.alefeducation.warehouse.core.RedshiftTransformer
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}
import scalikejdbc.AutoSession

object CourseSubjectTransformer extends RedshiftTransformer{
  override def pkNotation = Map("rel_course_subject_association" -> "cs_dw_id")
  val pkCol = List("cs_dw_id")

  val columnNames = List(
    "cs_dw_id",
    "cs_course_dw_id",
    "cs_course_id",
    "cs_subject_id",
    "cs_subject_dw_id",
    "cs_status",
    "cs_created_time",
    "cs_dw_created_time",
    "cs_updated_time",
    "cs_dw_updated_time",
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val selectPkQuery = getSelectQuery(pkCol, connection)
    val selectQuery = getSelectQuery(columnNames, connection)
    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.dim_course_subject_association (${columnNames.mkString(", ")})
         | (
         |  $selectQuery
         | )
         |""".stripMargin
    log.info(s"pkSelectQuery :  $selectPkQuery")
    log.info(s"insertQuery : $insertStatement")
    List(QueryMeta("rel_course_subject_association", selectPkQuery, insertStatement))
  }

  private def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |SELECT
       |     ${makeColumnNames(cols)}
       |FROM ${connection.schema}_stage.rel_course_subject_association p
       |  JOIN ${connection.schema}_stage.rel_dw_id_mappings pdw on p.cs_course_id = pdw.id and pdw.entity_type = 'course'
       |  LEFT JOIN ${connection.schema}.dim_curriculum_subject cs on p.cs_subject_id = cs.curr_subject_id
       |ORDER BY ${pkCol.mkString(",")}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String =
    cols.map(col =>
      if (col == "cs_course_dw_id")
        "pdw.dw_id as cs_course_dw_id"
      else if (col == "cs_subject_dw_id")
        "cs.curr_subject_dw_id as cs_subject_dw_id"
      else col
    ).mkString(", ")

}
