package com.alefeducation.warehouse.course

import com.alefeducation.warehouse.core.RedshiftTransformer
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}
import scalikejdbc.AutoSession

object CourseGradeTransformer extends RedshiftTransformer{
  override def pkNotation = Map("rel_course_grade_association" -> "cg_dw_id")
  val pkCol = List("cg_dw_id")

  val columnNames = List(
    "cg_dw_id",
    "cg_course_dw_id",
    "cg_course_id",
    "cg_grade_id",
    "cg_grade_dw_id",
    "cg_status",
    "cg_created_time",
    "cg_dw_created_time",
    "cg_updated_time",
    "cg_dw_updated_time",
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val selectPkQuery = getSelectQuery(pkCol, connection)
    val selectQuery = getSelectQuery(columnNames, connection)
    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.dim_course_grade_association (${columnNames.mkString(", ")})
         | (
         |  $selectQuery
         | )
         |""".stripMargin
    log.info(s"pkSelectQuery :  $selectPkQuery")
    log.info(s"insertQuery : $insertStatement")
    List(QueryMeta("rel_course_grade_association", selectPkQuery, insertStatement))
  }

  private def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |SELECT
       |     ${makeColumnNames(cols)}
       |FROM ${connection.schema}_stage.rel_course_grade_association p
       |  JOIN ${connection.schema}_stage.rel_dw_id_mappings pdw on p.cg_course_id = pdw.id and pdw.entity_type = 'course'
       |  LEFT JOIN ${connection.schema}.dim_curriculum_grade g on p.cg_grade_id = g.curr_grade_id
       |ORDER BY ${pkCol.mkString(",")}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String =
    cols.map(col =>
      if (col == "cg_course_dw_id")
        "pdw.dw_id as cg_course_dw_id"
      else if (col == "cg_grade_dw_id")
        "g.curr_grade_dw_id as cg_grade_dw_id"
      else col
    ).mkString(", ")

}
