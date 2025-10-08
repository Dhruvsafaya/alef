package com.alefeducation.warehouse.course

import com.alefeducation.warehouse.core.RedshiftTransformer
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}
import scalikejdbc.AutoSession

object CourseCurriculumTransformer extends RedshiftTransformer {

  override def pkNotation = Map("rel_course_curriculum_association" -> "cc_dw_id")
  val pkCol = List("cc_dw_id")

  val columnNames = List(
    "cc_dw_id",
    "cc_course_dw_id",
    "cc_course_id",
    "cc_curr_id",
    "cc_curr_grade_id",
    "cc_curr_subject_id",
    "cc_status",
    "cc_created_time",
    "cc_deleted_time",
    "cc_updated_time",
    "cc_dw_created_time",
    "cc_dw_updated_time"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val selectPkQuery = getSelectQuery(pkCol, connection)
    val selectQuery = getSelectQuery(columnNames, connection)
    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.dim_course_curriculum_association (${columnNames.mkString(", ")})
         | (
         |  $selectQuery
         | )
         |""".stripMargin
    log.info(s"pkSelectQuery :  $selectPkQuery")
    log.info(s"insertQuery : $insertStatement")
    List(QueryMeta("rel_course_curriculum_association", selectPkQuery, insertStatement))
  }

  private def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |SELECT
       |     ${makeColumnNames(cols)}
       |FROM ${connection.schema}_stage.rel_course_curriculum_association p
       |  JOIN ${connection.schema}_stage.rel_dw_id_mappings pdw on p.cc_course_id = pdw.id and pdw.entity_type = 'course'
       |
       |ORDER BY ${pkCol.mkString(",")}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String =
    cols.map(col =>
      if (col == "cc_course_dw_id")
        "pdw.dw_id as cc_course_dw_id"
      else col
    ).mkString(", ")

}
