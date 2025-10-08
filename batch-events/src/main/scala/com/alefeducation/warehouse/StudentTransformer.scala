package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object StudentTransformer extends CommonRedshiftTransformer {

  override def pkNotation = Map("rel_student" -> "rel_student_id")

  override val pkCol = List("rel_student_id")
  override val stagingTableName: String = "rel_student"
  override val mainTableName: String = "dim_student"

  val mainColumnNames = List(
    "student_created_time",
    "student_updated_time",
    "student_deleted_time",
    "student_dw_created_time",
    "student_dw_updated_time",
    "student_active_until",
    "student_status",
    "student_id",
    "student_username",
    "student_dw_id",
    "student_school_dw_id",
    "student_grade_dw_id",
    "student_section_dw_id",
    "student_tags",
    "student_special_needs"
  )



  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""select
       |${makeColumnNames(cols)}
       |from ${connection.schema}_stage.rel_student rs
       |inner join ${connection.schema}_stage.rel_user u on u.user_id = rs.student_uuid
       |inner join ${connection.schema}.dim_school s on s.school_id = rs.school_uuid
       |inner join ${connection.schema}.dim_grade g on g.grade_id = rs.grade_uuid
       |inner join ${connection.schema}.dim_section sec on sec.section_id = rs.section_uuid
       |
       |order by ${pkNotation("rel_student")}
       |limit $QUERY_LIMIT""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String = cols.map {
    case "student_dw_id" => "\tu.user_dw_id as student_dw_id"
    case "student_school_dw_id" => "\ts.school_dw_id as student_school_dw_id"
    case "student_grade_dw_id" => "\tg.grade_dw_id as student_grade_dw_id"
    case "student_section_dw_id" => "\tsec.section_dw_id as student_section_dw_id"
    case "student_id" => "\trs.student_uuid as student_id"
    case col => s"\t$col"
  }.mkString(",\n")

}
