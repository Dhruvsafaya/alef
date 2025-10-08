package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object TeacherTransformer extends CommonRedshiftTransformer {

  override val mainColumnNames: List[String] = List(
    "teacher_created_time",
    "teacher_updated_time",
    "teacher_deleted_time",
    "teacher_dw_created_time",
    "teacher_dw_updated_time",
    "teacher_active_until",
    "teacher_status",
    "teacher_id",
    "teacher_dw_id",
    "teacher_subject_dw_id",
    "teacher_school_dw_id"
  )
  override val stagingTableName: String = "rel_teacher"
  override val mainTableName: String = "dim_teacher"
  override def pkNotation: Map[String, String] = Map("rel_teacher" -> "rel_teacher_id")
  val pkCol = List("rel_teacher_id")

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       |     ${makeColumnNames(cols)}
       |from ${connection.schema}_stage.rel_teacher rt
       |inner join ${connection.schema}_stage.rel_user u on rt.teacher_id = u.user_id and u.user_type = 'TEACHER'
       |inner join ${connection.schema}.dim_school sc on rt.school_id = sc.school_id
       |left join ${connection.schema}.dim_subject su on rt.subject_id = su.subject_id
       |order by ${pkCol.mkString(",")}
       |      limit $QUERY_LIMIT
       |""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String = cols.map {
    case "teacher_dw_id" => "\tu.user_dw_id AS teacher_dw_id"
    case "teacher_subject_dw_id" => "\tsu.subject_dw_id AS teacher_subject_dw_id"
    case "teacher_school_dw_id" => "\tsc.school_dw_id AS teacher_school_dw_id"
    case col => s"\t$col"
  }.mkString(",\n")

}
