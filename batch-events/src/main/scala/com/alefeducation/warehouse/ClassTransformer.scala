package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object ClassTransformer extends RedshiftTransformer {
  override def tableNotation: Map[String, String] = Map.empty

  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation: Map[String, String] = Map(
    "rel_class" -> "rel_class_dw_id"
  )

  val pkCol = List(
    "rel_class_dw_id"
  )

  val dimColumnNames = List(
    "rel_class_dw_id",
    "class_dw_id",
    "class_created_time",
    "class_updated_time",
    "class_deleted_time",
    "class_dw_created_time",
    "class_dw_updated_time",
    "class_status",
    "class_id",
    "class_title",
    "class_school_id",
    "class_grade_id",
    "class_section_id",
    "class_academic_year_id",
    "class_academic_calendar_id",
    "class_gen_subject",
    "class_curriculum_id",
    "class_curriculum_grade_id",
    "class_curriculum_subject_id",
    "class_content_academic_year",
    "class_tutor_dhabi_enabled",
    "class_language_direction",
    "class_online",
    "class_practice",
    "class_course_status",
    "class_source_id",
    "class_curriculum_instructional_plan_id",
    "class_category_id",
    "class_active_until",
    "class_material_id",
    "class_material_type"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val selectPkQuery = getSelectQuery(pkCol, connection)
    val selectQuery = getSelectQuery(dimColumnNames, connection)
    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.dim_class (${dimColumnNames.mkString(", ")})
         | (
         |  $selectQuery
         | )
         |""".stripMargin
    log.info(s"pkSelectQuery :  $selectPkQuery")
    log.info(s"insertQuery : $insertStatement")
    List(QueryMeta("rel_class", selectPkQuery, insertStatement))
  }

  private def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       |     ${makeColumnNames(cols)}
       |from ${connection.schema}_stage.rel_class rc
       |inner join ${connection.schema}_stage.rel_dw_id_mappings rdim on rc.class_id=rdim.id
       |where rdim.entity_type = 'class'
       |order by ${pkCol.mkString(",")}
       |      limit $QUERY_LIMIT
       |""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String = cols.map(col => if(col =="class_dw_id") s"dw_id as class_dw_id" else col).mkString(", ")

}
