package com.alefeducation.warehouse.pathway

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object LevelCompletedTransformer extends CommonRedshiftTransformer {

  override def pkNotation = Map("staging_level_completed" -> "flc_dw_id")

  override val pkCol = List("flc_dw_id")
  override val stagingTableName : String = "staging_level_completed"
  override val mainTableName : String = "fact_level_completed"

  override val mainColumnNames = List(
      "flc_created_time",
      "flc_dw_created_time",
      "flc_date_dw_id",
      "flc_completed_on",
      "flc_tenant_dw_id",
      "flc_student_dw_id",
      "flc_class_dw_id",
      "flc_course_dw_id",
      "flc_course_activity_container_dw_id",
      "flc_total_stars",
      "flc_score",
      "flc_academic_year"
  )

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       |     ${makeColumnNames(cols)}
       |from ${connection.schema}_stage.staging_level_completed lc
       |inner join ${connection.schema}.dim_tenant t on t.tenant_id = lc.flc_tenant_id
       |inner join ${connection.schema}_stage.rel_user s on s.user_id = lc.flc_student_id
       |inner join ${connection.schema}_stage.rel_dw_id_mappings c on c.id = lc.flc_class_id and c.entity_type = 'class'
       |inner join ${connection.schema}_stage.rel_dw_id_mappings pc on pc.id = lc.flc_pathway_id and pc.entity_type = 'course'
       |inner join ${connection.schema}_stage.rel_dw_id_mappings plc on plc.id = lc.flc_level_id and plc.entity_type = 'course_activity_container'
       |
       |order by ${pkNotation("staging_level_completed")}
       |limit $QUERY_LIMIT
       |""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String = cols.map {
    case "flc_tenant_dw_id" => "t.tenant_dw_id as flc_tenant_dw_id"
    case "flc_student_dw_id" => "s.user_dw_id as flc_student_dw_id"
    case "flc_class_dw_id" => "c.dw_id as flc_class_dw_id"
    case "flc_course_dw_id" => "pc.dw_id as flc_course_dw_id"
    case "flc_course_activity_container_dw_id" => "plc.dw_id as flc_course_activity_container_dw_id"
    case col => col
  }.mkString(", ")

}
