package com.alefeducation.warehouse.pathway

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object LevelsRecommendedTransformer extends CommonRedshiftTransformer {

  override def pkNotation = Map("staging_levels_recommended" -> "flr_dw_id")

  override val pkCol = List("flr_dw_id")
  override val stagingTableName : String = "staging_levels_recommended"
  override val mainTableName : String = "fact_levels_recommended"

  val mainColumnNames = List(
    "flr_created_time",
    "flr_dw_created_time",
    "flr_date_dw_id",
    "flr_recommended_on",
    "flr_tenant_dw_id",
    "flr_student_dw_id",
    "flr_class_dw_id",
    "flr_course_dw_id",
    "flr_completed_course_activity_container_dw_id",
    "flr_course_activity_container_dw_id",
    "flr_status",
    "flr_recommendation_type",
    "flr_academic_year"
  )

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       |     ${makeColumnNames(cols)}
       |from ${connection.schema}_stage.staging_levels_recommended slr
       |inner join ${connection.schema}.dim_tenant t on t.tenant_id = slr.flr_tenant_id
       |inner join ${connection.schema}_stage.rel_user s on s.user_id = slr.flr_student_id
       |inner join ${connection.schema}_stage.rel_dw_id_mappings c on c.id = slr.flr_class_id and c.entity_type = 'class'
       |inner join ${connection.schema}_stage.rel_dw_id_mappings pc on pc.id = slr.flr_pathway_id and pc.entity_type = 'course'
       |inner join ${connection.schema}_stage.rel_dw_id_mappings plc on plc.id = slr.flr_level_id and plc.entity_type = 'course_activity_container'
       |left join ${connection.schema}_stage.rel_dw_id_mappings plc1 on plc1.id = slr.flr_completed_level_id and plc1.entity_type = 'course_activity_container'
       |order by ${pkNotation("staging_levels_recommended")}
       |limit $QUERY_LIMIT
       |""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String = cols.map {
    case "flr_tenant_dw_id" => "t.tenant_dw_id as flr_tenant_dw_id"
    case "flr_student_dw_id" => "s.user_dw_id as flr_student_dw_id"
    case "flr_class_dw_id" => "c.dw_id as flr_class_dw_id"
    case "flr_course_dw_id" => "pc.dw_id as flr_course_dw_id"
    case "flr_course_activity_container_dw_id" => "plc.dw_id as flr_course_activity_container_dw_id"
    case "flr_completed_course_activity_container_dw_id" => "plc1.dw_id as flr_completed_course_activity_container_dw_id"
    case col => col
  }.mkString(", ")

}
