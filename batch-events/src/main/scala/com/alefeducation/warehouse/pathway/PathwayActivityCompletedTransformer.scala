package com.alefeducation.warehouse.pathway

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object PathwayActivityCompletedTransformer extends CommonRedshiftTransformer {

  override def pkNotation = Map("staging_pathway_activity_completed" -> "fpac_dw_id")

  override val pkCol = List("fpac_dw_id")
  override val stagingTableName : String = "staging_pathway_activity_completed"
  override val mainTableName : String = "fact_pathway_activity_completed"

  val mainColumnNames = List(
    "fpac_created_time",
    "fpac_dw_created_time",
    "fpac_date_dw_id",
    "fpac_tenant_dw_id",
    "fpac_student_dw_id",
    "fpac_class_dw_id",
    "fpac_course_dw_id",
    "fpac_course_activity_container_dw_id",
    "fpac_activity_dw_id",
    "fpac_activity_type",
    "fpac_score",
    "fpac_time_spent",
    "fpac_attempt",
    "fpac_learning_session_id",
    "fpac_academic_year"
  )

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       |     ${makeColumnNames(cols)}
       |from ${connection.schema}_stage.staging_pathway_activity_completed pac
       |inner join ${connection.schema}.dim_tenant t on t.tenant_id = pac.fpac_tenant_id
       |inner join ${connection.schema}_stage.rel_user s on s.user_id = pac.fpac_student_id
       |inner join ${connection.schema}_stage.rel_dw_id_mappings c on c.id = pac.fpac_class_id and c.entity_type = 'class'
       |inner join ${connection.schema}_stage.rel_dw_id_mappings pc on pc.id = pac.fpac_pathway_id and pc.entity_type = 'course'
       |inner join ${connection.schema}_stage.rel_dw_id_mappings plc on plc.id = pac.fpac_level_id and plc.entity_type = 'course_activity_container'
       |left join ${connection.schema}.dim_learning_objective lesson on lesson.lo_id = pac.fpac_activity_id and pac.fpac_activity_type = 1
       |left join ${connection.schema}.dim_interim_checkpoint ic on ic.ic_id = pac.fpac_activity_id and pac.fpac_activity_type = 2
       |
       |where
       |(pac.fpac_activity_type = 1 and pac.fpac_activity_id is not null and lesson.lo_id is not null) or
       |(pac.fpac_activity_type = 2 and pac.fpac_activity_id is not null and ic.ic_id is not null)
       |
       |order by ${pkNotation("staging_pathway_activity_completed")}
       |limit $QUERY_LIMIT
       |""".stripMargin
  }

  def makeColumnNames(cols: List[String]): String = cols.map {
    case "fpac_tenant_dw_id" => "t.tenant_dw_id as fpac_tenant_dw_id"
    case "fpac_student_dw_id" => "s.user_dw_id as fpac_student_dw_id"
    case "fpac_class_dw_id" => "c.dw_id as fpac_class_dw_id"
    case "fpac_course_dw_id" => "pc.dw_id as fpac_course_dw_id"
    case "fpac_course_activity_container_dw_id" => "plc.dw_id as fpac_course_activity_container_dw_id"
    case "fpac_activity_dw_id" => "CASE WHEN pac.fpac_activity_type = 1 THEN lesson.lo_dw_id ELSE ic.ic_dw_id END AS fpac_activity_dw_id"
    case col => col
  }.mkString(", ")

}
