package com.alefeducation.warehouse.pathway

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object PathwayPlacementTransformer extends CommonRedshiftTransformer {
  override def pkNotation: Map[String, String] = Map("staging_pathway_placement" -> "fpp_staging_id")

  val pkCol = List(
    "fpp_staging_id"
  )

  override val stagingTableName: String = "staging_pathway_placement"
  override val mainTableName: String = "fact_pathway_placement"

  override val mainColumnNames = List(
    "fpp_created_time",
    "fpp_dw_created_time",
    "fpp_date_dw_id",
    "fpp_pathway_id",
    "fpp_course_dw_id",
    "fpp_new_pathway_domain",
    "fpp_new_pathway_grade",
    "fpp_class_id",
    "fpp_class_dw_id",
    "fpp_student_id",
    "fpp_student_dw_id",
    "fpp_tenant_id",
    "fpp_tenant_dw_id",
    "fpp_placement_type",
    "fpp_overall_grade",
    "fpp_created_by",
    "fpp_created_by_dw_id",
    "fpp_is_initial",
    "fpp_has_accelerated_domains"
  )

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       |     ${makeColumnNames(cols)}
       |from ${connection.schema}_stage.staging_pathway_placement
       |left join ${connection.schema}_stage.rel_user user1 on user1.user_id = staging_pathway_placement.fpp_created_by and user1.user_type = 'TEACHER'
       |inner join ${connection.schema}_stage.rel_dw_id_mappings pc on pc.id = staging_pathway_placement.fpp_pathway_id and pc.entity_type = 'course'
       |inner join ${connection.schema}.dim_tenant t on t.tenant_id = staging_pathway_placement.fpp_tenant_id
       |inner join ${connection.schema}_stage.rel_dw_id_mappings c on c.id = staging_pathway_placement.fpp_class_id and c.entity_type = 'class'
       |inner join ${connection.schema}_stage.rel_user user2 on user2.user_id = staging_pathway_placement.fpp_student_id where user2.user_type = 'STUDENT'
       |
       |order by ${pkNotation("staging_pathway_placement")}
       |limit $QUERY_LIMIT
       |""".stripMargin
  }

  def makeColumnNames(cols: List[String]): String = cols.map {
    case "fpp_dw_created_time" => "getdate() as fpp_dw_created_time"
    case "fpp_created_by_dw_id" => "user1.user_dw_id as fpp_created_by_dw_id"
    case "fpp_course_dw_id" => "pc.dw_id as fpp_course_dw_id"
    case "fpp_tenant_dw_id" => "t.tenant_dw_id as fpp_tenant_dw_id"
    case "fpp_class_dw_id" => "c.dw_id as fpp_class_dw_id"
    case "fpp_student_dw_id" => "user2.user_dw_id as fpp_student_dw_id"
    case col => col
  }.mkString(",\n")

}
