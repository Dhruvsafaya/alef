package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object GuardianJointActivityTransformer extends CommonRedshiftTransformer {

  override val pkCol: List[String] = List("fgja_dw_id")

  override val mainColumnNames: List[String] = List(
    "fgja_created_time"
    ,"fgja_dw_created_time"
    ,"fgja_date_dw_id"
    ,"fgja_tenant_dw_id"
    ,"fgja_school_dw_id"
    ,"fgja_k12_grade"
    ,"fgja_class_dw_id"
    ,"fgja_student_dw_id"
    ,"fgja_guardian_dw_id"
    ,"fgja_course_dw_id"
    ,"fgja_course_activity_container_dw_id"
    ,"fgja_attempt"
    ,"fgja_rating"
    ,"fgja_state"
  )

  override val stagingTableName: String = "staging_guardian_joint_activity"

  override val mainTableName: String = "fact_guardian_joint_activity"

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
      |SELECT
      |fgja_created_time,
      |fgja_dw_created_time,
      |gja.fgja_date_dw_id,
      |dim_tenant.tenant_dw_id AS fgja_tenant_dw_id,
      |dim_school.school_dw_id AS fgja_school_dw_id,
      |gja.fgja_k12_grade AS fgja_k12_grade,
      |class_dw_id.dw_id AS fgja_class_dw_id,
      |student_dw_id.user_dw_id AS fgja_student_dw_id,
      |guardian_dw_id.user_dw_id AS fgja_guardian_dw_id,
      |course_dw_id.dw_id AS fgja_course_dw_id,
      |container_dw_id.dw_id AS fgja_course_activity_container_dw_id,
      |fgja_attempt,
      |fgja_rating,
      |gja.fgja_state
      |FROM ${connection.schema}_stage.staging_guardian_joint_activity gja
      |INNER JOIN ${connection.schema}.dim_tenant ON gja.fgja_tenant_id = dim_tenant.tenant_id
      |INNER JOIN ${connection.schema}.dim_school ON gja.fgja_school_id = dim_school.school_id
      |INNER JOIN ${connection.schema}_stage.rel_dw_id_mappings class_dw_id ON gja.fgja_class_id = class_dw_id.id and class_dw_id.entity_type = 'class'
      |INNER JOIN ${connection.schema}_stage.rel_user student_dw_id ON gja.fgja_student_id = student_dw_id.user_id and student_dw_id.user_type = 'STUDENT'
      |INNER JOIN ${connection.schema}_stage.rel_user guardian_dw_id ON gja.fgja_guardian_id = guardian_dw_id.user_id and guardian_dw_id.user_type = 'GUARDIAN'
      |INNER JOIN ${connection.schema}_stage.rel_dw_id_mappings course_dw_id ON gja.fgja_pathway_id = course_dw_id.id and course_dw_id.entity_type = 'course'
      |LEFT JOIN ${connection.schema}_stage.rel_dw_id_mappings container_dw_id ON gja.fgja_pathway_level_id = container_dw_id.id and container_dw_id.entity_type = 'course_activity_container'
      |ORDER BY ${pkNotation(stagingTableName)}
      |LIMIT $QUERY_LIMIT
      |""".stripMargin

  override def pkNotation: Map[String, String] = Map("staging_guardian_joint_activity" -> "fgja_created_time")
}
