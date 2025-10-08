package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object AssignmentInstanceTransformer extends CommonTransformer {

  val dimTableColumns = List(
    "assignment_instance_id",
    "assignment_instance_created_time",
    "assignment_instance_updated_time",
    "assignment_instance_deleted_time",
    "assignment_instance_dw_created_time",
    "assignment_instance_dw_updated_time",
    "assignment_instance_instructional_plan_id",
    "assignment_instance_due_on",
    "assignment_instance_allow_late_submission",
    "assignment_instance_type",
    "assignment_instance_start_on",
    "assignment_instance_status",
    "assignment_instance_trimester_id",
    "assignment_instance_teaching_period_id"
  )

  val dwIds = List(
    "assignment_instance_assignment_dw_id",
    "assignment_instance_teacher_dw_id",
    "assignment_instance_grade_dw_id",
    "assignment_instance_subject_dw_id",
    "assignment_instance_class_dw_id",
    "assignment_instance_lo_dw_id",
    "assignment_instance_section_dw_id",
    "assignment_instance_tenant_dw_id"
  )

  val uuids = List(
    "ai.assignment_dw_id as assignment_instance_assignment_dw_id",
    "teacher.user_dw_id as assignment_instance_teacher_dw_id",
    "g.grade_dw_id as assignment_instance_grade_dw_id",
    "s.subject_dw_id as assignment_instance_subject_dw_id",
    "c.dw_id as assignment_instance_class_dw_id",
    "lo.lo_dw_id as assignment_instance_lo_dw_id",
    "sec.section_dw_id as assignment_instance_section_dw_id",
    "tnt.tenant_dw_id as assignment_instance_tenant_dw_id"
  )

  val insCols = dimTableColumns ++ dwIds
  val selCols = dimTableColumns ++ uuids

  override def getSelectQuery(schema: String): String = {
    s"""
       |SELECT a.${getPkColumn()} ${fromStatement(schema)}
       |ORDER BY a.${getPkColumn()}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin
  }

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val insColsStr = insCols.mkString("\n\t", ",\n\t", "\n")
    val selColsStr = selCols.mkString("\n\t", ",\n\t", "\n")
    val idCols = ids.mkString(",")
    s"""INSERT INTO $schema.dim_assignment_instance ($insColsStr)
       |SELECT $selColsStr
       |${fromStatement(schema)}
       |AND a.${getPkColumn()} IN ($idCols)""".stripMargin
  }

  private def fromStatement(schema: String): String =
    s"""
        |FROM ${schema}_stage.${getStagingTableName()} a
        |INNER JOIN ${schema}_stage.rel_user teacher ON teacher.user_id = a.teacher_uuid
        |INNER JOIN $schema.dim_tenant tnt ON tnt.tenant_id = a.tenant_uuid
        |INNER JOIN $schema.dim_assignment ai on ai.assignment_id = a.assignment_uuid
        |INNER JOIN $schema.dim_grade g ON g.grade_id = a.grade_uuid
        |LEFT JOIN $schema.dim_subject s ON s.subject_id = a.subject_uuid
        |LEFT JOIN ${schema}_stage.rel_dw_id_mappings c ON c.id = a.class_uuid AND c.entity_type = 'class'
        |INNER JOIN (select lo_id, lo_dw_id from (
        |         select lo_id, lo_dw_id, row_number() over (partition by lo_id order by lo_created_time desc) as rank
        |         from $schema.dim_learning_objective
        |          ) t where t.rank = 1
        |       ) AS lo ON lo.lo_id = a.lo_uuid
        |LEFT JOIN $schema.dim_section sec on sec.section_id = a.section_uuid
        |
        |WHERE ((a.section_uuid is null) OR (sec.section_id is not null)) AND
        |      ((a.subject_uuid is null) OR (s.subject_id is not null)) AND
        |      ((a.class_uuid is null) OR (c.id is not null))
        |""".stripMargin

  override def getPkColumn(): String = "assignment_instance_staging_id"

  override def getStagingTableName(): String = "rel_assignment_instance"
}
