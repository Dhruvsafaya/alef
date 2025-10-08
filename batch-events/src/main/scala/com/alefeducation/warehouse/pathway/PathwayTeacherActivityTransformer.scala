package com.alefeducation.warehouse.pathway

import com.alefeducation.warehouse.core.CommonTransformer

object PathwayTeacherActivityTransformer extends CommonTransformer {

  val mainColumnNames: List[String] = List(
    "fpta_created_time",
    "fpta_dw_created_time",
    "fpta_date_dw_id",
    "fpta_student_id",
    "fpta_student_dw_id",
    "fpta_class_id",
    "fpta_class_dw_id",
    "fpta_pathway_id",
    "fpta_pathway_dw_id",
    "fpta_level_id",
    "fpta_level_dw_id",
    "fpta_tenant_id",
    "fpta_tenant_dw_id",
    "fpta_activity_id",
    "fpta_action_name",
    "fpta_action_time",
    "fpta_dw_id",
    "fpta_teacher_id",
    "fpta_teacher_dw_id",
    "fpta_activity_dw_id",
    "fpta_activity_type",
    "fpta_start_date",
    "fpta_end_date",
    "fpta_course_dw_id",
    "fpta_course_activity_container_dw_id",
    "fpta_activity_progress_status",
    "fpta_activity_type_value",
    "fpta_is_added_as_resource"
  )

  override def getSelectQuery(schema: String): String = {
    s"""
       |SELECT ${getPkColumn()}
       |${fromStatement(schema)}
       |ORDER BY ${getPkColumn()}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin
  }

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idClause = ids.mkString(",")
    val insCols = mainColumnNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColumnNames(mainColumnNames)
    s"""
       |INSERT INTO $schema.fact_pathway_teacher_activity ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |  AND staging.$getPkColumn IN ($idClause)
       |""".stripMargin
  }

  def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.${getStagingTableName()} staging
       |   JOIN $schema.dim_tenant ON dim_tenant.tenant_id = staging.fpta_tenant_id
       |   JOIN ${schema}_stage.rel_user te ON te.user_id = staging.fpta_teacher_id
       |   JOIN ${schema}_stage.rel_user st ON st.user_id = staging.fpta_student_id
       |   LEFT JOIN ${schema}_stage.rel_dw_id_mappings cls ON cls.id = staging.fpta_class_id and cls.entity_type = 'class'
       |   JOIN ${schema}_stage.rel_dw_id_mappings crs ON crs.id = staging.fpta_pathway_id and crs.entity_type = 'course'
       |   LEFT JOIN ${schema}_stage.rel_dw_id_mappings cac on cac.id = staging.fpta_level_id and cac.entity_type = 'course_activity_container'
       |   LEFT JOIN (select lo_id, lo_dw_id from (
       |         select lo_id, lo_dw_id, row_number() over (partition by lo_id order by lo_created_time desc) as rank
       |         from $schema.dim_learning_objective
       |          ) t where t.rank = 1
       |       ) AS lesson ON lesson.lo_id = staging.fpta_activity_id
       |   LEFT JOIN $schema.dim_interim_checkpoint ic on ic.ic_id = staging.fpta_activity_id
       |""".stripMargin

  def makeColumnNames(cols: List[String]): String =
    cols
      .map {
        case "fpta_tenant_dw_id"   => s"\tdim_tenant.tenant_dw_id AS fpta_tenant_dw_id"
        case "fpta_teacher_dw_id"  => s"\tte.user_dw_id AS fpta_teacher_dw_id"
        case "fpta_student_dw_id"  => s"\tst.user_dw_id AS fpta_student_dw_id"
        case "fpta_class_dw_id"    => s"\tcls.dw_id AS fpta_class_dw_id"
        case "fpta_pathway_dw_id"  => s"\tcrs.dw_id AS fpta_pathway_dw_id"
        case "fpta_level_dw_id"    => s"\tcac.dw_id AS fpta_level_dw_id"
        case "fpta_activity_dw_id" => s"\tNVL(lesson.lo_dw_id, ic.ic_dw_id) AS fpta_activity_dw_id"
        case "fpta_activity_type" =>
          "\tCASE " +
            "WHEN staging.fpta_activity_type IN (1,2) THEN staging.fpta_activity_type " +
            "WHEN lesson.lo_id IS NOT NULL THEN 1 " +
            "WHEN ic.ic_id IS NOT NULL THEN 2 " +
            "ELSE -1 " +
            "END AS fpta_activity_type"
        case "fpta_course_dw_id" => s"\tcrs.dw_id AS fpta_course_dw_id"
        case "fpta_course_activity_container_dw_id" => s"\tcac.dw_id AS fpta_course_activity_container_dw_id"
        case col => s"\t$col"
      }
      .mkString(",\n")

  override def getPkColumn(): String = "fpta_dw_id"

  override def getStagingTableName(): String = "staging_pathway_teacher_activity"
}
