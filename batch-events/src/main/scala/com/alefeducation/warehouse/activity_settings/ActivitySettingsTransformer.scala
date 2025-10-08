package com.alefeducation.warehouse.activity_settings

import com.alefeducation.warehouse.core.CommonTransformer

object ActivitySettingsTransformer extends CommonTransformer {

  val mainColumnNames: List[String] = List(
    "fas_activity_dw_id",
    "fas_activity_id",
    "fas_class_dw_id",
    "fas_class_gen_subject_name",
    "fas_class_id",
    "fas_created_time",
    "fas_dw_created_time",
    "fas_dw_id",
    "fas_grade_dw_id",
    "fas_grade_id",
    "fas_k12_grade",
    "fas_school_dw_id",
    "fas_school_id",
    "fas_teacher_dw_id",
    "fas_teacher_id",
    "fas_tenant_dw_id",
    "fas_tenant_id"
  )

  override def getSelectQuery(schema: String): String = {
    s"""
       |SELECT ${getPkColumn()}
       |${from(schema)}
       |ORDER BY ${getPkColumn()}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin
  }

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val allIds = ids.mkString(",")
    val columnsToInsert = mainColumnNames.mkString("\n\t", ",\n\t", "\n")
    val columnsToSelect = prepareColumnNames(mainColumnNames)
    s"""
       |INSERT INTO $schema.fact_activity_setting ($columnsToInsert)
       |SELECT $columnsToSelect
       |${from(schema)}
       |  AND staging.${getPkColumn()} IN ($allIds)
       |""".stripMargin
  }

  override def getPkColumn(): String = "fas_dw_id"

  override def getStagingTableName(): String = "staging_activity_setting"

  private def from(schema: String): String = {
    s"""
       |FROM ${schema}_stage.staging_activity_setting staging
       |   JOIN ${schema}.dim_tenant t ON t.tenant_id = staging.fas_tenant_id
       |   JOIN ${schema}.dim_school s ON s.school_id = staging.fas_school_id
       |   JOIN ${schema}_stage.rel_user te ON te.user_id = staging.fas_teacher_id
       |   JOIN ${schema}_stage.rel_dw_id_mappings dwidm ON dwidm.id = staging.fas_class_id and dwidm.entity_type = 'class'
       |   LEFT JOIN
       |   (SELECT lo_id, lo_dw_id
       |   FROM
       |     (SELECT lo_id, lo_dw_id, row_number() OVER (PARTITION BY lo_id ORDER BY lo_created_time DESC) AS rank
       |      FROM ${schema}.dim_learning_objective) r
       |   WHERE r.rank = 1) AS lesson ON lesson.lo_id = staging.fas_activity_id
       |   LEFT JOIN ${schema}.dim_interim_checkpoint ic on ic.ic_id = staging.fas_activity_id
       |   JOIN ${schema}.dim_grade g ON g.grade_id = staging.fas_grade_id
       |
       |WHERE (lesson.lo_id IS NOT NULL OR ic.ic_id IS NOT NULL)
       |""".stripMargin
  }

  private def prepareColumnNames(columns: List[String]): String = {
    columns
      .map {
        case "fas_teacher_dw_id"  => s"\tte.user_dw_id AS fas_teacher_dw_id"
        case "fas_activity_dw_id" => s"\tNVL(lesson.lo_dw_id, ic.ic_dw_id) AS fas_activity_dw_id"
        case "fas_class_dw_id"    => s"\tdwidm.dw_id AS fas_class_dw_id"
        case "fas_school_dw_id"   => s"\ts.school_dw_id AS fas_school_dw_id"
        case "fas_grade_dw_id"    => s"\tg.grade_dw_id AS fas_grade_dw_id"
        case "fas_tenant_dw_id"   => s"\tt.tenant_dw_id AS fas_tenant_dw_id"
        case column               => s"\t$column"
      }
      .mkString(",\n")
  }
}
