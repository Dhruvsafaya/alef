package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object StudentProficiencyTrackerTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
      "dw_id",
      "event_type",
      "_trace_id",
      "tenant_id",
      "tenant_dw_id",
      "date_dw_id",
      "uuid",
      "created_time",
      "dw_created_time",
      "student_id",
      "student_dw_id",
      "class_id",
      "class_dw_id",
      "school_id",
      "school_dw_id",
      "pathway_id",
      "pathway_dw_id",
      "level_id",
      "level_dw_id",
      "academic_year_tag",
      "session_id",
      "ml_session_id",
      "level_proficiency_score",
      "level_proficiency_tier",
      "assessment_id",
      "session_attempt",
      "stars",
      "time_spent",
      "skill_proficiency_tier",
      "skill_proficiency_score",
      "skill_id",
      "skill_dw_id",
      "status",
      "previous_proficiency_tier"
  )

  override def getSelectQuery(schema: String): String = {
    val pkCol = s"staging.$getPkColumn"
    val from = fromStatement(schema)

    s"SELECT $pkCol $from ORDER BY $pkCol LIMIT $QUERY_LIMIT"
  }

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idsInStatement = ids.mkString(",")
    val insCols = mainColNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColNames(mainColNames)
    s"""
       |INSERT INTO $schema.fact_student_proficiency_tracker ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "dw_id"

  override def getStagingTableName(): String = "staging_student_proficiency_tracker"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       |  JOIN $schema.dim_tenant t ON t.tenant_id = staging.tenant_id
       |  JOIN $schema.dim_school s ON s.school_id = staging.school_id
       |  JOIN ${schema}_stage.rel_dw_id_mappings c ON c.id = staging.class_id AND c.entity_type='class'
       |  JOIN ${schema}_stage.rel_dw_id_mappings p ON p.id = staging.pathway_id AND p.entity_type='course'
       |  JOIN ${schema}_stage.rel_dw_id_mappings l ON l.id = staging.level_id AND l.entity_type='course_activity_container'
       |  JOIN ${schema}_stage.rel_user st ON st.user_id = staging.student_id
       |  JOIN $schema.dim_learning_objective dlo ON dlo.lo_id = staging.skill_id
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "tenant_dw_id" => s"\tt.tenant_dw_id AS tenant_dw_id"
        case "school_dw_id" => s"\ts.school_dw_id AS school_dw_id"
        case "class_dw_id" => s"\tc.dw_id AS class_dw_id"
        case "student_dw_id" => s"\tst.user_dw_id AS student_dw_id"
        case "pathway_dw_id" => s"\tp.dw_id AS pathway_dw_id"
        case "level_dw_id" => s"\tl.dw_id AS level_dw_id"
        case "skill_dw_id" => s"\tdlo.lo_dw_id AS skill_dw_id"
        case col => s"\tstaging.$col"
      }.mkString(",\n")
}
