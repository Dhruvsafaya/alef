package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object AssessmentCandidateProgressTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
    "dw_id",
    "_trace_id",
    "event_type",
    "created_time",
    "dw_created_time",
    "date_dw_id",
    "assessment_id",
    "academic_year_tag",
    "tenant_id",
    "tenant_dw_id",
    "school_id",
    "school_dw_id",
    "grade_id",
    "grade_dw_id",
    "grade",
    "class_id",
    "class_dw_id",
    "candidate_id",
    "candidate_dw_id",
    "test_level_session_id",
    "test_level",
    "test_id",
    "test_version",
    "test_level_id",
    "test_level_version",
    "test_level_version_dw_id",
    "test_level_section_id",
    "attempt_number",
    "material_type",
    "skill",
    "subject",
    "language",
    "status",
    "report_id",
    "total_timespent",
    "final_score",
    "final_grade",
    "final_category",
    "final_uncertainty",
    "framework",
    "time_to_return"
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
       |INSERT INTO $schema.fact_candidate_assessment_progress ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "dw_id"

  override def getStagingTableName(): String = "staging_candidate_assessment_progress"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       |  JOIN $schema.dim_tenant t ON t.tenant_id = staging.tenant_id
       |  JOIN $schema.dim_school s ON s.school_id = staging.school_id
       |  JOIN $schema.dim_grade g ON g.grade_id = staging.grade_id
       |  JOIN ${schema}_stage.rel_dw_id_mappings ids ON ids.id = staging.class_id
       |  JOIN ${schema}_stage.rel_user u ON u.user_id = staging.candidate_id
       |  JOIN $schema.dim_testpart tp ON tp.id = staging.test_level_id and tp.version = staging.test_level_version
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "tenant_dw_id" => s"\tt.tenant_dw_id AS tenant_dw_id"
        case "school_dw_id" => s"\ts.school_dw_id AS school_dw_id"
        case "grade_dw_id" => s"\tg.grade_dw_id AS grade_dw_id"
        case "class_dw_id" => s"\tids.dw_id AS class_dw_id"
        case "candidate_dw_id" => s"\tu.user_dw_id AS candidate_dw_id"
        case "test_level_version_dw_id" => s"\ttp.dw_id AS test_level_version_dw_id"
        case col => s"\tstaging.$col"
      }.mkString(",\n")
}
