package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object AssessmentAnswerSubmittedTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
    "dw_id",
    "created_time",
    "dw_created_time",
    "date_dw_id",
    "_trace_id",
    "tenant_id",
    "tenant_dw_id",
    "event_type",
    "assessment_id",
    "academic_year_tag",
    "school_id",
    "school_dw_id",
    "grade_id",
    "grade_dw_id",
    "candidate_id",
    "candidate_dw_id",
    "class_id",
    "class_dw_id",
    "grade",
    "material_type",
    "attempt_number",
    "skill",
    "subject",
    "language",
    "test_level_session_id",
    "test_level_version",
    "test_level_dw_id",
    "test_level_section_id",
    "test_level_section_dw_id",
    "test_level",
    "time_spent",
    "question_id",
    "question_code",
    "question_version",
    "reference_code"
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
       |INSERT INTO $schema.fact_assessment_answer_submitted ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "dw_id"

  override def getStagingTableName(): String = "staging_assessment_answer_submitted"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       |  JOIN $schema.dim_tenant t ON t.tenant_id = staging.tenant_id
       |  JOIN $schema.dim_school s ON s.school_id = staging.school_id
       |  JOIN $schema.dim_grade g ON g.grade_id = staging.grade_id
       |  JOIN ${schema}_stage.rel_dw_id_mappings c ON c.id = staging.class_id
       |  JOIN ${schema}_stage.rel_user u ON u.user_id = staging.candidate_id
       |  JOIN $schema.dim_testpart tp ON tp.id = staging.test_level_id and tp.version = staging.test_level_version
       |  JOIN $schema.dim_testpart_section_association tps ON tps.id = staging.test_level_section_id and tps.question_id = staging.question_id
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "tenant_dw_id" => s"\tt.tenant_dw_id AS tenant_dw_id"
        case "school_dw_id" => s"\ts.school_dw_id AS school_dw_id"
        case "grade_dw_id" => s"\tg.grade_dw_id AS grade_dw_id"
        case "class_dw_id" => s"\tc.dw_id AS class_dw_id"
        case "candidate_dw_id" => s"\tu.user_dw_id AS candidate_dw_id"
        case "test_level_dw_id" => s"\ttp.dw_id AS test_level_dw_id"
        case "test_level_section_dw_id" => s"\ttps.dw_id AS test_level_section_dw_id"
        case col => s"\tstaging.$col"
      }.mkString(",\n")
}
