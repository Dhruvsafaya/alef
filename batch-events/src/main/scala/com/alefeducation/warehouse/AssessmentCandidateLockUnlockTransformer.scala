package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object AssessmentCandidateLockUnlockTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
      "dw_id",
      "event_type",
      "_trace_id",
      "created_time",
      "dw_created_time",
      "date_dw_id",
      "id",
      "tenant_id",
      "tenant_dw_id",
      "academic_year_tag",
      "attempt",
      "teacher_id",
      "teacher_dw_id",
      "candidate_id",
      "candidate_dw_id",
      "school_id",
      "school_dw_id",
      "class_id",
      "class_dw_id",
      "test_part_session_id",
      "test_level_name",
      "test_level_dw_id",
      "test_level_id",
      "test_level_version",
      "skill",
      "subject"
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
       |INSERT INTO $schema.fact_assessment_lock_action ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "dw_id"

  override def getStagingTableName(): String = "staging_assessment_lock_action"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       |  JOIN $schema.dim_tenant t ON t.tenant_id = staging.tenant_id
       |  JOIN $schema.dim_school s ON s.school_id = staging.school_id
       |  JOIN ${schema}_stage.rel_dw_id_mappings c ON c.id = staging.class_id
       |  JOIN ${schema}_stage.rel_user st ON st.user_id = staging.candidate_id
       |  JOIN ${schema}_stage.rel_user tc ON tc.user_id = staging.teacher_id
       |  JOIN $schema.dim_testpart tp ON tp.id = staging.test_level_id and tp.version = staging.test_level_version
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "tenant_dw_id" => s"\tt.tenant_dw_id AS tenant_dw_id"
        case "school_dw_id" => s"\ts.school_dw_id AS school_dw_id"
        case "class_dw_id" => s"\tc.dw_id AS class_dw_id"
        case "candidate_dw_id" => s"\tst.user_dw_id AS candidate_dw_id"
        case "teacher_dw_id" => s"\ttc.user_dw_id AS teacher_dw_id"
        case "test_level_dw_id" => s"\ttp.dw_id AS test_level_dw_id"
        case col => s"\tstaging.$col"
      }.mkString(",\n")
}
