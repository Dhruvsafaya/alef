package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object AlefChallengeGameProgressTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
    "fgc_dw_id",
    "fgc_id",
    "fgc_game_id",
    "fgc_state",
    "fgc_tenant_dw_id",
    "fgc_tenant_id",
    "fgc_student_dw_id",
    "fgc_student_id",
    "fgc_academic_year_dw_id",
    "fgc_academic_year_id",
    "fgc_academic_year_tag",
    "fgc_school_dw_id",
    "fgc_school_id",
    "fgc_grade",
    "fgc_organization",
    "fgc_score",
    "fgc_created_time",
    "fgc_dw_created_time",
    "fgc_date_dw_id"
  )

  override def getSelectQuery(schema: String): String = {
    val pkCol = getPkColumn()
    val from = fromStatement(schema)

    s"SELECT $pkCol $from ORDER BY $pkCol LIMIT $QUERY_LIMIT"
  }

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idsInStatement = ids.mkString(",")
    val insCols = mainColNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColNames(mainColNames)
    s"""
       |INSERT INTO $schema.fact_challenge_game_progress ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "fgc_dw_id"

  override def getStagingTableName(): String = "staging_challenge_game_progress"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       |  JOIN $schema.dim_tenant t ON t.tenant_id = staging.fgc_tenant_id
       |  JOIN ${schema}_stage.rel_user st ON st.user_id = staging.fgc_student_id and st.user_type = 'STUDENT'
       |  LEFT JOIN $schema.dim_school s ON s.school_id = staging.fgc_school_id
       |  LEFT JOIN $schema.dim_academic_year ay ON ay.academic_year_id = staging.fgc_academic_year_id
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "fgc_tenant_dw_id" => s"\tt.tenant_dw_id AS fgc_tenant_dw_id"
        case "fgc_student_dw_id" => s"\tst.user_dw_id AS fgc_student_dw_id"
        case "fgc_school_dw_id" => s"\ts.school_dw_id AS fgc_school_dw_id"
        case "fgc_academic_year_dw_id" => s"\tay.academic_year_dw_id AS fgc_academic_year_dw_id"
        case col => s"\t$col"
      }.mkString(",\n")
}
