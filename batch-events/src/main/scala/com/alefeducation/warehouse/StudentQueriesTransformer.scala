package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object StudentQueriesTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
    "dw_id",
    "created_time",
    "dw_created_time",
    "date_dw_id",
    "_trace_id",
    "event_type",
    "message_id",
    "query_id",
    "tenant_id",
    "tenant_dw_id",
    "school_id",
    "school_dw_id",
    "grade_id",
    "grade_dw_id",
    "class_id",
    "class_dw_id",
    "section_id",
    "section_dw_id",
    "teacher_id",
    "teacher_dw_id",
    "academic_year_id",
    "academic_year_dw_id",
    "student_id",
    "student_dw_id",
    "activity_id",
    "activity_dw_id",
    "activity_type",
    "can_student_reply",
    "with_audio",
    "with_text",
    "material_type",
    "gen_subject",
    "lang_code",
    "is_follow_up",
    "has_screenshot"
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
       |INSERT INTO $schema.fact_student_queries ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "dw_id"

  override def getStagingTableName(): String = "staging_student_queries"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       | JOIN ${schema}.dim_tenant t ON t.tenant_id = staging.tenant_id
       |JOIN ${schema}.dim_school s ON s.school_id = staging.school_id
       |JOIN ${schema}.dim_grade g ON g.grade_id = staging.grade_id
       |JOIN ${schema}_stage.rel_dw_id_mappings c ON c.id = staging.class_id and c.entity_type = 'class'
       |JOIN ${schema}.dim_section sc ON sc.section_id = staging.section_id
       |LEFT JOIN ${schema}_stage.rel_user u ON u.user_id = staging.teacher_id and u.user_type = 'TEACHER'
       |JOIN ${schema}.dim_academic_year ay ON ay.academic_year_id = staging.academic_year_id
       |JOIN ${schema}_stage.rel_user st ON st.user_id = staging.student_id and st.user_type = 'STUDENT'
       |LEFT JOIN ${schema}.dim_learning_objective lo ON lo.lo_id = staging.activity_id
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "tenant_dw_id"  => s"\tt.tenant_dw_id AS tenant_dw_id"
        case "school_dw_id"  => s"\ts.school_dw_id AS school_dw_id"
        case "grade_dw_id"  => s"\tg.grade_dw_id AS grade_dw_id"
        case "class_dw_id"  => s"\tc.dw_id AS class_dw_id"
        case "section_dw_id"  => s"\tsc.section_dw_id AS section_dw_id"
        case "teacher_dw_id"  => s"\tu.user_dw_id AS teacher_dw_id"
        case "academic_year_dw_id"  => s"\tay.academic_year_dw_id AS academic_year_dw_id"
        case "student_dw_id"  => s"\tst.user_dw_id AS student_dw_id"
        case "activity_dw_id"  => s"\tlo.lo_dw_id AS activity_dw_id"
        case "dw_created_time" => "getdate()"
        case col => s"\tstaging.$col"
      }.mkString(",\n")
}
