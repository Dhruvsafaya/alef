package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object EBookProgressTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
    "fep_dw_id",
    "fep_id",
    "fep_session_id",
    "fep_exp_id",
    "fep_tenant_id",
    "fep_tenant_dw_id",
    "fep_student_id",
    "fep_student_dw_id",
    "feb_ay_tag",
    "fep_school_id",
    "fep_school_dw_id",
    "fep_grade_id",
    "fep_grade_dw_id",
    "fep_class_id",
    "fep_class_dw_id",
    "fep_lo_id",
    "fep_lo_dw_id",
    "fep_step_instance_step_id",
    "fep_material_type",
    "fep_title",
    "fep_total_pages",
    "fep_has_audio",
    "fep_action",
    "fep_is_last_page",
    "fep_location",
    "fep_state",
    "fep_time_spent",
    "fep_bookmark_location",
    "fep_highlight_location",
    "fep_created_time",
    "fep_dw_created_time",
    "fep_date_dw_id"
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
       |INSERT INTO $schema.fact_ebook_progress ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "fep_dw_id"

  override def getStagingTableName(): String = "staging_ebook_progress"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       |  JOIN $schema.dim_tenant t ON t.tenant_id = staging.fep_tenant_id
       |  JOIN $schema.dim_school s ON s.school_id = staging.fep_school_id
       |  JOIN $schema.dim_grade g ON g.grade_id = staging.fep_grade_id
       |  JOIN ${schema}_stage.rel_dw_id_mappings c ON c.id = staging.fep_class_id and c.entity_type = 'class'
       |  JOIN ${schema}_stage.rel_user u ON u.user_id = staging.fep_student_id and u.user_type = 'STUDENT'
       |  JOIN $schema.dim_learning_objective lo ON lo.lo_id = staging.fep_lo_id
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "fep_tenant_dw_id"  => s"\tt.tenant_dw_id AS fep_tenant_dw_id"
        case "fep_school_dw_id"  => s"\ts.school_dw_id AS fep_school_dw_id"
        case "fep_grade_dw_id"  => s"\tg.grade_dw_id AS fep_grade_dw_id"
        case "fep_class_dw_id"  => s"\tc.dw_id AS fep_class_dw_id"
        case "fep_student_dw_id"  => s"\tu.user_dw_id AS fep_student_dw_id"
        case "fep_lo_dw_id"  => s"\tlo.lo_dw_id AS fep_lo_dw_id"
        case col => s"\t$col"
      }.mkString(",\n")
}
