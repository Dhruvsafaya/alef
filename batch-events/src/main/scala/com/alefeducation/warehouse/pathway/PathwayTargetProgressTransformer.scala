package com.alefeducation.warehouse.pathway

import com.alefeducation.warehouse.core.CommonTransformer

object PathwayTargetProgressTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
    "fptp_dw_id",
    "fptp_id",
    "fptp_student_target_id",
    "fptp_target_id",
    "fptp_target_dw_id",
    "fptp_student_id",
    "fptp_student_dw_id",
    "fptp_tenant_id",
    "fptp_tenant_dw_id",
    "fptp_school_id",
    "fptp_school_dw_id",
    "fptp_grade_id",
    "fptp_grade_dw_id",
    "fptp_pathway_id",
    "fptp_pathway_dw_id",
    "fptp_teacher_id",
    "fptp_teacher_dw_id",
    "fptp_class_id",
    "fptp_class_dw_id",
    "fptp_target_state",
    "fptp_recommended_target_level",
    "fptp_finalized_target_level",
    "fptp_levels_completed",
    "fptp_created_time",
    "fptp_dw_created_time",
    "fptp_date_dw_id"
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
       |INSERT INTO $schema.fact_pathway_target_progress ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "fptp_dw_id"

  override def getStagingTableName(): String = "staging_pathway_target_progress"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       |  JOIN $schema.dim_tenant t ON t.tenant_id = staging.fptp_tenant_id
       |  JOIN $schema.dim_school s ON s.school_id = staging.fptp_school_id
       |  JOIN $schema.dim_grade g ON g.grade_id = staging.fptp_grade_id
       |  JOIN ${schema}_stage.rel_dw_id_mappings c ON c.id = staging.fptp_class_id and c.entity_type = 'class'
       |  JOIN ${schema}_stage.rel_dw_id_mappings p ON p.id = staging.fptp_pathway_id and p.entity_type = 'course'
       |  JOIN ${schema}_stage.rel_dw_id_mappings target ON target.id = staging.fptp_target_id and target.entity_type = 'pathway_target'
       |  JOIN ${schema}_stage.rel_user te ON te.user_id = staging.fptp_teacher_id and te.user_type = 'TEACHER'
       |  JOIN ${schema}_stage.rel_user st ON st.user_id = staging.fptp_student_id and st.user_type = 'STUDENT'
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "fptp_tenant_dw_id" => s"\tt.tenant_dw_id AS fptp_tenant_dw_id"
        case "fptp_school_dw_id" => s"\ts.school_dw_id AS fptp_school_dw_id"
        case "fptp_grade_dw_id" => s"\tg.grade_dw_id AS fptp_grade_dw_id"
        case "fptp_class_dw_id" => s"\tc.dw_id AS fptp_class_dw_id"
        case "fptp_pathway_dw_id" => s"\tp.dw_id AS fptp_pathway_dw_id"
        case "fptp_target_dw_id" => s"\ttarget.dw_id AS fptp_target_dw_id"
        case "fptp_teacher_dw_id" => s"\tte.user_dw_id AS fptp_teacher_dw_id"
        case "fptp_student_dw_id" => s"\tst.user_dw_id AS fptp_student_dw_id"
        case col => s"\t$col"
      }.mkString(",\n")
}
