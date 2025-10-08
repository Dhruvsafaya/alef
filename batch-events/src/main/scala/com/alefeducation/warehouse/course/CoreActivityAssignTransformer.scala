package com.alefeducation.warehouse.course

import com.alefeducation.warehouse.core.CommonTransformer

object CoreActivityAssignTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
      "cta_dw_id",
      "cta_created_time",
      "cta_dw_created_time",
      "cta_active_until",
      "cta_status",
      "cta_event_type",
      "cta_id",
      "cta_action_time",
      "cta_ay_tag",
      "cta_tenant_dw_id",
      "cta_tenant_id",
      "cta_student_dw_id",
      "cta_student_id",
      "cta_course_dw_id",
      "cta_course_id",
      "cta_class_dw_id",
      "cta_class_id",
      "cta_teacher_dw_id",
      "cta_teacher_id",
      "cta_activity_dw_id",
      "cta_activity_id",
      "cta_activity_type",
      "cta_progress_status",
      "cta_start_date",
      "cta_end_date"
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
       |INSERT INTO $schema.dim_core_activity_assign ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE rel.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "cta_dw_id"

  override def getStagingTableName(): String = "rel_core_activity_assign"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName rel
       |  JOIN $schema.dim_tenant t ON t.tenant_id = rel.cta_tenant_id
       |  JOIN $schema.dim_learning_objective lo ON lo.lo_id = rel.cta_activity_id
       |  JOIN ${schema}_stage.rel_dw_id_mappings c ON c.id = rel.cta_class_id and c.entity_type = 'class'
       |  JOIN ${schema}_stage.rel_dw_id_mappings cr ON cr.id = rel.cta_course_id and cr.entity_type = 'course'
       |  JOIN ${schema}_stage.rel_user u ON u.user_id = rel.cta_student_id and u.user_type = 'STUDENT'
       |  JOIN ${schema}_stage.rel_user teacher ON teacher.user_id = rel.cta_teacher_id and teacher.user_type = 'TEACHER'
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "cta_tenant_dw_id" => s"\tt.tenant_dw_id AS cta_tenant_dw_id"
        case "cta_activity_dw_id" => s"\tlo.lo_dw_id AS cta_activity_dw_id"
        case "cta_class_dw_id" => s"\tc.dw_id AS cta_class_dw_id"
        case "cta_course_dw_id" => s"\tcr.dw_id AS cta_course_dw_id"
        case "cta_student_dw_id" => s"\tu.user_dw_id AS cta_student_dw_id"
        case "cta_teacher_dw_id" => s"\tteacher.user_dw_id AS cta_teacher_dw_id"
        case col => s"\t$col"
      }.mkString(",\n")
}
