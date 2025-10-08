package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object TeacherTaskCenterTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
    "dw_id",
    "created_time",
    "dw_created_time",
    "date_dw_id",
    "event_type",
    "_trace_id",
    "event_id",
    "task_id",
    "task_type",
    "school_id",
    "school_dw_id",
    "class_id",
    "class_dw_id",
    "teacher_id",
    "teacher_dw_id",
    "tenant_id",
    "tenant_dw_id"
  )

  override def getSelectQuery(schema: String): String = {
    val fipPkCol = s"staging.$getPkColumn"
    val from = fromStatement(schema)

    s"SELECT $fipPkCol $from ORDER BY $fipPkCol LIMIT $QUERY_LIMIT"
  }

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idsInStatement = ids.mkString(",")
    val insCols = mainColNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColNames(mainColNames)
    s"""
       |INSERT INTO $schema.fact_teacher_task_center ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "dw_id"

  override def getStagingTableName(): String = "staging_teacher_task_center"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       |  JOIN $schema.dim_tenant t ON t.tenant_id = staging.tenant_id
       |  JOIN $schema.dim_school s ON s.school_id = staging.school_id
       |  JOIN ${schema}_stage.rel_dw_id_mappings c ON c.id = staging.class_id and c.entity_type = 'class'
       |  JOIN ${schema}_stage.rel_user u ON u.user_id = staging.teacher_id and u.user_type = 'TEACHER'
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "tenant_dw_id"  => s"\tt.tenant_dw_id AS tenant_dw_id"
        case "school_dw_id"  => s"\ts.school_dw_id AS school_dw_id"
        case "class_dw_id"  => s"\tc.dw_id AS class_dw_id"
        case "teacher_dw_id"  => s"\tu.user_dw_id AS teacher_dw_id"
        case "dw_created_time" => "getdate()"
        case col => s"\tstaging.$col"
      }.mkString(",\n")
}
