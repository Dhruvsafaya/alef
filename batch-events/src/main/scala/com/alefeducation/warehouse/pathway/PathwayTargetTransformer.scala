package com.alefeducation.warehouse.pathway

import com.alefeducation.warehouse.core.CommonTransformer

object PathwayTargetTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
    "pt_dw_id",
    "pt_id",
    "pt_target_id",
    "pt_target_dw_id",
    "pt_status",
    "pt_active_until",
    "pt_target_state",
    "pt_teacher_id",
    "pt_teacher_dw_id",
    "pt_pathway_id",
    "pt_pathway_dw_id",
    "pt_class_id",
    "pt_class_dw_id",
    "pt_school_id",
    "pt_school_dw_id",
    "pt_tenant_id",
    "pt_tenant_dw_id",
    "pt_grade_id",
    "pt_grade_dw_id",
    "pt_start_date",
    "pt_end_date",
    "pt_created_time",
    "pt_dw_created_time"
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
       |INSERT INTO $schema.dim_pathway_target ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE rel.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "pt_dw_id"

  override def getStagingTableName(): String = "rel_pathway_target"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName rel
       |  JOIN $schema.dim_tenant t ON t.tenant_id = rel.pt_tenant_id
       |  JOIN $schema.dim_school s ON s.school_id = rel.pt_school_id
       |  JOIN $schema.dim_grade g ON g.grade_id = rel.pt_grade_id
       |  JOIN ${schema}_stage.rel_dw_id_mappings c ON c.id = rel.pt_class_id and c.entity_type = 'class'
       |  JOIN ${schema}_stage.rel_dw_id_mappings p ON p.id = rel.pt_pathway_id and p.entity_type = 'course'
       |  JOIN ${schema}_stage.rel_dw_id_mappings pt ON pt.id = rel.pt_target_id and pt.entity_type = 'pathway_target'
       |  JOIN ${schema}_stage.rel_user u ON u.user_id = rel.pt_teacher_id and u.user_type = 'TEACHER'
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "pt_tenant_dw_id" => s"\tt.tenant_dw_id AS pt_tenant_dw_id"
        case "pt_school_dw_id" => s"\ts.school_dw_id AS pt_school_dw_id"
        case "pt_grade_dw_id" => s"\tg.grade_dw_id AS pt_grade_dw_id"
        case "pt_class_dw_id" => s"\tc.dw_id AS pt_class_dw_id"
        case "pt_pathway_dw_id" => s"\tp.dw_id AS pt_pathway_dw_id"
        case "pt_target_dw_id" => s"\tpt.dw_id AS pt_target_dw_id"
        case "pt_teacher_dw_id" => s"\tu.user_dw_id AS pt_teacher_dw_id"
        case col => s"\t$col"
      }.mkString(",\n")
}
