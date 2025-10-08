package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object PacingGuideTransformer extends CommonTransformer{
  val mainColumnNames: List[String] = List(
    "pacing_period_start_date",
    "pacing_period_label",
    "pacing_activity_id",
    "pacing_course_id",
    "pacing_updated_time",
    "pacing_status",
    "pacing_interval_id",
    "pacing_academic_calendar_id",
    "pacing_academic_year_id",
    "pacing_period_id",
    "pacing_ip_id",
    "pacing_activity_order",
    "pacing_period_end_date",
    "pacing_id",
    "pacing_interval_start_date",
    "pacing_interval_type",
    "pacing_interval_label",
    "pacing_class_id",
    "pacing_dw_updated_time",
    "pacing_interval_end_date",
    "pacing_created_time",
    "pacing_dw_created_time",
    "pacing_dw_id",
    "pacing_tenant_id",
    "pacing_tenant_dw_id",
    "pacing_activity_dw_id",
    "pacing_class_dw_id",
    "pacing_course_dw_id"
  )

  override def getSelectQuery(schema: String): String =   s"""
                                                             |SELECT ${getPkColumn()}
                                                             |${fromStatement(schema)}
                                                             |ORDER BY ${getPkColumn()}
                                                             |LIMIT $QUERY_LIMIT
                                                             |""".stripMargin

  def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.${getStagingTableName()} staging
       |   JOIN $schema.dim_tenant ON dim_tenant.tenant_id = staging.pacing_tenant_id
       |   JOIN ${schema}_stage.rel_dw_id_mappings c ON c.id = staging.pacing_class_id and c.entity_type = 'class'
       |   JOIN ${schema}_stage.rel_dw_id_mappings p ON p.id = staging.pacing_course_id and p.entity_type = 'course'
       |   JOIN
       |   (select lo_dw_id as act_dw_id, lo_id as act_id
       |    from ${schema}.dim_learning_objective
       |    union
       |    select ic_dw_id as act_dw_id, ic_id as act_id
       |    from ${schema}.dim_interim_checkpoint
       |    )  AS act ON act.act_id = staging.pacing_activity_id
       |""".stripMargin

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idClause = ids.mkString(",")
    val insCols = mainColumnNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColumnNames(mainColumnNames)
    s"""
       |INSERT INTO $schema.dim_pacing_guide ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |  AND staging.$getPkColumn IN ($idClause)
       |""".stripMargin
  }

  def makeColumnNames(cols: List[String]): String =
    cols
      .map {
        case "pacing_tenant_dw_id"   => s"\tdim_tenant.tenant_dw_id AS pacing_tenant_dw_id"
        case "pacing_activity_dw_id"  => s"\tact.act_dw_id AS pacing_activity_dw_id"
        case "pacing_class_dw_id"  => s"\tc.dw_id AS pacing_class_dw_id"
        case "pacing_course_dw_id"    => s"\tp.dw_id AS pacing_course_dw_id"
        case col                   => s"\t$col"
      }
      .mkString(",\n")

  override def getPkColumn(): String = "pacing_dw_id"

  override def getStagingTableName(): String = "rel_pacing_guide"
}
