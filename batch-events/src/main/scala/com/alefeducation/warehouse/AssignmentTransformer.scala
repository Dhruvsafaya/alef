package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object AssignmentTransformer extends CommonTransformer {

  val dimTableColumns = List(
    "assignment_created_time",
    "assignment_updated_time",
    "assignment_deleted_time",
    "assignment_dw_created_time",
    "assignment_dw_updated_time",
    "assignment_id",
    "assignment_title",
    "assignment_attachment_file_id",
    "assignment_attachment_file_name",
    "assignment_attachment_path",
    "assignment_allow_submission",
    "assignment_language",
    "assignment_status",
    "assignment_is_gradeable",
    "assignment_assignment_status",
    "assignment_created_by",
    "assignment_updated_by",
    "assignment_published_on",
    "assignment_attachment_required",
    "assignment_comment_required",
    "assignment_type",
    "assignment_metadata_author",
    "assignment_metadata_is_sa",
    "assignment_metadata_authored_date",
    "assignment_metadata_language",
    "assignment_metadata_format_type",
    "assignment_metadata_lexile_level",
    "assignment_metadata_difficulty_level",
    "assignment_metadata_resource_type",
    "assignment_metadata_knowledge_dimensions",
    "assignment_max_score"
  )

  val dwIds = List(
    "assignment_school_dw_id",
    "assignment_tenant_dw_id",
  )

  val uuids = List(
    "s.school_dw_id AS assignment_school_dw_id",
    "t.tenant_dw_id AS assignment_school_dw_id",
  )

  val insCols = dimTableColumns ++ dwIds
  val selCols = dimTableColumns ++ uuids

  override def getSelectQuery(schema: String): String =
    s"""
      |SELECT
      |     a.${getPkColumn()}
      | ${fromStatement(schema)}
      |ORDER BY ${getPkColumn()}
      |LIMIT $QUERY_LIMIT
      |""".stripMargin

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val insColsStr = insCols.mkString("\n\t", ",\n\t", "\n")
    val selColsStr = selCols.mkString("\n\t", ",\n\t", "\n")
    val idCols = ids.mkString(",")
    s"""
      |INSERT INTO $schema.dim_assignment ($insColsStr)
      |SELECT $selColsStr ${fromStatement(schema)}
      |AND a.${getPkColumn()} IN ($idCols)
      |""".stripMargin
  }

  override def getPkColumn(): String = "rel_assignment_id"

  override def getStagingTableName(): String = "rel_assignment"

  private def fromStatement(schema: String): String =
    s"""
      |FROM ${schema}_stage.${getStagingTableName()} a
      | LEFT JOIN $schema.dim_tenant t ON t.tenant_id = a.assignment_tenant_id
      | LEFT JOIN $schema.dim_school s ON s.school_id = a.assignment_school_id
      |WHERE
      | (a.assignment_tenant_id is null) OR (t.tenant_id is not null) AND
      | (a.assignment_school_id is null) OR (s.school_id is not null)
      |""".stripMargin
}
