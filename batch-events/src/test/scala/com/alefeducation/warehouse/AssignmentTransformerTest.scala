package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AssignmentTransformerTest extends AnyFunSuite with Matchers {

  val transformer = AssignmentTransformer
  val schema = "alefdw"

  test("should prepare query") {
    val selectQuery = transformer.getSelectQuery(schema)
    val expectedSelectQuery =
      """
        |SELECT a.rel_assignment_id
        |FROM alefdw_stage.rel_assignment a
        |         LEFT JOIN alefdw.dim_tenant t ON t.tenant_id = a.assignment_tenant_id
        |         LEFT JOIN alefdw.dim_school s ON s.school_id = a.assignment_school_id
        |WHERE (a.assignment_tenant_id is null)
        |   OR (t.tenant_id is not null) AND (a.assignment_school_id is null)
        |   OR (s.school_id is not null)
        |ORDER BY rel_assignment_id
        |LIMIT 60000
        |""".stripMargin

    val insertQuery = transformer.getInsertFromSelectQuery(schema, List(4003, 4004, 4005))

    val expectedInsertQuery =
      """
        |INSERT INTO alefdw.dim_assignment (
        |	assignment_created_time,
        |	assignment_updated_time,
        |	assignment_deleted_time,
        |	assignment_dw_created_time,
        |	assignment_dw_updated_time,
        |	assignment_id,
        |	assignment_title,
        |	assignment_attachment_file_id,
        |	assignment_attachment_file_name,
        |	assignment_attachment_path,
        |	assignment_allow_submission,
        |	assignment_language,
        |	assignment_status,
        |	assignment_is_gradeable,
        |	assignment_assignment_status,
        |	assignment_created_by,
        |	assignment_updated_by,
        |	assignment_published_on,
        |	assignment_attachment_required,
        |	assignment_comment_required,
        |	assignment_type,
        |	assignment_metadata_author,
        |	assignment_metadata_is_sa,
        |	assignment_metadata_authored_date,
        |	assignment_metadata_language,
        |	assignment_metadata_format_type,
        |	assignment_metadata_lexile_level,
        |	assignment_metadata_difficulty_level,
        |	assignment_metadata_resource_type,
        |	assignment_metadata_knowledge_dimensions,
        |	assignment_max_score,
        |	assignment_school_dw_id,
        |	assignment_tenant_dw_id
        |)
        |SELECT
        |	assignment_created_time,
        |	assignment_updated_time,
        |	assignment_deleted_time,
        |	assignment_dw_created_time,
        |	assignment_dw_updated_time,
        |	assignment_id,
        |	assignment_title,
        |	assignment_attachment_file_id,
        |	assignment_attachment_file_name,
        |	assignment_attachment_path,
        |	assignment_allow_submission,
        |	assignment_language,
        |	assignment_status,
        |	assignment_is_gradeable,
        |	assignment_assignment_status,
        |	assignment_created_by,
        |	assignment_updated_by,
        |	assignment_published_on,
        |	assignment_attachment_required,
        |	assignment_comment_required,
        |	assignment_type,
        |	assignment_metadata_author,
        |	assignment_metadata_is_sa,
        |	assignment_metadata_authored_date,
        |	assignment_metadata_language,
        |	assignment_metadata_format_type,
        |	assignment_metadata_lexile_level,
        |	assignment_metadata_difficulty_level,
        |	assignment_metadata_resource_type,
        |	assignment_metadata_knowledge_dimensions,
        |	assignment_max_score,
        |	s.school_dw_id AS assignment_school_dw_id,
        |	t.tenant_dw_id AS assignment_school_dw_id
        |
        |FROM alefdw_stage.rel_assignment a
        | LEFT JOIN alefdw.dim_tenant t ON t.tenant_id = a.assignment_tenant_id
        | LEFT JOIN alefdw.dim_school s ON s.school_id = a.assignment_school_id
        |WHERE
        | (a.assignment_tenant_id is null) OR (t.tenant_id is not null) AND
        | (a.assignment_school_id is null) OR (s.school_id is not null)
        |
        |AND a.rel_assignment_id IN (4003,4004,4005)
        |""".stripMargin

    transformer.getPkColumn() should be("rel_assignment_id")
    transformer.getStagingTableName() should be("rel_assignment")
    replaceSpecChars(selectQuery) should be(replaceSpecChars(expectedSelectQuery))
    replaceSpecChars(insertQuery) should be(replaceSpecChars(expectedInsertQuery))
  }
}
