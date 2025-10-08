package com.alefeducation.warehouse.school

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SchoolContentRepositoryAssociationTransformerTest extends AnyFunSuite with Matchers {

  val transformer: SchoolContentRepositoryAssociationTransformer.type = SchoolContentRepositoryAssociationTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")

    val expectedQuery =
      s"""
         |SELECT scra_dw_id
         |FROM alefdw_stage.rel_school_content_repository_association staging
         |  JOIN alefdw.dim_school s ON s.school_id = staging.scra_school_id
         |  JOIN alefdw.dim_content_repository r ON r.content_repository_id = staging.scra_content_repository_id
         |ORDER BY scra_dw_id LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO alefdw.dim_school_content_repository_association (
        |   scra_dw_id,
        |   scra_school_id,
        |   scra_school_dw_id,
        |   scra_content_repository_id,
        |   scra_content_repository_dw_id,
        |   scra_status,
        |   scra_active_until,
        |   scra_created_time,
        |   scra_dw_created_time
        |)
        |SELECT
        |   scra_dw_id,
        |   scra_school_id,
        |   s.school_dw_id AS scra_school_dw_id,
        |   scra_content_repository_id,
        |   r.content_repository_dw_id AS scra_content_repository_dw_id,
        |   scra_status,
        |   scra_active_until,
        |   scra_created_time,
        |   scra_dw_created_time
        |FROM alefdw_stage.rel_school_content_repository_association staging
        | JOIN alefdw.dim_school s ON s.school_id = staging.scra_school_id
        | JOIN alefdw.dim_content_repository r ON r.content_repository_id = staging.scra_content_repository_id
        |WHERE staging.scra_dw_id IN (10,12,13)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}