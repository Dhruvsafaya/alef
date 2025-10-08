package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CourseActivityOutcomeAssociationTransformerTest extends AnyFunSuite with Matchers {

  private val transformer = CourseActivityOutcomeAssociationTransformer
  private val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
  private val schema = "testalefdw"

  test("should prepare query") {
    val selectQuery = transformer.getSelectQuery(connection.schema)
    val expectedSelectQuery =
      s"""
         |SELECT
         |  caoa_dw_id
         |FROM testalefdw_stage.rel_course_activity_outcome_association caoa
         |  JOIN testalefdw_stage.rel_dw_id_mappings course ON course.id = caoa.caoa_course_id AND course.entity_type = 'course'
         |  JOIN (
         |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
         |    FROM testalefdw.dim_learning_objective
         |    UNION
         |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
         |    FROM testalefdw.dim_interim_checkpoint
         |  ) AS activity ON activity.act_id = caoa.caoa_activity_id
         |ORDER BY caoa_dw_id
         |LIMIT 60000
         |""".stripMargin

    transformer.getStagingTableName() should be("rel_course_activity_outcome_association")
    transformer.getPkColumn() should be("caoa_dw_id")

    replaceSpecChars(selectQuery) should be(replaceSpecChars(expectedSelectQuery))


    val expectedInsertQuery =
      """
        |
        |
        |INSERT INTO testalefdw.dim_course_activity_outcome_association (
        |	caoa_dw_id,
        |   caoa_created_time,
        |   caoa_updated_time,
        |   caoa_dw_created_time,
        |   caoa_dw_updated_time,
        |   caoa_status,
        |   caoa_course_id,
        |   caoa_activity_id,
        |   caoa_outcome_id,
        |   caoa_outcome_type,
        |   caoa_curr_id,
        |   caoa_curr_grade_id,
        |   caoa_curr_subject_id,
        |   caoa_course_dw_id,
        |   caoa_activity_dw_id
        |)
        |
        |SELECT
        |	caoa_dw_id,
        |   caoa_created_time,
        |   caoa_updated_time,
        |   caoa_dw_created_time,
        |   caoa_dw_updated_time,
        |   caoa_status,
        |   caoa_course_id,
        |   caoa_activity_id,
        |   caoa_outcome_id,
        |   caoa_outcome_type,
        |   caoa_curr_id,
        |   caoa_curr_grade_id,
        |   caoa_curr_subject_id,
        |   course.dw_id AS caoa_course_dw_id,
        |   activity.act_dw_id AS caoa_activity_dw_id
        |FROM testalefdw_stage.rel_course_activity_outcome_association caoa
        |  JOIN testalefdw_stage.rel_dw_id_mappings course ON course.id = caoa.caoa_course_id AND course.entity_type = 'course'
        |  JOIN (
        |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
        |    FROM testalefdw.dim_learning_objective
        |    UNION
        |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
        |    FROM testalefdw.dim_interim_checkpoint
        |  ) AS activity ON activity.act_id = caoa.caoa_activity_id
        |WHERE caoa_dw_id IN (3003,3004,3005)
        |""".stripMargin

    val insertQuery = transformer.getInsertFromSelectQuery(schema, List(3003, 3004, 3005))

    replaceSpecChars(insertQuery) should be(replaceSpecChars(expectedInsertQuery))
  }
}
