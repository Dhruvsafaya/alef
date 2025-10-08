package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CourseResourceActivityOutcomeAssociationTransformerTest extends AnyFunSuite with Matchers  {
  private val transformer = CourseResourceActivityOutcomeAssociationTransformer
  private val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
  private val schema = "testalefdw"

  test("should prepare query") {
    val selectQuery = transformer.getSelectQuery(connection.schema)
    val expectedSelectQuery =
      s"""
         |SELECT
         |  craoa_dw_id
         |FROM testalefdw_stage.rel_course_resource_activity_outcome_association caoa
         |  JOIN testalefdw_stage.rel_dw_id_mappings course ON course.id = caoa.craoa_course_id AND course.entity_type = 'course'
         |  JOIN (
         |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
         |    FROM testalefdw.dim_learning_objective
         |    UNION
         |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
         |    FROM testalefdw.dim_interim_checkpoint
         |  ) AS activity ON activity.act_id = caoa.craoa_activity_id
         |ORDER BY craoa_dw_id
         |LIMIT 60000
         |""".stripMargin

    transformer.getStagingTableName() should be("rel_course_resource_activity_outcome_association")
    transformer.getPkColumn() should be("craoa_dw_id")

    replaceSpecChars(selectQuery) should be(replaceSpecChars(expectedSelectQuery))


    val expectedInsertQuery =
      """
        |
        |
        |INSERT INTO testalefdw.dim_course_resource_activity_outcome_association (
        |	craoa_dw_id,
        |   craoa_created_time,
        |   craoa_updated_time,
        |   craoa_dw_created_time,
        |   craoa_dw_updated_time,
        |   craoa_status,
        |   craoa_course_id,
        |   craoa_activity_id,
        |   craoa_outcome_id,
        |   craoa_outcome_type,
        |   craoa_curr_id,
        |   craoa_curr_grade_id,
        |   craoa_curr_subject_id,
        |   craoa_course_dw_id,
        |   craoa_activity_dw_id
        |)
        |
        |SELECT
        |	craoa_dw_id,
        |   craoa_created_time,
        |   craoa_updated_time,
        |   craoa_dw_created_time,
        |   craoa_dw_updated_time,
        |   craoa_status,
        |   craoa_course_id,
        |   craoa_activity_id,
        |   craoa_outcome_id,
        |   craoa_outcome_type,
        |   craoa_curr_id,
        |   craoa_curr_grade_id,
        |   craoa_curr_subject_id,
        |   course.dw_id AS craoa_course_dw_id,
        |   activity.act_dw_id AS craoa_activity_dw_id
        |FROM testalefdw_stage.rel_course_resource_activity_outcome_association caoa
        |  JOIN testalefdw_stage.rel_dw_id_mappings course ON course.id = caoa.craoa_course_id AND course.entity_type = 'course'
        |  JOIN (
        |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
        |    FROM testalefdw.dim_learning_objective
        |    UNION
        |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
        |    FROM testalefdw.dim_interim_checkpoint
        |  ) AS activity ON activity.act_id = caoa.craoa_activity_id
        |WHERE craoa_dw_id IN (3003,3004,3005)
        |""".stripMargin

    val insertQuery = transformer.getInsertFromSelectQuery(schema, List(3003, 3004, 3005))

    replaceSpecChars(insertQuery) should be(replaceSpecChars(expectedInsertQuery))
  }
}
