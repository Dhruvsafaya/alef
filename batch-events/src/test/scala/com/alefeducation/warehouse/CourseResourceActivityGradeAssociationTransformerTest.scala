package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CourseResourceActivityGradeAssociationTransformerTest extends AnyFunSuite with Matchers  {
  private val transformer = CourseResourceActivityGradeAssociationTransformer
  private val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
  private val schema = "testalefdw"

  test("should prepare query") {
    val selectQuery = transformer.getSelectQuery(connection.schema)
    val expectedSelectQuery =
      s"""
         |SELECT
         |  craga_dw_id
         |FROM testalefdw_stage.rel_course_resource_activity_grade_association caga
         |  JOIN testalefdw_stage.rel_dw_id_mappings course ON course.id = caga.craga_course_id AND course.entity_type = 'course'
         |  LEFT JOIN (
         |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
         |    FROM testalefdw.dim_learning_objective
         |    UNION
         |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
         |    FROM testalefdw.dim_interim_checkpoint
         |  ) AS activity ON activity.act_id = caga.craga_activity_id
         |ORDER BY craga_dw_id
         |LIMIT 60000
         |""".stripMargin

    transformer.getStagingTableName() should be("rel_course_resource_activity_grade_association")
    transformer.getPkColumn() should be("craga_dw_id")

    replaceSpecChars(selectQuery) should be(replaceSpecChars(expectedSelectQuery))


    val expectedInsertQuery =
      """
        |
        |
        |INSERT INTO testalefdw.dim_course_resource_activity_grade_association (
        |	craga_dw_id,
        |   craga_created_time,
        |   craga_updated_time,
        |   craga_deleted_time,
        |   craga_dw_created_time,
        |   craga_dw_updated_time,
        |   craga_dw_deleted_time,
        |   craga_status,
        |   craga_course_id,
        |   craga_activity_id,
        |   craga_grade_id,
        |   craga_activity_dw_id,
        |   craga_course_dw_id
        |)
        |
        |SELECT
        |	craga_dw_id,
        |   craga_created_time,
        |   craga_updated_time,
        |   craga_deleted_time,
        |   craga_dw_created_time,
        |   craga_dw_updated_time,
        |   craga_dw_deleted_time,
        |   craga_status,
        |   craga_course_id,
        |   craga_activity_id,
        |   craga_grade_id,
        |   activity.act_dw_id AS craga_activity_dw_id,
        |   course.dw_id AS craga_course_dw_id
        |FROM testalefdw_stage.rel_course_resource_activity_grade_association caga
        |  JOIN testalefdw_stage.rel_dw_id_mappings course ON course.id = caga.craga_course_id AND course.entity_type = 'course'
        |  LEFT JOIN (
        |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
        |    FROM testalefdw.dim_learning_objective
        |    UNION
        |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
        |    FROM testalefdw.dim_interim_checkpoint
        |  ) AS activity ON activity.act_id = caga.craga_activity_id
        |WHERE craga_dw_id IN (3003,3004,3005)
        |""".stripMargin

    val insertQuery = transformer.getInsertFromSelectQuery(schema, List(3003, 3004, 3005))

    replaceSpecChars(insertQuery) should be(replaceSpecChars(expectedInsertQuery))
  }
}
