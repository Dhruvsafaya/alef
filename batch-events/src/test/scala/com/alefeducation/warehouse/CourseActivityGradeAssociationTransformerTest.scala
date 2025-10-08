package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CourseActivityGradeAssociationTransformerTest extends AnyFunSuite with Matchers {

  private val transformer = CourseActivityGradeAssociationTransformer
  private val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
  private val schema = "testalefdw"

  test("should prepare query") {
    val selectQuery = transformer.getSelectQuery(connection.schema)
    val expectedSelectQuery =
      s"""
         |SELECT
         |  caga_dw_id
         |FROM testalefdw_stage.rel_course_activity_grade_association caga
         |  JOIN testalefdw_stage.rel_dw_id_mappings course ON course.id = caga.caga_course_id AND course.entity_type = 'course'
         |  JOIN (
         |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
         |    FROM testalefdw.dim_learning_objective
         |    UNION
         |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
         |    FROM testalefdw.dim_interim_checkpoint
         |  ) AS activity ON activity.act_id = caga.caga_activity_id
         |ORDER BY caga_dw_id
         |LIMIT 60000
         |""".stripMargin

    transformer.getStagingTableName() should be("rel_course_activity_grade_association")
    transformer.getPkColumn() should be("caga_dw_id")

    replaceSpecChars(selectQuery) should be(replaceSpecChars(expectedSelectQuery))


    val expectedInsertQuery =
      """
        |
        |
        |INSERT INTO testalefdw.dim_course_activity_grade_association (
        |	caga_dw_id,
        |   caga_created_time,
        |   caga_updated_time,
        |   caga_deleted_time,
        |   caga_dw_created_time,
        |   caga_dw_updated_time,
        |   caga_dw_deleted_time,
        |   caga_status,
        |   caga_course_id,
        |   caga_activity_id,
        |   caga_grade_id,
        |   caga_activity_dw_id,
        |   caga_course_dw_id
        |)
        |
        |SELECT
        |	caga_dw_id,
        |   caga_created_time,
        |   caga_updated_time,
        |   caga_deleted_time,
        |   caga_dw_created_time,
        |   caga_dw_updated_time,
        |   caga_dw_deleted_time,
        |   caga_status,
        |   caga_course_id,
        |   caga_activity_id,
        |   caga_grade_id,
        |   activity.act_dw_id AS caga_activity_dw_id,
        |   course.dw_id AS caga_course_dw_id
        |FROM testalefdw_stage.rel_course_activity_grade_association caga
        |  JOIN testalefdw_stage.rel_dw_id_mappings course ON course.id = caga.caga_course_id AND course.entity_type = 'course'
        |  JOIN (
        |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
        |    FROM testalefdw.dim_learning_objective
        |    UNION
        |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
        |    FROM testalefdw.dim_interim_checkpoint
        |  ) AS activity ON activity.act_id = caga.caga_activity_id
        |WHERE caga_dw_id IN (3003,3004,3005)
        |""".stripMargin

    val insertQuery = transformer.getInsertFromSelectQuery(schema, List(3003, 3004, 3005))

    replaceSpecChars(insertQuery) should be(replaceSpecChars(expectedInsertQuery))
  }
}
