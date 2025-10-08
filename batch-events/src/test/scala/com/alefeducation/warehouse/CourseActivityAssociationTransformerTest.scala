package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CourseActivityAssociationTransformerTest extends AnyFunSuite with Matchers {

  private val transformer = CourseActivityAssociationTransformer
  private val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
  private val schema = "testalefdw"

  test("should prepare query") {
    val selectQuery = transformer.getSelectQuery(connection.schema)
    val expectedSelectQuery =
      s"""
         |SELECT
         |  caa_dw_id
         |FROM testalefdw_stage.rel_course_activity_association caa
         |  JOIN testalefdw_stage.rel_dw_id_mappings course ON course.id = caa.caa_course_id AND course.entity_type = 'course'
         |  LEFT JOIN testalefdw_stage.rel_dw_id_mappings container ON container.id = caa.caa_container_id AND container.entity_type = 'course_activity_container'
         |  JOIN (
         |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
         |    FROM testalefdw.dim_learning_objective
         |    UNION
         |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
         |    FROM testalefdw.dim_interim_checkpoint
         |  ) AS activity ON activity.act_id = caa.caa_activity_id
         |WHERE (
         |  caa.caa_container_id IS NULL AND container.id IS NULL
         |  OR caa.caa_container_id IS NOT NULL AND container.id IS NOT NULL
         |)
         |ORDER BY caa_dw_id
         |LIMIT 60000
         |""".stripMargin

    transformer.getStagingTableName() should be("rel_course_activity_association")
    transformer.getPkColumn() should be("caa_dw_id")

    replaceSpecChars(selectQuery) should be(replaceSpecChars(expectedSelectQuery))


    val expectedInsertQuery =
      """
        |
        |
        |INSERT INTO testalefdw.dim_course_activity_association (
        |	caa_dw_id,
        |   caa_created_time,
        |   caa_updated_time,
        |   caa_deleted_time,
        |   caa_dw_created_time,
        |   caa_dw_updated_time,
        |   caa_dw_deleted_time,
        |   caa_status,
        |   caa_attach_status,
        |   caa_course_id,
        |   caa_container_id,
        |   caa_activity_id,
        |   caa_activity_type,
        |   caa_activity_pacing,
        |   caa_activity_index,
        |   caa_course_version,
        |   caa_is_parent_deleted,
        |   caa_grade,
        |   caa_activity_is_optional,
        |   caa_is_joint_parent_activity,
        |   caa_course_dw_id,
        |   caa_container_dw_id,
        |   caa_activity_dw_id
        |)
        |
        |SELECT
        |	caa_dw_id,
        |   caa_created_time,
        |   caa_updated_time,
        |   caa_deleted_time,
        |   caa_dw_created_time,
        |   caa_dw_updated_time,
        |   caa_dw_deleted_time,
        |   caa_status,
        |   caa_attach_status,
        |   caa_course_id,
        |   caa_container_id,
        |   caa_activity_id,
        |   caa_activity_type,
        |   caa_activity_pacing,
        |   caa_activity_index,
        |   caa_course_version,
        |   caa_is_parent_deleted,
        |   caa_grade,
        |   caa_activity_is_optional,
        |   caa_is_joint_parent_activity,
        |   course.dw_id AS caa_course_dw_id,
        |   container.dw_id AS caa_container_dw_id,
        |   activity.act_dw_id AS caa_activity_dw_id
        |FROM testalefdw_stage.rel_course_activity_association caa
        |  JOIN testalefdw_stage.rel_dw_id_mappings course ON course.id = caa.caa_course_id AND course.entity_type = 'course'
        |  LEFT JOIN testalefdw_stage.rel_dw_id_mappings container ON container.id = caa.caa_container_id AND container.entity_type = 'course_activity_container'
        |  JOIN (
        |    SELECT lo_dw_id as act_dw_id, lo_id as act_id
        |    FROM testalefdw.dim_learning_objective
        |    UNION
        |    SELECT ic_dw_id as act_dw_id, ic_id as act_id
        |    FROM testalefdw.dim_interim_checkpoint
        |  ) AS activity ON activity.act_id = caa.caa_activity_id
        |WHERE (
        | caa.caa_container_id IS NULL AND container.id IS NULL
        | OR caa.caa_container_id IS NOT NULL AND container.id IS NOT NULL
        |) AND caa_dw_id IN (3003,3004,3005)
        |""".stripMargin

    val insertQuery = transformer.getInsertFromSelectQuery(schema, List(3003, 3004, 3005))

    replaceSpecChars(insertQuery) should be(replaceSpecChars(expectedInsertQuery))
  }
}
