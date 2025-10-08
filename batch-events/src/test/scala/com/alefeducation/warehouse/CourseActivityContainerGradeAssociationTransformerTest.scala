package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CourseActivityContainerGradeAssociationTransformerTest extends AnyFunSuite with Matchers {

  val transformer = CourseActivityContainerGradeAssociationTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")
    val expectedQuery =
      s"""
         |SELECT
         |   staging.cacga_dw_id
         |FROM alefdw_stage.rel_course_activity_container_grade_association staging
         | JOIN alefdw_stage.rel_dw_id_mappings dwid ON staging.cacga_container_id = dwid.id AND
         |   dwid.entity_type = 'course_activity_container'
         | JOIN alefdw_stage.rel_dw_id_mappings cdwid ON staging.cacga_course_id = cdwid.id AND
         |   cdwid.entity_type = 'course'
         |
         |ORDER BY staging.cacga_dw_id
         |LIMIT 60000
         |""".stripMargin
    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert from select query") {

    val query = transformer.getInsertFromSelectQuery("alefdw", List(1001, 1002, 1003))

    val expectedQuery =
      """
        |INSERT INTO alefdw.dim_course_activity_container_grade_association (
        |	cacga_dw_id,
        |	cacga_created_time,
        |	cacga_dw_created_time,
        |	cacga_updated_time,
        |	cacga_dw_updated_time,
        |	cacga_container_id,
        |	cacga_container_dw_id,
        |	cacga_course_id,
        |   cacga_course_dw_id,
        |	cacga_grade,
        |	cacga_status
        |)
        |SELECT 	cacga_dw_id,
        |	cacga_created_time,
        |	cacga_dw_created_time,
        |	cacga_updated_time,
        |	cacga_dw_updated_time,
        |	cacga_container_id,
        |	dwid.dw_id AS cacga_container_dw_id,
        |	cacga_course_id,
        |   cdwid.dw_id AS cacga_course_dw_id,
        |	cacga_grade,
        |	cacga_status
        |FROM alefdw_stage.rel_course_activity_container_grade_association staging
        | JOIN alefdw_stage.rel_dw_id_mappings dwid ON staging.cacga_container_id = dwid.id AND
        |   dwid.entity_type = 'course_activity_container'
        | JOIN alefdw_stage.rel_dw_id_mappings cdwid ON staging.cacga_course_id = cdwid.id AND
        |   cdwid.entity_type = 'course'
        |
        |  AND staging.cacga_dw_id IN (1001,1002,1003)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
