package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CourseActivityContainerDomainTransformerTest extends AnyFunSuite with Matchers {
  val transformer = CourseActivityContainerDomainTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")
    val expectedQuery =
      s"""SELECT staging.cacd_dw_id FROM alefdw_stage.rel_course_activity_container_domain staging JOIN alefdw_stage.rel_dw_id_mappings dwid ON staging.cacd_container_id = dwid.id AND dwid.entity_type = 'course_activity_container' JOIN alefdw_stage.rel_dw_id_mappings cdwid ON staging.cacd_course_id = cdwid.id AND cdwid.entity_type = 'course' ORDER BY staging.cacd_dw_id LIMIT 60000""".stripMargin
    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert from select query") {

    val query = transformer.getInsertFromSelectQuery("alefdw", List(1001, 1002, 1003))

    val expectedQuery =
      """INSERT INTO alefdw.dim_course_activity_container_domain ( cacd_dw_id, cacd_created_time, cacd_dw_created_time, cacd_updated_time, cacd_dw_updated_time, cacd_container_id, cacd_container_dw_id, cacd_course_id, cacd_course_dw_id, cacd_domain, cacd_sequence, cacd_status ) SELECT cacd_dw_id, cacd_created_time, cacd_dw_created_time, cacd_updated_time, cacd_dw_updated_time, cacd_container_id, dwid.dw_id AS cacd_container_dw_id, cacd_course_id, cdwid.dw_id AS cacd_course_dw_id, cacd_domain, cacd_sequence, cacd_status FROM alefdw_stage.rel_course_activity_container_domain staging JOIN alefdw_stage.rel_dw_id_mappings dwid ON staging.cacd_container_id = dwid.id AND dwid.entity_type = 'course_activity_container' JOIN alefdw_stage.rel_dw_id_mappings cdwid ON staging.cacd_course_id = cdwid.id AND cdwid.entity_type = 'course' AND staging.cacd_dw_id IN (1001,1002,1003)""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
