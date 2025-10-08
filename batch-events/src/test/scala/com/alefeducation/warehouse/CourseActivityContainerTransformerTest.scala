package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class CourseActivityContainerTransformerTest extends AnyFunSuite with Matchers {

  val transformer = CourseActivityContainerTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")
    val expectedQuery =
      s"""
         |SELECT
         |   pl.rel_course_activity_container_dw_id
         |
         |FROM alefdw_stage.rel_course_activity_container pl
         | JOIN alefdw_stage.rel_dw_id_mappings dwid on pl.course_activity_container_id = dwid.id
         |WHERE dwid.entity_type = 'course_activity_container'
         |
         |ORDER BY pl.rel_course_activity_container_dw_id
         |LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert from select query") {

    val query = transformer.getInsertFromSelectQuery("alefdw", List(1001, 1002, 1003))

    val expectedQuery =
      """
        |INSERT INTO alefdw.dim_course_activity_container (
        |	course_activity_container_is_accelerated,
        |	course_activity_container_pacing,
        |	course_activity_container_sequence,
        |	course_activity_container_course_version,
        |	course_activity_container_attach_status,
        |	course_activity_container_id,
        |	course_activity_container_dw_id,
        |	course_activity_container_longname,
        |	course_activity_container_updated_time,
        |	course_activity_container_dw_created_time,
        |	course_activity_container_title,
        |	course_activity_container_created_time,
        |	course_activity_container_course_id,
        |	course_activity_container_domain,
        |	course_activity_container_status,
        |	course_activity_container_dw_updated_time,
        |	course_activity_container_index,
        |	rel_course_activity_container_dw_id
        |)
        |SELECT course_activity_container_is_accelerated,
        |	course_activity_container_pacing,
        |	course_activity_container_sequence,
        |	course_activity_container_course_version,
        |	course_activity_container_attach_status,
        |	course_activity_container_id,
        |	dw_id as course_activity_container_dw_id,
        |	course_activity_container_longname,
        |	course_activity_container_updated_time,
        |	course_activity_container_dw_created_time,
        |	course_activity_container_title,
        |	course_activity_container_created_time,
        |	course_activity_container_course_id,
        |	course_activity_container_domain,
        |	course_activity_container_status,
        |	course_activity_container_dw_updated_time,
        |	course_activity_container_index,
        |	rel_course_activity_container_dw_id
        |
        |FROM alefdw_stage.rel_course_activity_container pl
        | JOIN alefdw_stage.rel_dw_id_mappings dwid on pl.course_activity_container_id = dwid.id
        |WHERE dwid.entity_type = 'course_activity_container'
        |
        |  AND pl.rel_course_activity_container_dw_id IN (1001,1002,1003)
        |""".stripMargin

    println(query)
    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
