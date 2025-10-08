package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AcademicCalendarTransformerTest extends AnyFunSuite with Matchers {
  val transformer = AcademicCalendarTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")
    val expectedQuery =
      s"""SELECT staging.academic_calendar_dw_id FROM alefdw_stage.rel_academic_calendar staging LEFT OUTER JOIN alefdw_stage.rel_user cb ON cb.user_id = staging.academic_calendar_created_by_id LEFT OUTER JOIN alefdw_stage.rel_user ub ON ub.user_id = staging.academic_calendar_updated_by_id JOIN alefdw.dim_organization org ON org.organization_code = staging.academic_calendar_organization JOIN alefdw.dim_academic_year ay ON ay.academic_year_id = staging.academic_calendar_academic_year_id and ay.academic_year_type = staging.academic_calendar_type LEFT OUTER JOIN alefdw.dim_school ON dim_school.school_id = staging.academic_calendar_school_id ORDER BY staging.academic_calendar_dw_id LIMIT 60000""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert from select query") {

    val query = transformer.getInsertFromSelectQuery("alefdw", List(1001, 1002, 1003))

    val expectedQuery = "INSERT INTO alefdw.dim_academic_calendar ( academic_calendar_dw_id, academic_calendar_created_time, academic_calendar_updated_time, academic_calendar_deleted_time, academic_calendar_dw_created_time, academic_calendar_dw_updated_time, academic_calendar_status, academic_calendar_title, academic_calendar_id, academic_calendar_school_id, academic_calendar_school_dw_id, academic_calendar_is_default, academic_calendar_type, academic_calendar_academic_year_id, academic_calendar_academic_year_dw_id, academic_calendar_organization, academic_calendar_tenant_id, academic_calendar_organization_dw_id, academic_calendar_created_by_id, academic_calendar_created_by_dw_id, academic_calendar_updated_by_id, academic_calendar_updated_by_dw_id ) SELECT academic_calendar_dw_id, academic_calendar_created_time, academic_calendar_updated_time, academic_calendar_deleted_time, academic_calendar_dw_created_time, academic_calendar_dw_updated_time, academic_calendar_status, academic_calendar_title, academic_calendar_id, academic_calendar_school_id, dim_school.school_dw_id AS academic_calendar_school_dw_id, academic_calendar_is_default, academic_calendar_type, academic_calendar_academic_year_id, ay.academic_year_dw_id AS academic_calendar_academic_year_dw_id, academic_calendar_organization, academic_calendar_tenant_id, org.organization_dw_id AS academic_calendar_organization_dw_id, academic_calendar_created_by_id, cb.user_dw_id AS academic_calendar_created_by_dw_id, academic_calendar_updated_by_id, ub.user_dw_id AS academic_calendar_updated_by_dw_id FROM alefdw_stage.rel_academic_calendar staging LEFT OUTER JOIN alefdw_stage.rel_user cb ON cb.user_id = staging.academic_calendar_created_by_id LEFT OUTER JOIN alefdw_stage.rel_user ub ON ub.user_id = staging.academic_calendar_updated_by_id JOIN alefdw.dim_organization org ON org.organization_code = staging.academic_calendar_organization JOIN alefdw.dim_academic_year ay ON ay.academic_year_id = staging.academic_calendar_academic_year_id and ay.academic_year_type = staging.academic_calendar_type LEFT OUTER JOIN alefdw.dim_school ON dim_school.school_id = staging.academic_calendar_school_id WHERE staging.academic_calendar_dw_id IN (1001,1002,1003)"

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

}
