package com.alefeducation.warehouse.staff

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class StaffUserSchoolRoleAssociationTransformerTest extends AnyFunSuite with Matchers {

  val transformer: StaffUserSchoolRoleAssociationTransformer.type = StaffUserSchoolRoleAssociationTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")

    val expectedQuery =
      s"""
         |SELECT susra_dw_id
         |FROM alefdw_stage.rel_staff_user_school_role_association staging
         | JOIN alefdw_stage.rel_user u ON u.user_id = staging.susra_staff_id
         | LEFT OUTER JOIN alefdw.dim_school s ON s.school_id = staging.susra_school_id
         | JOIN alefdw.dim_role r ON r.role_uuid = staging.susra_role_uuid
         |WHERE (staging.susra_school_id IS NULL OR s.school_id IS NOT NULL)
         |ORDER BY susra_dw_id
         |LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }


  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO alefdw.dim_staff_user_school_role_association (
        |	susra_dw_id,
        |	susra_staff_id,
        |	susra_staff_dw_id,
        |	susra_school_id,
        |	susra_school_dw_id,
        |   susra_role_name,
        |	susra_role_uuid,
        |	susra_role_dw_id,
        |	susra_organization,
        |	susra_status,
        |	susra_created_time,
        |	susra_active_until,
        |	susra_dw_created_time,
        |   susra_event_type
        |)
        |SELECT 	susra_dw_id,
        |	susra_staff_id,
        |	u.user_dw_id AS susra_staff_dw_id,
        |	susra_school_id,
        |	s.school_dw_id AS susra_school_dw_id,
        |   susra_role_name,
        |	susra_role_uuid,
        |	r.role_dw_id AS susra_role_dw_id,
        |	susra_organization,
        |	susra_status,
        |	susra_created_time,
        |	susra_active_until,
        |	susra_dw_created_time,
        |   susra_event_type
        |FROM alefdw_stage.rel_staff_user_school_role_association staging
        | JOIN alefdw_stage.rel_user u ON u.user_id = staging.susra_staff_id
        | LEFT OUTER JOIN alefdw.dim_school s ON s.school_id = staging.susra_school_id
        | JOIN alefdw.dim_role r ON r.role_uuid = staging.susra_role_uuid
        |WHERE (staging.susra_school_id IS NULL OR s.school_id IS NOT NULL)
        |AND staging.susra_dw_id IN (10,12,13)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
