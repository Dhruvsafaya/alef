package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class TeacherTestCandidateSessionTransformerTest extends AnyFunSuite with Matchers  {
  val transformer = TeacherTestCandidateSessionTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")
    val expectedQuery =
      s"""SELECT fttcp_dw_id FROM alefdw_stage.staging_teacher_test_candidate_progress stcqa JOIN alefdw_stage.rel_user u ON u.user_id = stcqa.fttcp_candidate_id JOIN alefdw.dim_teacher_test_delivery_settings d ON d.ttds_test_delivery_id = stcqa.fttcp_test_delivery_id ORDER BY fttcp_dw_id LIMIT 60000""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(1001, 1002, 1003))
    val expectedQuery =
      s"""INSERT INTO alefdw.fact_teacher_test_candidate_progress ( fttcp_dw_id, fttcp_session_id, fttcp_candidate_id, fttcp_candidate_dw_id, fttcp_test_delivery_id, fttcp_assessment_id, fttcp_test_delivery_dw_id, fttcp_score, fttcp_stars_awarded, fttcp_status, fttcp_updated_at, fttcp_created_at, fttcp_date_dw_id, fttcp_created_time, fttcp_dw_created_time ) SELECT fttcp_dw_id, fttcp_session_id, fttcp_candidate_id, u.user_dw_id AS fttcp_candidate_dw_id, fttcp_test_delivery_id, fttcp_assessment_id, d.ttds_dw_id AS fttcp_test_delivery_dw_id, fttcp_score, fttcp_stars_awarded, fttcp_status, fttcp_updated_at, fttcp_created_at, fttcp_date_dw_id, fttcp_created_time, fttcp_dw_created_time FROM alefdw_stage.staging_teacher_test_candidate_progress stcqa JOIN alefdw_stage.rel_user u ON u.user_id = stcqa.fttcp_candidate_id JOIN alefdw.dim_teacher_test_delivery_settings d ON d.ttds_test_delivery_id = stcqa.fttcp_test_delivery_id WHERE stcqa.fttcp_dw_id IN (1001,1002,1003)""".stripMargin
    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
