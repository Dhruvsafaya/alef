package com.alefeducation.warehouse.tutor

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers



class TuterOnboardTransformerTest extends AnyFunSuite with Matchers  {
  val transformer = TuterOnboardTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")
    val expectedQuery =
      s"""SELECT fto_dw_id FROM alefdw_stage.staging_tutor_onboarding stcqa JOIN alefdw_stage.rel_user u ON u.user_id = stcqa.fto_user_id ORDER BY fto_dw_id LIMIT 60000""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(1001, 1002, 1003))
    val expectedQuery =
      s"""INSERT INTO alefdw.fact_tutor_onboarding ( fto_dw_id, fto_created_time, fto_dw_created_time, fto_date_dw_id, fto_user_id, fto_tenant_id, fto_question_id, fto_question_category, fto_user_free_text_response, fto_onboarding_complete, fto_onboarding_skipped, fto_user_dw_id ) SELECT fto_dw_id, fto_created_time, fto_dw_created_time, fto_date_dw_id, fto_user_id, fto_tenant_id, fto_question_id, fto_question_category, fto_user_free_text_response, fto_onboarding_complete, fto_onboarding_skipped, u.user_dw_id AS fto_user_dw_id FROM alefdw_stage.staging_tutor_onboarding stcqa JOIN alefdw_stage.rel_user u ON u.user_id = stcqa.fto_user_id WHERE stcqa.fto_dw_id IN (1001,1002,1003)""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
