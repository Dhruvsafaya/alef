package com.alefeducation.warehouse.tutor

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TutorTranslationTransformerTest extends AnyFunSuite with Matchers  {
  val transformer = TutorTranslationTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")
    val expectedQuery =
      s"""SELECT ftt_dw_id FROM alefdw_stage.staging_tutor_translation stcqa JOIN alefdw_stage.rel_user u ON u.user_id = stcqa.ftt_user_id ORDER BY ftt_dw_id LIMIT 60000""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(1001, 1002, 1003))
    val expectedQuery =
      s"""INSERT INTO alefdw.fact_tutor_translation ( ftt_dw_id, ftt_created_time, ftt_dw_created_time, ftt_date_dw_id, ftt_user_id, ftt_user_dw_id, ftt_tenant_id, ftt_message_id, ftt_session_id, ftt_conversation_id, ftt_translation_language ) SELECT ftt_dw_id, ftt_created_time, ftt_dw_created_time, ftt_date_dw_id, ftt_user_id, u.user_dw_id AS ftt_user_dw_id, ftt_tenant_id, ftt_message_id, ftt_session_id, ftt_conversation_id, ftt_translation_language FROM alefdw_stage.staging_tutor_translation stcqa JOIN alefdw_stage.rel_user u ON u.user_id = stcqa.ftt_user_id WHERE stcqa.ftt_dw_id IN (1001,1002,1003)""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}