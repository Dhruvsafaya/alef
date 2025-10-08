package com.alefeducation.warehouse.tutor

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class TutorAnalogousTransformerTest extends AnyFunSuite with Matchers  {
  val transformer = TutorAnalogousTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")
    val expectedQuery =
      s"""SELECT fta_dw_id FROM alefdw_stage.staging_tutor_analogous stcqa JOIN alefdw_stage.rel_user u ON u.user_id = stcqa.fta_user_id ORDER BY fta_dw_id LIMIT 60000""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(1001, 1002, 1003))
    val expectedQuery =
      s"""INSERT INTO alefdw.fact_tutor_analogous ( fta_dw_id, fta_created_time, fta_dw_created_time, fta_date_dw_id, fta_user_id, fta_user_dw_id, fta_tenant_id, fta_message_id, fta_session_id, fta_conversation_id ) SELECT fta_dw_id, fta_created_time, fta_dw_created_time, fta_date_dw_id, fta_user_id, u.user_dw_id AS fta_user_dw_id, fta_tenant_id, fta_message_id, fta_session_id, fta_conversation_id FROM alefdw_stage.staging_tutor_analogous stcqa JOIN alefdw_stage.rel_user u ON u.user_id = stcqa.fta_user_id WHERE stcqa.fta_dw_id IN (1001,1002,1003)""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}