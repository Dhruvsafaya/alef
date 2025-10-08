package com.alefeducation.warehouse.tutor

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TutorSimplificationTransformerTest extends AnyFunSuite with Matchers  {
  val transformer = TutorSimplificationTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")
    val expectedQuery =
      s"""SELECT fts_dw_id FROM alefdw_stage.staging_tutor_simplification stcqa JOIN alefdw_stage.rel_user u ON u.user_id = stcqa.fts_user_id ORDER BY fts_dw_id LIMIT 60000""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(1001, 1002, 1003))
    val expectedQuery =
      s"""INSERT INTO alefdw.fact_tutor_simplification ( fts_dw_id, fts_created_time, fts_dw_created_time, fts_date_dw_id, fts_user_id, fts_user_dw_id, fts_tenant_id, fts_message_id, fts_session_id, fts_conversation_id ) SELECT fts_dw_id, fts_created_time, fts_dw_created_time, fts_date_dw_id, fts_user_id, u.user_dw_id AS fts_user_dw_id, fts_tenant_id, fts_message_id, fts_session_id, fts_conversation_id FROM alefdw_stage.staging_tutor_simplification stcqa JOIN alefdw_stage.rel_user u ON u.user_id = stcqa.fts_user_id WHERE stcqa.fts_dw_id IN (1001,1002,1003)""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}