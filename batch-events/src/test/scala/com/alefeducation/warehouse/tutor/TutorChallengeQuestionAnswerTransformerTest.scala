package com.alefeducation.warehouse.tutor

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TutorChallengeQuestionAnswerTransformerTest extends AnyFunSuite with Matchers  {
  val transformer = TutorChallengeQuestionAnswerTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")
    val expectedQuery =
      s"""SELECT ftcqa_dw_id FROM alefdw_stage.staging_tutor_challenge_question_answer stcqa JOIN alefdw_stage.rel_user u ON u.user_id = stcqa.ftcqa_user_id ORDER BY ftcqa_dw_id LIMIT 60000""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(1001, 1002, 1003))
    val expectedQuery =
      s"""INSERT INTO alefdw.fact_tutor_challenge_question_answer ( ftcqa_bot_question_timestamp, ftcqa_bot_question_source, ftcqa_bot_question_id, ftcqa_bot_question_tokens, ftcqa_conversation_id, ftcqa_session_id, ftcqa_message_id, ftcqa_user_id, ftcqa_tenant_id, ftcqa_date_dw_id, ftcqa_created_time, ftcqa_dw_created_time, ftcqa_is_answer_evaluated, ftcqa_dw_id, ftcqa_user_attempt_tokens, ftcqa_user_attempt_number, ftcqa_user_remaining_attempts, ftcqa_user_attempt_timestamp, ftcqa_user_attempt_is_correct, ftcqa_user_attempt_source, ftcqa_user_attempt_id, ftcqa_user_dw_id ) SELECT ftcqa_bot_question_timestamp, ftcqa_bot_question_source, ftcqa_bot_question_id, ftcqa_bot_question_tokens, ftcqa_conversation_id, ftcqa_session_id, ftcqa_message_id, ftcqa_user_id, ftcqa_tenant_id, ftcqa_date_dw_id, ftcqa_created_time, ftcqa_dw_created_time, ftcqa_is_answer_evaluated, ftcqa_dw_id, ftcqa_user_attempt_tokens, ftcqa_user_attempt_number, ftcqa_user_remaining_attempts, ftcqa_user_attempt_timestamp, ftcqa_user_attempt_is_correct, ftcqa_user_attempt_source, ftcqa_user_attempt_id, u.user_dw_id AS ftcqa_user_dw_id FROM alefdw_stage.staging_tutor_challenge_question_answer stcqa JOIN alefdw_stage.rel_user u ON u.user_id = stcqa.ftcqa_user_id WHERE stcqa.ftcqa_dw_id IN (1001,1002,1003)""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
