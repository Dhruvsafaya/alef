package com.alefeducation.warehouse.tutor

import com.alefeducation.warehouse.core.CommonTransformer

object TutorChallengeQuestionAnswerTransformer extends CommonTransformer {
  private val mainColNames: List[String] = List(
      "ftcqa_bot_question_timestamp",
      "ftcqa_bot_question_source",
      "ftcqa_bot_question_id",
      "ftcqa_bot_question_tokens",
      "ftcqa_conversation_id",
      "ftcqa_session_id",
      "ftcqa_message_id",
      "ftcqa_user_id",
      "ftcqa_tenant_id",
      "ftcqa_date_dw_id",
      "ftcqa_created_time",
      "ftcqa_dw_created_time",
      "ftcqa_is_answer_evaluated",
      "ftcqa_dw_id",
      "ftcqa_user_attempt_tokens",
      "ftcqa_user_attempt_number",
      "ftcqa_user_remaining_attempts",
      "ftcqa_user_attempt_timestamp",
      "ftcqa_user_attempt_is_correct",
      "ftcqa_user_attempt_source",
      "ftcqa_user_attempt_id",
      "ftcqa_user_dw_id"
  )
  override def getSelectQuery(schema: String): String =  s"""
                                                            |SELECT
                                                            |     ${getPkColumn()}
                                                            |${fromStatement(schema)}
                                                            |ORDER BY ${getPkColumn()}
                                                            |LIMIT $QUERY_LIMIT
                                                            |""".stripMargin

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idsInStatement = ids.mkString(",")
    log.info(idsInStatement)
    val insCols = mainColNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColNames(mainColNames)
    s"""
       |INSERT INTO $schema.fact_tutor_challenge_question_answer ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE stcqa.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "ftcqa_dw_id"

  override def getStagingTableName(): String = "staging_tutor_challenge_question_answer"

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "ftcqa_user_dw_id"  => s"\tu.user_dw_id AS ftcqa_user_dw_id"
        case col => s"\t$col"
      }.mkString(",\n")

  private def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.staging_tutor_challenge_question_answer stcqa
       |JOIN ${schema}_stage.rel_user u ON u.user_id = stcqa.ftcqa_user_id
       |""".stripMargin
}