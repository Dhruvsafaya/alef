package com.alefeducation.facts

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course.transform.CourseFieldMutatedTransform
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class TutorChallengeQuestionAnswerTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct question generated DDL") {
    val challengeQuestionGeneratedSource =
      """
        |[
        |{
        |  "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
        |  "eventType": "ChallengeQuestionGenerated",
        |  "loadtime":"2023-05-17T05:39:28.481Z",
        |  "userId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
        |  "sessionId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
        |  "conversationId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
        |  "messageId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
        |  "botQuestionMessage": "string",
        |  "botQuestionTokens": 1,
        |  "botQuestionId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
        |  "botQuestionTimestamp": "2018-09-08T02:35:00.0Z",
        |  "botQuestionSource": "string",
        |  "occurredOn": "2018-09-08T02:35:00.0Z"
        |}
        |]
        """.stripMargin


    val sprk = spark
    import sprk.implicits._

    val challengeQuestionGeneratedDf = spark.read.json(Seq(challengeQuestionGeneratedSource).toDS())

    when(service.readOptional("parquet-tutor-challenge-question-source", sprk, extraProps = List(("mergeSchema", "true"))))
      .thenReturn(Some(challengeQuestionGeneratedDf))

    val transformer = new TutorChallengeQuestionAnswer(sprk, service, "tutor-challenge-question-transform")
    val sinks = transformer.transform()

    val df = sinks.filter(_.name == "transformed-tutor-challenge-question").head.output

    assert(df.count() == 1)
    val expectedColumns = Set(
      "ftcqa_dw_id",
      "ftcqa_bot_question_timestamp",
      "ftcqa_is_answer_evaluated",
      "ftcqa_tenant_id",
      "ftcqa_dw_created_time",
      "ftcqa_bot_question_message",
      "ftcqa_bot_question_source",
      "eventdate",
      "ftcqa_message_id",
      "ftcqa_conversation_id",
      "ftcqa_created_time",
      "ftcqa_bot_question_id",
      "ftcqa_session_id",
      "ftcqa_bot_question_tokens",
      "ftcqa_user_id",
      "ftcqa_date_dw_id"
    )
    assert(df.columns.toSet === expectedColumns)
  }

  test("should construct question evaluated DDL") {

        val challengeQuestionEvaluatedSource =
          """
            |[
            |{
            |  "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
            |  "eventType": "ChallengeQuestionEvaluated",
            |  "loadtime":"2023-05-17T05:39:28.481Z",
            |  "userId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
            |  "sessionId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
            |  "conversationId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
            |  "messageId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
            |  "botQuestionId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
            |  "userAttemptMessage": "string",
            |  "userAttemptTokens": 1,
            |  "userAttemptId": 2,
            |  "userAttemptTimestamp": "2018-09-08T02:35:00.0Z",
            |  "userAttemptSource": "string",
            |  "userAttemptNumber": 3,
            |  "userRemainingAttempts": 4,
            |  "userAttemptIsCorrect": true,
            |  "occurredOn": "2018-09-08T02:35:00.0Z"
            |}
            |]
            """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val challengeQuestionEvaluatedDf = spark.read.json(Seq(challengeQuestionEvaluatedSource).toDS())
    when(service.readOptional("parquet-tutor-challenge-answer-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(challengeQuestionEvaluatedDf))

    val transformer = new TutorChallengeQuestionAnswer(sprk, service, "tutor-challenge-answer-transform")
    val sinks = transformer.transform()

    val df = sinks.filter(_.name == "transformed-tutor-challenge-answer").head.output

    assert(df.count() == 1)
    val expectedColumns = Set("ftcqa_dw_id",
      "ftcqa_is_answer_evaluated",
      "ftcqa_user_attempt_source",
      "ftcqa_user_attempt_id",
      "ftcqa_tenant_id",
      "ftcqa_user_attempt_number",
      "ftcqa_user_attempt_is_correct",
      "ftcqa_dw_created_time",
      "ftcqa_user_attempt_timestamp",
      "eventdate",
      "ftcqa_message_id",
      "ftcqa_user_attempt_message",
      "ftcqa_user_attempt_tokens",
      "ftcqa_conversation_id",
      "ftcqa_created_time",
      "ftcqa_user_remaining_attempts",
      "ftcqa_bot_question_id",
      "ftcqa_session_id",
      "ftcqa_user_id",
      "ftcqa_date_dw_id"
    )
    assert(df.columns.toSet === expectedColumns)
  }
}

