package com.alefeducation.facts.tutor

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.tutor.TutorConversationTransform.ParquetTutorConversationSource
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class TutorConversationTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "eventdate",
    "ftc_date_dw_id",
    "ftc_created_time",
    "ftc_dw_created_time",
    "ftc_user_id",
    "ftc_role",
    "ftc_context_id",
    "ftc_school_id",
    "ftc_grade_id",
    "ftc_grade",
    "ftc_subject_id",
    "ftc_subject",
    "ftc_language",
    "ftc_material_id",
    "ftc_material_type",
    "ftc_activity_id",
    "ftc_activity_status",
    "ftc_level_id",
    "ftc_outcome_id",
    "ftc_tenant_id",
    "ftc_session_id",
    "ftc_session_state",
    "ftc_conversation_id",
    "ftc_conversation_max_tokens",
    "ftc_conversation_token_count",
    "ftc_system_prompt",
    "ftc_system_prompt_tokens",
    "ftc_message_id",
    "ftc_message_language",
    "ftc_message_feedback",
    "ftc_message_tokens",
    "ftc_user_message",
    "ftc_user_message_source",
    "ftc_user_message_tokens",
    "ftc_user_message_timestamp",
    "ftc_bot_message",
    "ftc_bot_message_source",
    "ftc_bot_message_tokens",
    "ftc_bot_message_timestamp",
    "ftc_bot_message_confidence",
    "ftc_bot_message_response_time" ,
    "ftc_suggestions_prompt",
    "ftc_suggestions_prompt_tokens",
    "ftc_activity_page_context_id",
    "ftc_student_location",
    "ftc_suggestion_clicked",
    "ftc_clicked_suggestion_id"
  )

  test("should transform tutor conversation event successfully") {
    val value = """
                  |[
                  |{
                  |"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
                  |"eventType":"ChatConversationOccurred",
                  |"loadtime":"2023-05-17T05:39:28.481Z",
                  |"role":"student",
                  |"grade":3,
                  |"subject":"Math",
                  |"language":"english",
                  |"schoolId": "457ae0bc-30b7-4ea9-b97b-7d6221dedbdf",
                  |"gradeId": "e73fa736-59ca-42df-ade6-87558c7df8c2",
                  |"subjectId": "1223448",
                  |"contextId": "df840323-9ba3-4ffa-a279-464e010fcdd0",
                  |"userId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
                  |"occurredOn": "2023-04-27 10:01:38.373481108",
                  |"sessionId": "s57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"materialId": "m57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"materialType": "pathways",
                  |"activityId": "a57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"activityStatus": "in_progress",
                  |"levelId": "l57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"outcomeId": "o57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"conversationMaxTokens": 0,
                  |"conversationTokenCount": 0,
                  |"systemPrompt": "prompt",
                  |"systemPromptTokens": 0,
                  |"messageLanguage": "english",
                  |"messageTokens": 0,
                  |"messageFeedback": "unclicked",
                  |"userMessage": "message",
                  |"userMessageSource": "default",
                  |"userMessageTokens": 0,
                  |"userMessageTimestamp": "0000-000-00T00:00:00",
                  |"botMessage": "string",
                  |"botMessageSource": "user",
                  |"botMessageTokens": 0,
                  |"botMessageTimestamp": "0000-000-00T00:00:00",
                  |"botMessageConfidence": 0.0,
                  |"botMessageResponseTime": 0.0,
                  |"eventDateDw": "20230427",
                  |"studentLocation": "school",
                  |"suggestionClicked": true,
                  |"clickedSuggestionId": "157ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"activityPageContextId": "af840323-9ba3-4ffa-a279-464e010fcdd0",
                  |"conversationId": "bf840323-9ba3-4ffa-a279-464e010fcdd0",
                  |"messageId": "bf27619a-3695-40f1-87f2-55ebb18629d4",
                  |"suggestionsPrompt": "You are a helpful question generator",
                  |"suggestionsPromptTokens": 0
                  |}
                  |]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._

    val transformer = new TutorConversationTransform(sprk, service)

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetTutorConversationSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assertTutorConversationalFields(df)
  }

  test("should transform tutor conversation with sessionStatus event successfully") {
    val value = """
                  |[
                  |{
                  |"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
                  |"eventType":"ChatConversationOccurred",
                  |"loadtime":"2023-05-17T05:39:28.481Z",
                  |"role":"student",
                  |"grade":3,
                  |"subject":"Math",
                  |"language":"english",
                  |"schoolId": "457ae0bc-30b7-4ea9-b97b-7d6221dedbdf",
                  |"gradeId": "e73fa736-59ca-42df-ade6-87558c7df8c2",
                  |"subjectId": "1223448",
                  |"contextId": "df840323-9ba3-4ffa-a279-464e010fcdd0",
                  |"userId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
                  |"occurredOn": "2023-04-27 10:01:38.373481108",
                  |"sessionId": "s57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"sessionStatus": "in_progress",
                  |"materialId": "m57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"materialType": "pathways",
                  |"activityId": "a57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"activityStatus": "in_progress",
                  |"levelId": "l57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"outcomeId": "o57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"conversationMaxTokens": 0,
                  |"conversationTokenCount": 0,
                  |"systemPrompt": "prompt",
                  |"systemPromptTokens": 0,
                  |"messageLanguage": "english",
                  |"messageTokens": 0,
                  |"messageFeedback": "unclicked",
                  |"userMessage": "message",
                  |"userMessageSource": "default",
                  |"userMessageTokens": 0,
                  |"userMessageTimestamp": "0000-000-00T00:00:00",
                  |"botMessage": "string",
                  |"botMessageSource": "user",
                  |"botMessageTokens": 0,
                  |"botMessageTimestamp": "0000-000-00T00:00:00",
                  |"botMessageConfidence": 0.0,
                  |"botMessageResponseTime": 0.0,
                  |"eventDateDw": "20230427",
                  |"studentLocation": "school",
                  |"suggestionClicked": true,
                  |"clickedSuggestionId": "157ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"activityPageContextId": "af840323-9ba3-4ffa-a279-464e010fcdd0",
                  |"conversationId": "bf840323-9ba3-4ffa-a279-464e010fcdd0",
                  |"messageId": "bf27619a-3695-40f1-87f2-55ebb18629d4",
                  |"suggestionsPrompt": "You are a helpful question generator",
                  |"suggestionsPromptTokens": 0
                  |}
                  |]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._

    val transformer = new TutorConversationTransform(sprk, service)

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetTutorConversationSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assertTutorConversationalFields(df)
    assert[Int](df, "ftc_session_state", 2)
  }

  private def assertTutorConversationalFields(df: DataFrame): Unit = {
    assert[String](df, "ftc_role", "student")
    assert[String](df, "ftc_context_id", "df840323-9ba3-4ffa-a279-464e010fcdd0")
    assert[String](df, "ftc_school_id", "457ae0bc-30b7-4ea9-b97b-7d6221dedbdf")
    assert[String](df, "ftc_grade_id", "e73fa736-59ca-42df-ade6-87558c7df8c2")
    assert[String](df, "eventdate", "2023-04-27")
    assert[String](df, "ftc_created_time", "2023-04-27 10:01:38.373481")
    assert[Int](df, "ftc_grade", 3)
    assert[Long](df, "ftc_subject_id", 1223448)
    assert[String](df, "ftc_subject", "Math")
    assert[String](df, "ftc_language", "english")
    assert[String](df, "ftc_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](df, "ftc_user_id", "fd605223-dbe9-426e-a8f4-67c76d6357c1")
    assert[String](df, "ftc_activity_status", "in_progress")
  }
}
