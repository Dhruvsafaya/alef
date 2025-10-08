package com.alefeducation.facts.tutor

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.tutor.TutorSuggestionsTransform.suggestionssourceName
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class TutorSuggestionsTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "fts_dw_id",
    "fts_message_id",
    "fts_suggestion_id",
    "fts_user_id",
    "fts_session_id",
    "fts_conversation_id",
    "fts_response_time",
    "fts_success_parser_tokens",
    "fts_failure_parser_tokens",
    "fts_suggestion_question",
    "fts_suggestion_clicked",
    "fts_created_time",
    "fts_date_dw_id",
    "fts_dw_created_time",
    "fts_tenant_id",
    "eventdate"
  )

  test("should transform tutor suggestions event successfully") {
    val value = """
                  |[
                  |	{
                  |		"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
                  |		"eventType":"ChatSuggestionsGenerated",
                  |		"loadtime":"2023-05-17T05:39:28.481Z",
                  |		"userId": "bbacf0ad-6cf2-4a12-a6bc-be8494c8a733",
                  |		"sessionId": "57e45c68-52d6-4a38-a36a-bd6467cf767e",
                  |		"conversationId": "0ce0582f-574b-4d10-9608-1031d5841d3d",
                  |		"messageId": "6458a99d-5a6d-4969-8f58-65cdd0310517",
                  |		"responseTime": 4.21193,
                  |		"successParserTokens": 32,
                  |		"failureParserTokens": 0,
                  |		"suggestions": [
                  |			{
                  |				"suggestionId": "73d1e751-85df-40da-8c23-ab817e5c6aeb",
                  |				"suggestionQuestion": "What is the purpose of the test?",
                  |				"suggestionClicked": false
                  |			},
                  |			{
                  |				"suggestionId": "0951f91d-e7e1-44fd-a729-1b73f6a5f207",
                  |				"suggestionQuestion": "What topics will be covered in the test?",
                  |				"suggestionClicked": false
                  |			},
                  |			{
                  |				"suggestionId": "1fb34889-c710-4b8d-98ec-7adbb8a70901",
                  |				"suggestionQuestion": "How long will the test be?",
                  |				"suggestionClicked": false
                  |			}
                  |		],
                  |		"occurredOn": "2023-09-26T08:36:24.646Z"
                  |	}
                  |]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._

    val transformer = new TutorSuggestionsTransform(sprk, service)

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(suggestionssourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "fts_message_id", "6458a99d-5a6d-4969-8f58-65cdd0310517")
    assert[String](df, "fts_suggestion_id", "73d1e751-85df-40da-8c23-ab817e5c6aeb")
    assert[String](df, "fts_user_id", "bbacf0ad-6cf2-4a12-a6bc-be8494c8a733")
    assert[String](df, "fts_session_id", "57e45c68-52d6-4a38-a36a-bd6467cf767e")
    assert[String](df, "fts_conversation_id", "0ce0582f-574b-4d10-9608-1031d5841d3d")
    assert[Double](df, "fts_response_time", 4.21193)
    assert[Int](df, "fts_success_parser_tokens", 32)
    assert[Int](df, "fts_failure_parser_tokens", 0)
    assert[String](df, "fts_suggestion_question", "What is the purpose of the test?")
    assert[Boolean](df, "fts_suggestion_clicked", false)
    assert[String](df, "fts_date_dw_id", "20230926")
    assert[String](df, "fts_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
  }
}
