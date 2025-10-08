package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.schema.tutor.Suggestion
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers

class TutorEventsSpec extends SparkSuite with Matchers {

  val expectedChatConversationOccurredColumns: Set[String] = Set(
    "eventType",
    "eventDateDw",
    "loadtime",
    "occurredOn",
    "tenantId",
    "userMessage",
    "botMessageTokens",
    "userMessageSource",
    "sessionId",
    "subject",
    "systemPromptTokens",
    "role",
    "conversationMaxTokens",
    "schoolId",
    "gradeId",
    "conversationTokenCount",
    "botMessageTimestamp",
    "systemPrompt",
    "userMessageTimestamp",
    "userMessageClassification",
    "botMessageConfidence",
    "userId",
    "userMessageTokens",
    "botMessageResponseTime",
    "messageLanguage",
    "materialId",
    "levelId",
    "activityStatus",
    "language",
    "grade",
    "activityId",
    "outcomeId",
    "messageFeedback",
    "materialType",
    "messageTokens",
    "contextId",
    "botMessage",
    "botMessageSource",
    "subjectId",
    "sessionStatus",
    "clickedSuggestionId",
    "suggestionClicked",
    "studentLocation",
    "activityPageContextId",
    "suggestionsPrompt",
    "suggestionsPromptTokens",
    "messageId",
    "conversationId"
  )

  trait Setup {
    implicit val transformer = new TutorRawEvents(spark)
  }

  test("handle UserContextCreated events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "subject",
        "role",
        "loadtime",
        "schoolId",
        "gradeId",
        "userId",
        "occurredOn",
        "language",
        "grade",
        "tenantId",
        "contextId",
        "subjectId",
        "tutorLocked"
      )
      val fixtures = List(
        SparkFixture(
          key = "tutor-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"UserContextUpdated",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "userId": "user-id",
              |	      "role": "student",
              |	      "contextId": "context-id",
              |	      "schoolId": "school-id",
              |	      "gradeId": "grade-id",
              |	      "grade": 7,
              |	      "subjectId": "subject-id",
              |	      "subject": "maths",
              |	      "language": "english",
              |       "tutorLocked": false,
              |  	    "occurredOn": "2023-05-15 16:23:46.609"
              |     }
              | },
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "tutor-context-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "UserContextUpdated"
            fst.getAs[String]("contextId") shouldBe "context-id"
            fst.getAs[String]("subjectId") shouldBe "subject-id"
            fst.getAs[Int]("grade") shouldBe 7
            fst.getAs[String]("schoolId") shouldBe "school-id"
            fst.getAs[Boolean]("tutorLocked") shouldBe false
            fst.getAs[String]("occurredOn") shouldBe "2023-05-15 16:23:46.609"
          }
        }
      )
    }
  }

  test("handle ChatSessionUpdated events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "userId",
        "role",
        "sessionId",
        "subject",
        "loadtime",
        "schoolId",
        "contextId",
        "subjectId",
        "gradeId",
        "materialId",
        "occurredOn",
        "levelId",
        "activityStatus",
        "language",
        "grade",
        "activityId",
        "outcomeId",
        "learningSessionId",
        "materialType",
        "tenantId",
        "sessionStatus",
        "sessionMessageLimitReached"
      )
      val fixtures = List(
        SparkFixture(
          key = "tutor-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"ChatSessionUpdated",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |         "userId": "student-id-1",
              |	      "role": "student",
              |	      "contextId": "context-id-1",
              |	      "schoolId": "school-id-1",
              |	      "gradeId": "grade-id-1",
              |	      "grade": 0,
              |	      "subjectId": "subject-id-1",
              |	      "subject": "math",
              |	      "language": "english",
              |	      "sessionId": "session-id-1",
              |	      "sessionStatus": "in_progress",
              |	      "materialId": "material-id-1",
              |         "materialType": "pathways",
              |	      "activityId": "string",
              |         "activityStatus": "inProgress",
              |	      "levelId": "level-id-1",
              |	      "outcomeId": "outcome-id-1",
              |         "learningSessionId": "learning-session-id-1",
              |         "sessionMessageLimitReached": false,
              |         "occurredOn": "2022-05-20 16:23:46.609"
              |     }
              | },
              | "timestamp": "2022-05-20 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "tutor-session-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "ChatSessionUpdated"
            fst.getAs[String]("userId") shouldBe "student-id-1"
            fst.getAs[String]("role") shouldBe "student"
            fst.getAs[String]("contextId") shouldBe "context-id-1"
            fst.getAs[String]("schoolId") shouldBe "school-id-1"
            fst.getAs[String]("gradeId") shouldBe "grade-id-1"
            fst.getAs[Int]("grade") shouldBe 0
            fst.getAs[String]("subjectId") shouldBe "subject-id-1"
            fst.getAs[String]("subject") shouldBe "math"
            fst.getAs[String]("language") shouldBe "english"
            fst.getAs[String]("sessionId") shouldBe "session-id-1"
            fst.getAs[String]("sessionStatus") shouldBe "in_progress"
            fst.getAs[String]("materialId") shouldBe "material-id-1"
            fst.getAs[String]("materialType") shouldBe "pathways"
            fst.getAs[String]("activityId") shouldBe "string"
            fst.getAs[String]("activityStatus") shouldBe "inProgress"
            fst.getAs[String]("levelId") shouldBe "level-id-1"
            fst.getAs[String]("outcomeId") shouldBe "outcome-id-1"
            fst.getAs[String]("learningSessionId") shouldBe "learning-session-id-1"
            fst.getAs[Boolean]("sessionMessageLimitReached") shouldBe false
            fst.getAs[String]("occurredOn") shouldBe "2022-05-20 16:23:46.609"
          }
        }
      )
    }
  }

  test("handle ChatConversationOccurred events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "tutor-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"ChatConversationOccurred",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "userId": "student-1",
              |	      "role": "student",
              |	      "contextId": "context-id-1",
              |	      "schoolId": "school-id-1",
              |	      "gradeId": "grade-id-1",
              |	      "grade": 0,
              |	      "subjectId": "subject-id-1",
              |	      "subject": "math",
              |	      "language": "english",
              |	      "sessionId": "session-id-1",
              |	      "materialId": "material-id-1",
              |         "materialType": "pathways",
              |	      "activityId": "activity-id-1",
              |         "activityStatus": "inProgress",
              |	      "levelId": "level-id-1",
              |	      "outcomeId": "outcome-id-1",
              |         "messageId": "message-id-1",
              |	      "conversationId": "conversation-id-1",
              |	      "conversationMaxTokens": 0,
              |         "conversationTokenCount": 0,
              |	      "systemPrompt": "hello",
              |	      "systemPromptTokens": 0,
              |         "suggestionsPrompt": "suggestions prompt",
              |	      "suggestionsPromptTokens": 0,
              |	      "messageLanguage": "english",
              |	      "messageTokens": 0,
              |	      "messageFeedback": "unclicked",
              |	      "userMessage": "please answer my question",
              |	      "userMessageSource": "default",
              |	      "userMessageTokens": 0,
              |	      "userMessageTimestamp": "2023-05-16 15:00:46.609",
              |         "userMessageClassification": "non-math",
              |	      "botMessage": "here is your answer",
              |	      "botMessageSource": "default",
              |	      "botMessageTokens": 0,
              |	      "botMessageTimestamp": "2023-05-16 15:23:46.609",
              |	      "botMessageConfidence": 0.0,
              |	      "botMessageResponseTime": 0.0 ,
              |         "activityPageContextId": "page_context_id",
              |	      "studentLocation": "school",
              |	      "suggestionClicked": false,
              |         "clickedSuggestionId": null,
              |	      "occurredOn": "2023-05-16 16:23:46.609"
              |     }
              | },
              | "timestamp": "2022-05-20 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "tutor-conversation-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedChatConversationOccurredColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.map(_.first()).foreach(assertChatConversationOccurredFields)
        }
      )
    }
  }

  test("handle ChatConversationOccurred with sessionStatus events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "tutor-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"ChatConversationOccurred",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |         "userId": "student-1",
              |	      "role": "student",
              |	      "contextId": "context-id-1",
              |	      "schoolId": "school-id-1",
              |	      "gradeId": "grade-id-1",
              |	      "grade": 0,
              |	      "subjectId": "subject-id-1",
              |	      "subject": "math",
              |	      "language": "english",
              |	      "sessionId": "session-id-1",
              |         "sessionStatus": "in_progress",
              |	      "materialId": "material-id-1",
              |         "materialType": "pathways",
              |	      "activityId": "activity-id-1",
              |         "activityStatus": "inProgress",
              |	      "levelId": "level-id-1",
              |	      "outcomeId": "outcome-id-1",
              |         "messageId": "message-id-1",
              |	      "conversationId": "conversation-id-1",
              |	      "conversationMaxTokens": 0,
              |         "conversationTokenCount": 0,
              |	      "systemPrompt": "hello",
              |	      "systemPromptTokens": 0,
              |         "suggestionsPrompt": "suggestions prompt",
              |	      "suggestionsPromptTokens": 0,
              |	      "messageLanguage": "english",
              |	      "messageTokens": 0,
              |	      "messageFeedback": "unclicked",
              |	      "userMessage": "please answer my question",
              |	      "userMessageSource": "default",
              |	      "userMessageTokens": 0,
              |	      "userMessageTimestamp": "2023-05-16 15:00:46.609",
              |         "userMessageClassification": "non-math",
              |	      "botMessage": "here is your answer",
              |	      "botMessageSource": "default",
              |	      "botMessageTokens": 0,
              |	      "botMessageTimestamp": "2023-05-16 15:23:46.609",
              |	      "botMessageConfidence": 0.0,
              |	      "botMessageResponseTime": 0.0 ,
              |         "activityPageContextId": "page_context_id",
              |	      "studentLocation": "school",
              |	      "suggestionClicked": false,
              |         "clickedSuggestionId": null,
              |	      "occurredOn": "2023-05-16 16:23:46.609"
              |     }
              | },
              | "timestamp": "2022-05-20 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "tutor-conversation-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedChatConversationOccurredColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.map(_.first()).foreach(assertChatConversationOccurredFields _ compose assertSessionStatusField)
        }
      )
    }
  }

  test("handle ChatSuggestionsGenerated events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "tenantId",
        "userId",
        "sessionId",
        "conversationId",
        "messageId",
        "responseTime",
        "successParserTokens",
        "failureParserTokens",
        "suggestions"
      )
      val fixtures = List(
        SparkFixture(
          key = "tutor-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"ChatSuggestionsGenerated",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "userId": "user-id-1",
              |       "sessionId": "session-id-1",
              |       "conversationId": "conversation-id-1",
              |       "messageId": "message-id-1",
              |       "responseTime": 0.0,
              |       "successParserTokens": 0,
              |       "failureParserTokens": 0,
              |       "suggestions": [
              |          {
              |            "suggestionId": "suggestion-id-1",
              |            "suggestionQuestion": "What is the sum of 2 and 3?",
              |            "suggestionClicked": false
              |          },
              |          {
              |            "suggestionId": "suggestion-id-2",
              |            "suggestionQuestion": "What is the result of adding 2 and 3?",
              |            "suggestionClicked": false
              |          }
              |        ],
              |       "occurredOn": "2022-05-20 16:23:46.609"
              |     }
              | },
              | "timestamp": "2022-05-20 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "tutor-suggestions-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "ChatSuggestionsGenerated"
            fst.getAs[String]("userId") shouldBe "user-id-1"
            fst.getAs[String]("sessionId") shouldBe "session-id-1"
            fst.getAs[String]("conversationId") shouldBe "conversation-id-1"
            fst.getAs[String]("messageId") shouldBe "message-id-1"
            fst.getAs[Double]("responseTime") shouldBe 0.0
            fst.getAs[Int]("successParserTokens") shouldBe 0
            fst.getAs[Int]("failureParserTokens") shouldBe 0
            val suggestions = fst.getAs[Seq[Row]]("suggestions").map(rowToSuggestion).toList
            suggestions.size shouldBe 2
            suggestions(0).suggestionId shouldBe "suggestion-id-1"
            suggestions(0).suggestionQuestion shouldBe "What is the sum of 2 and 3?"
            suggestions(0).suggestionClicked shouldBe false
            suggestions(1).suggestionId shouldBe "suggestion-id-2"
            suggestions(1).suggestionQuestion shouldBe "What is the result of adding 2 and 3?"
            suggestions(1).suggestionClicked shouldBe false
            fst.getAs[String]("occurredOn") shouldBe "2022-05-20 16:23:46.609"
          }
        }
      )
    }
  }

  def rowToSuggestion(row: Row): Suggestion = {
    Suggestion(
      suggestionId = row.getAs[String]("suggestionId"),
      suggestionQuestion = row.getAs[String]("suggestionQuestion"),
      suggestionClicked = row.getAs[Boolean]("suggestionClicked"),
    )
  }

  private def assertSessionStatusField(fst: Row): Row = {
    fst.getAs[String]("sessionStatus") shouldBe "in_progress"
    fst
  }

  private def assertChatConversationOccurredFields(fst: Row): Row = {
    fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
    fst.getAs[String]("eventType") shouldBe "ChatConversationOccurred"
    fst.getAs[String]("userId") shouldBe "student-1"
    fst.getAs[String]("role") shouldBe "student"
    fst.getAs[String]("contextId") shouldBe "context-id-1"
    fst.getAs[String]("schoolId") shouldBe "school-id-1"
    fst.getAs[String]("gradeId") shouldBe "grade-id-1"
    fst.getAs[Int]("grade") shouldBe 0
    fst.getAs[String]("subjectId") shouldBe "subject-id-1"
    fst.getAs[String]("subject") shouldBe "math"
    fst.getAs[String]("language") shouldBe "english"
    fst.getAs[String]("sessionId") shouldBe "session-id-1"
    fst.getAs[String]("materialId") shouldBe "material-id-1"
    fst.getAs[String]("materialType") shouldBe "pathways"
    fst.getAs[String]("activityId") shouldBe "activity-id-1"
    fst.getAs[String]("activityStatus") shouldBe "inProgress"
    fst.getAs[String]("levelId") shouldBe "level-id-1"
    fst.getAs[String]("outcomeId") shouldBe "outcome-id-1"
    fst.getAs[String]("messageId") shouldBe "message-id-1"
    fst.getAs[String]("conversationId") shouldBe "conversation-id-1"
    fst.getAs[Int]("conversationMaxTokens") shouldBe 0
    fst.getAs[Int]("conversationTokenCount") shouldBe 0
    fst.getAs[String]("systemPrompt") shouldBe "hello"
    fst.getAs[Int]("systemPromptTokens") shouldBe 0
    fst.getAs[String]("suggestionsPrompt") shouldBe "suggestions prompt"
    fst.getAs[Int]("suggestionsPromptTokens") shouldBe 0
    fst.getAs[String]("messageLanguage") shouldBe "english"
    fst.getAs[Int]("messageTokens") shouldBe 0
    fst.getAs[Boolean]("messageFeedback") shouldBe "unclicked"
    fst.getAs[String]("userMessage") shouldBe "please answer my question"
    fst.getAs[String]("userMessageSource") shouldBe "default"
    fst.getAs[Int]("userMessageTokens") shouldBe 0
    fst.getAs[String]("userMessageTimestamp") shouldBe "2023-05-16 15:00:46.609"
    fst.getAs[String]("userMessageClassification") shouldBe "non-math"
    fst.getAs[String]("botMessage") shouldBe "here is your answer"
    fst.getAs[String]("botMessageSource") shouldBe "default"
    fst.getAs[Int]("botMessageTokens") shouldBe 0
    fst.getAs[String]("botMessageTimestamp") shouldBe "2023-05-16 15:23:46.609"
    fst.getAs[Double]("botMessageConfidence") shouldBe 0.0
    fst.getAs[Double]("botMessageResponseTime") shouldBe 0.0
    fst.getAs[String]("activityPageContextId") shouldBe "page_context_id"
    fst.getAs[String]("studentLocation") shouldBe "school"
    fst.getAs[Boolean]("suggestionClicked") shouldBe false
    fst.getAs[String]("clickedSuggestionId") shouldBe null
    fst.getAs[String]("occurredOn") shouldBe "2023-05-16 16:23:46.609"
    fst
  }

  test("handle OnboardingQuestionAnswered events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "questionId",
        "questionCategory",
        "userResponse",
        "userFreeTextResponse",
        "onboardingSkipped",
        "onboardingComplete",
        "userId",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = "tutor-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"OnboardingQuestionAnswered",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "userId": "user-id-1",
              |       "questionId": "question-id-1",
              |       "questionCategory": "About",
              |       "userResponse": "previous-response",
              |       "userFreeTextResponse": false,
              |       "onboardingSkipped": false,
              |       "onboardingComplete": false,
              |       "occurredOn": "2022-05-20 16:23:46.609"
              |     }
              | },
              | "timestamp": "2022-05-20 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "tutor-onboarding-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "OnboardingQuestionAnswered"
            fst.getAs[String]("userId") shouldBe "user-id-1"
            fst.getAs[String]("questionId") shouldBe "question-id-1"
            fst.getAs[String]("questionCategory") shouldBe "About"
            fst.getAs[String]("userResponse") shouldBe "previous-response"
            fst.getAs[Boolean]("userFreeTextResponse") shouldBe false
            fst.getAs[Boolean]("onboardingSkipped") shouldBe false
            fst.getAs[Boolean]("onboardingComplete") shouldBe false
            fst.getAs[String]("occurredOn") shouldBe "2022-05-20 16:23:46.609"
          }
        }
      )
    }
  }

  test("handle CallToActionAnswered events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "tenantId",
        "userId",
        "message",
        "actionType",
        "actionMetadata",
        "accepted",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = "tutor-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"CallToActionAnswered",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "userId": "user-id-1",
              |       "message": "message-1",
              |       "actionType": "action-type-1",
              |       "actionMetadata": {
              |           "pathway_id": "pathway-id-1"
              |       }, 
              |       "accepted": false, 
              |       "occurredOn": "2022-05-20 16:23:46.609"
              |     }
              | },
              | "timestamp": "2022-05-20 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "tutor-calltoaction-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "CallToActionAnswered"
            fst.getAs[String]("userId") shouldBe "user-id-1"
            fst.getAs[String]("message") shouldBe "message-1"
            fst.getAs[String]("actionType") shouldBe "action-type-1"
            fst.getAs[Boolean]("accepted") shouldBe false
            fst.getAs[String]("occurredOn") shouldBe "2022-05-20 16:23:46.609"
          }
        }
      )
    }
  }


  test("handle AnalogousOccurred events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "userId",
        "sessionId",
        "conversationId",
        "messageId",
        "analogous",
        "userInterest",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = "tutor-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AnalogousOccurred"
              |    },
              |    "body":{
              |       "userId": "user-id-1",
              |       "sessionId": "session-id-1",
              |       "conversationId": "conversation-id-1",
              |       "messageId": "message-id-1",
              |       "analogous": "analogous-1",
              |       "userInterest": "interest-1",
              |       "occurredOn": "2022-05-20 16:23:46.609"
              |     }
              | },
              | "timestamp": "2022-05-20 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "tutor-analogous-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "AnalogousOccurred"
            fst.getAs[String]("userId") shouldBe "user-id-1"
            fst.getAs[String]("sessionId") shouldBe "session-id-1"
            fst.getAs[String]("conversationId") shouldBe "conversation-id-1"
            fst.getAs[String]("messageId") shouldBe "message-id-1"
            fst.getAs[String]("analogous") shouldBe "analogous-1"
            fst.getAs[String]("userInterest") shouldBe "interest-1"
            fst.getAs[String]("occurredOn") shouldBe "2022-05-20 16:23:46.609"
          }
        }
      )
    }
  }

  test("handle TranslationOccurred events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "userId",
        "sessionId",
        "conversationId",
        "messageId",
        "translation",
        "translationLanguage",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = "tutor-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"TranslationOccurred"
              |    },
              |    "body":{
              |       "userId": "user-id-1",
              |       "sessionId": "session-id-1",
              |       "conversationId": "conversation-id-1",
              |       "messageId": "message-id-1",
              |       "translation": "translated-text",
              |       "translationLanguage": "english",
              |       "occurredOn": "2022-05-20 16:23:46.609"
              |     }
              | },
              | "timestamp": "2022-05-20 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "tutor-translation-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "TranslationOccurred"
            fst.getAs[String]("userId") shouldBe "user-id-1"
            fst.getAs[String]("sessionId") shouldBe "session-id-1"
            fst.getAs[String]("conversationId") shouldBe "conversation-id-1"
            fst.getAs[String]("messageId") shouldBe "message-id-1"
            fst.getAs[String]("translation") shouldBe "translated-text"
            fst.getAs[String]("translationLanguage") shouldBe "english"
            fst.getAs[String]("occurredOn") shouldBe "2022-05-20 16:23:46.609"
          }
        }
      )
    }
  }

  test("handle SimplificationOccurred events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "userId",
        "sessionId",
        "conversationId",
        "messageId",
        "simplification",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = "tutor-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"SimplificationOccurred"
              |    },
              |    "body":{
              |       "userId": "user-id-1",
              |       "sessionId": "session-id-1",
              |       "conversationId": "conversation-id-1",
              |       "messageId": "message-id-1",
              |       "simplification": "simplified-text",
              |       "occurredOn": "2022-05-20 16:23:46.609"
              |     }
              | },
              | "timestamp": "2022-05-20 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "tutor-simplification-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "SimplificationOccurred"
            fst.getAs[String]("userId") shouldBe "user-id-1"
            fst.getAs[String]("sessionId") shouldBe "session-id-1"
            fst.getAs[String]("conversationId") shouldBe "conversation-id-1"
            fst.getAs[String]("messageId") shouldBe "message-id-1"
            fst.getAs[String]("simplification") shouldBe "simplified-text"
            fst.getAs[String]("occurredOn") shouldBe "2022-05-20 16:23:46.609"
          }
        }
      )
    }
  }

  test("handle ChallengeQuestionGenerated events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "userId",
        "sessionId",
        "conversationId",
        "messageId",
        "botQuestionMessage",
        "botQuestionTokens",
        "botQuestionId",
        "botQuestionTimestamp",
        "botQuestionSource",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = "tutor-events-source",
          value =
            """
              |[
              |{
              |  "key": "key1",
              |  "value":{
              |     "headers":{
              |       "eventType": "ChallengeQuestionGenerated"
              |     },
              |     "body": {
              |       "userId": "user-id-1",
              |       "sessionId": "session-id-1",
              |       "conversationId": "conversation-id-1",
              |       "messageId": "message-id-1",
              |       "botQuestionSource": "default",
              |       "botQuestionMessage": "What is the capital of France?",
              |       "botQuestionTokens": 6,
              |       "botQuestionId": "question-id-1",
              |       "botQuestionTimestamp": "2022-05-20 16:23:46.609",
              |       "occurredOn": "2022-05-20 16:23:46.609"
              |     }
              |   },
              |   "timestamp": "2022-05-20 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "tutor-challengequestions-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "ChallengeQuestionGenerated"
            fst.getAs[String]("userId") shouldBe "user-id-1"
            fst.getAs[String]("sessionId") shouldBe "session-id-1"
            fst.getAs[String]("conversationId") shouldBe "conversation-id-1"
            fst.getAs[String]("messageId") shouldBe "message-id-1"
            fst.getAs[String]("botQuestionMessage") shouldBe "What is the capital of France?"
            fst.getAs[Int]("botQuestionTokens") shouldBe 6
            fst.getAs[String]("botQuestionId") shouldBe "question-id-1"
            fst.getAs[String]("botQuestionTimestamp") shouldBe "2022-05-20 16:23:46.609"
            fst.getAs[String]("botQuestionSource") shouldBe "default"
            fst.getAs[String]("occurredOn") shouldBe "2022-05-20 16:23:46.609"
          }
        }
      )
    }
  
}
}
