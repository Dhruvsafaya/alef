package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.should.Matchers

class GuardianTeacherFeedbackSpec extends SparkSuite with Matchers {

  trait Setup {

    val session = spark

    implicit val transformer = new PracticeEventsTransformer("practice-events", spark)

  }

  test("handle TeacherFeedbackSentEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "feedbackThreadId",
        "feedbackType",
        "responseEnabled",
        "loadtime",
        "guardianIds",
        "teacherId",
        "studentId",
        "occurredOn",
        "classId",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "TeacherFeedbackSentEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |    "occurredOn": 1592122860957,
              |    "feedbackThreadId": "fft1",
              |    "feedbackType": "ACADEMIC",
              |    "responseEnabled": true,
              |    "guardianIds": [
              |    {
              |    "id":"id1",
              |    "id":"id2"
              |    }
              |    ],
              |    "teacherId": "fc604213-e624-4502-a48f-db3f1f1d7667",
              |    "studentId": "6e70b79d-51f0-4c74-82cb-9ee9d01ab728",
              |    "classId": "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17"
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "teacher-feedback-sent-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("teacher-feedback-sent-sink is not found"))

          df.columns.toSet shouldBe expectedColumns
          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "TeacherFeedbackSentEvent"
          fst.getAs[String]("feedbackThreadId") shouldBe "fft1"
          fst.getAs[String]("feedbackType") shouldBe "ACADEMIC"
          fst.getAs[String]("teacherId") shouldBe "fc604213-e624-4502-a48f-db3f1f1d7667"
          fst.getAs[String]("studentId") shouldBe "6e70b79d-51f0-4c74-82cb-9ee9d01ab728"
          fst.getAs[String]("classId") shouldBe "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17"
        }
      )
    }
  }

  test("handle TeacherMessageSentEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "feedbackThreadId",
        "messageId",
        "isFirstOfThread",
        "loadtime",
        "occurredOn",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |  "headers": {
                    |    "eventType": "TeacherMessageSentEvent",
                    |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                    |  },
                    |  "body": {
                    |    "occurredOn": 1592122860957,
                    |    "feedbackThreadId": "fft1",
                    |    "messageId": "msg1",
                    |    "isFirstOfThread": true
                    |  }
                    |},
                    | "timestamp": "2019-04-20 16:24:37.501"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "teacher-message-sent-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("teacher-message-sent-sink is not found"))

          df.columns.toSet shouldBe expectedColumns
          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "TeacherMessageSentEvent"
          fst.getAs[String]("feedbackThreadId") shouldBe "fft1"
          fst.getAs[String]("messageId") shouldBe "msg1"
          fst.getAs[Boolean]("isFirstOfThread") shouldBe true
        }
      )
    }
  }

  test("handle GuardianFeedbackReadEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "feedbackThreadId",
        "loadtime",
        "occurredOn",
        "tenantId",
        "guardianId"
      )

      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |  "headers": {
                    |    "eventType": "GuardianFeedbackReadEvent",
                    |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                    |  },
                    |  "body": {
                    |    "occurredOn": 1592122860957,
                    |    "feedbackThreadId": "fft1",
                    |    "guardianId": "g1"
                    |  }
                    |},
                    | "timestamp": "2019-04-20 16:24:37.501"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "guardian-feedback-read-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("guardian-feedback-read-sink is not found"))

          df.columns.toSet shouldBe expectedColumns
          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "GuardianFeedbackReadEvent"
          fst.getAs[String]("feedbackThreadId") shouldBe "fft1"
          fst.getAs[String]("guardianId") shouldBe "g1"
        }
      )
    }
  }

  test("handle GuardianMessageSentEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "feedbackThreadId",
        "messageId",
        "guardianId",
        "loadtime",
        "occurredOn",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |  "headers": {
                    |    "eventType": "GuardianMessageSentEvent",
                    |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                    |  },
                    |  "body": {
                    |    "occurredOn": 1592122860957,
                    |    "feedbackThreadId": "fft1",
                    |    "messageId": "msg1",
                    |    "guardianId":"g1"
                    |  }
                    |},
                    | "timestamp": "2019-04-20 16:24:37.501"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "guardian-message-sent-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("guardian-message-sent-sink is not found"))

          df.columns.toSet shouldBe expectedColumns
          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "GuardianMessageSentEvent"
          fst.getAs[String]("feedbackThreadId") shouldBe "fft1"
          fst.getAs[String]("messageId") shouldBe "msg1"
          fst.getAs[String]("guardianId") shouldBe "g1"
        }
      )
    }
  }

  test("handle TeacherMessageDeletedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "messageId",
        "loadtime",
        "occurredOn",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |  "headers": {
                    |    "eventType": "TeacherMessageDeletedEvent",
                    |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                    |  },
                    |  "body": {
                    |    "occurredOn": 1592122860957,
                    |    "messageId": "msg1"
                    |  }
                    |},
                    | "timestamp": "2019-04-20 16:24:37.501"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "teacher-message-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("teacher-message-deleted-sink is not found"))

          df.columns.toSet shouldBe expectedColumns
          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "TeacherMessageDeletedEvent"
          fst.getAs[String]("messageId") shouldBe "msg1"
        }
      )
    }
  }

  test("handle TeacherFeedbackDeletedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "feedbackThreadId",
        "loadtime",
        "occurredOn",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |  "headers": {
                    |    "eventType": "TeacherFeedbackDeletedEvent",
                    |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                    |  },
                    |  "body": {
                    |    "occurredOn": 1592122860957,
                    |    "feedbackThreadId": "fft1"
                    |  }
                    |},
                    | "timestamp": "2019-04-20 16:24:37.501"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "teacher-feedback-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("teacher-feedback-deleted-sink is not found"))

          df.columns.toSet shouldBe expectedColumns
          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "TeacherFeedbackDeletedEvent"
          fst.getAs[String]("feedbackThreadId") shouldBe "fft1"
        }
      )
    }
  }

}
