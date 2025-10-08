package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.bigdata.streaming.ccl.testuils.CommonSpec
import com.alefeducation.schema.Schema._
import com.alefeducation.schema.Schema.{schema => schemaAlias}
import com.alefeducation.schema.ccl.QuestionBody
import com.alefeducation.util.Constants.{LessonSkillLinkedEvent, LessonSkillUnlinkedEvent}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.StructType
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedSet

class CCLEventsSpec extends SparkSuite with CommonSpec with Matchers {

  import CCLEvents._

  trait Setup {
    implicit val transformer: CCLEvents = new CCLEvents(spark)
  }

  val expectedSkillLinkedColumnsV2: Set[String] = Set(
    "skillId",
    "skillCode",
    "previousSkillId",
    "previousSkillCode",
    "loadtime",
    "occurredOn",
    "eventDateDw",
    "eventType"
  )

  val expectedSkillLinkedColumns: Set[String] = Set(
    "skillId",
    "skillCode",
    "nextSkillId",
    "nextSkillCode",
    "loadtime",
    "occurredOn",
    "eventDateDw",
    "eventType"
  )

  val expectedSkillCreatedColumns: Set[String] = Set(
    "id",
    "code",
    "name",
    "description",
    "loadtime",
    "subjectId",
    "occurredOn",
    "eventDateDw",
    "translations",
    "eventType"
  )

  val expectedColsForCclLessonSkill: Set[String] = Set(
    "eventType",
    "eventDateDw",
    "loadtime",
    "occurredOn",
    "derived",
    "skillId",
    "lessonUuid",
    "lessonId"
  )

  val expectedLessonContentAttached: Set[String] = Set(
    "eventType",
    "eventDateDw",
    "loadtime",
    "occurredOn",
    "outcomeId",
    "lessonUuid",
    "lessonId"
  )

  val expectedLessonColumns: Set[String] = Set(
    "eventType",
    "eventDateDw",
    "loadtime",
    "lessonId",
    "lessonUuid",
    "code",
    "status",
    "description",
    "title",
    "boardId",
    "gradeId",
    "subjectId",
    "skillable",
    "mloFrameworkId",
    "thumbnailFileName",
    "thumbnailContentType",
    "thumbnailFileSize",
    "thumbnailLocation",
    "thumbnailUpdatedAt",
    "userId",
    "themeIds",
    "lessonType",
    "mloTemplateId",
    "academicYearId",
    "assessmentTool",
    "academicYear",
    "createdAt",
    "updatedAt",
    "occurredOn",
    "lessonRoles",
    "duration",
    "organisation"
  )

  val lessonWithContentPublished : Set[String] = Set(
    "publisherId",
    "maxStars",
    "eventType",
    "eventDateDw",
    "lessonJSON",
    "loadtime",
    "publishedBefore",
    "occurredOn",
    "publishedDate",
    "lessonUuid",
    "lessonId"
  )

  val expectedLessonContent: Set[String] = Set(
    "lessonId",
    "lessonUuid",
    "eventType",
    "eventDateDw",
    "contentId",
    "loadtime",
    "stepId",
    "stepUuid",
    "occurredOn"
  )

  test("handle CCL Lesson created event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |   "headers":{
              |      "eventType":"LessonCreatedEvent"
              |   },
              |   "body":{
              |      "lessonId":4128,
              |      "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
              |      "code":"lesson_Code1",
              |      "status":"DRAFT",
              |      "description":"lesson_desc",
              |      "title":"lesson1",
              |      "boardId":392027,
              |      "gradeId":322135,
              |      "subjectId":571671,
              |      "skillable":true,
              |      "mloFrameworkId":1,
              |      "thumbnailFileName":"lesson_Code1_thumb.jpeg",
              |      "thumbnailContentType":"image/jpeg",
              |      "thumbnailFileSize":10156,
              |      "thumbnailLocation":"77/0c/0e/4128",
              |      "thumbnailUpdatedAt":1586848461741,
              |      "userId":30,
              |      "mloTemplateId":null,
              |      "academicYearId":2,
              |      "academicYear":"2020",
              |      "assessmentTool":"ALEF",
              |      "lessonType":"INSTRUCTIONAL_LESSON",
              |      "createdAt":1586848461741,
              |      "themeIds":[4,5,6],
              |      "duration":25,
              |      "lessonRoles": [
              |         "REMEDIATION_LESSON"
              |       ],
              |      "occurredOn":1586848461743,
              |      "organisation": "shared"
              |   }
              | },
              | "timestamp": "2019-04-20 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val cclLessonCreatedSinks = sinks
            .find(_.name == "ccl-lesson-mutated-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-lesson-mutated-sink is not found"))

          cclLessonCreatedSinks.columns.toSet shouldBe expectedLessonColumns
          val lessonCreated = cclLessonCreatedSinks.first()
          lessonCreated.getAs[String]("eventType") shouldBe "LessonCreatedEvent"
          lessonCreated.getAs[Long]("lessonId") shouldBe 4128
          lessonCreated.getAs[String]("lessonUuid") shouldBe "f22b5423-c60e-4c0d-b06d-000000004128"
          lessonCreated.getAs[String]("code") shouldBe "lesson_Code1"
          lessonCreated.getAs[String]("status") shouldBe "DRAFT"
          lessonCreated.getAs[String]("description") shouldBe "lesson_desc"
          lessonCreated.getAs[String]("title") shouldBe "lesson1"
          lessonCreated.getAs[Long]("boardId") shouldBe 392027
          lessonCreated.getAs[Long]("gradeId") shouldBe 322135
          lessonCreated.getAs[Long]("subjectId") shouldBe 571671
          lessonCreated.getAs[Boolean]("skillable") shouldBe true
          lessonCreated.getAs[Long]("mloFrameworkId") shouldBe 1
          lessonCreated.getAs[String]("thumbnailFileName") shouldBe "lesson_Code1_thumb.jpeg"
          lessonCreated.getAs[String]("thumbnailContentType") shouldBe "image/jpeg"
          lessonCreated.getAs[Long]("thumbnailFileSize") shouldBe 10156
          lessonCreated.getAs[String]("thumbnailLocation") shouldBe "77/0c/0e/4128"
          lessonCreated.getAs[Long]("thumbnailUpdatedAt") shouldBe 1586848461741L
          lessonCreated.getAs[Long]("userId") shouldBe 30
          lessonCreated.getAs[Long]("academicYearId") shouldBe 2
          lessonCreated.getAs[String]("academicYear") shouldBe "2020"
          lessonCreated.getAs[String]("assessmentTool") shouldBe "ALEF"
          lessonCreated.getAs[String]("lessonType") shouldBe "INSTRUCTIONAL_LESSON"
          lessonCreated.getAs[String]("mloTemplateId") shouldBe null
          lessonCreated.getAs[Long]("createdAt") shouldBe 1586848461741L
          lessonCreated.getAs[List[Long]]("themeIds") shouldBe List(4, 5, 6)
          lessonCreated.getAs[Int]("duration") shouldBe 25
          lessonCreated.getAs[String]("loadtime") shouldBe "2019-04-20 16:23:46.609"
          lessonCreated.getAs[String]("eventDateDw") shouldBe "20200414"
          lessonCreated.getAs[String]("occurredOn") shouldBe "2020-04-14 07:14:21.743"
          lessonCreated.getAs[List[String]]("lessonRoles") shouldBe List("REMEDIATION_LESSON")
        }
      )
    }
  }

  test("handle CCL Lesson updated event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |   "headers":{
              |      "eventType":"LessonMetadataUpdatedEvent"
              |   },
              |   "body":{
              |      "lessonId":4128,
              |      "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
              |      "code":"lesson_Code1",
              |      "status":"DRAFT",
              |      "description":"lesson_desc_updated",
              |      "title":"lesson1",
              |      "boardId":392027,
              |      "gradeId":322135,
              |      "subjectId":571671,
              |      "skillable":true,
              |      "mloFrameworkId":1,
              |      "thumbnailFileName":"lesson_Code1_thumb.jpeg",
              |      "thumbnailContentType":"image/jpeg",
              |      "thumbnailFileSize":10156,
              |      "thumbnailLocation":"77/0c/0e/4128",
              |      "thumbnailUpdatedAt":1586848461741,
              |      "userId":30,
              |      "mloTemplateId":null,
              |      "academicYearId":2,
              |      "academicYear":"2020",
              |      "assessmentTool":"ALEF",
              |      "lessonType":"INSTRUCTIONAL_LESSON",
              |      "createdAt":null,
              |      "updatedAt":1586848461741,
              |      "themeIds":[4,5,6],
              |      "duration":25,
              |      "lessonRoles": [
              |         "REMEDIATION_LESSON"
              |       ],
              |      "occurredOn":1586848461743,
              |      "organisation": "shared"
              |   }
              | },
              | "timestamp": "2019-04-20 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val cclLessonUpdatedSinks = sinks
            .find(_.name == "ccl-lesson-mutated-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-lesson-mutated-sink is not found"))

          cclLessonUpdatedSinks.columns.toSet shouldBe expectedLessonColumns
          val lessonCreated = cclLessonUpdatedSinks.first()
          lessonCreated.getAs[String]("eventType") shouldBe "LessonMetadataUpdatedEvent"
          lessonCreated.getAs[String]("lessonUuid") shouldBe "f22b5423-c60e-4c0d-b06d-000000004128"
          lessonCreated.getAs[Long]("lessonId") shouldBe 4128
          lessonCreated.getAs[String]("code") shouldBe "lesson_Code1"
          lessonCreated.getAs[String]("status") shouldBe "DRAFT"
          lessonCreated.getAs[String]("description") shouldBe "lesson_desc_updated"
          lessonCreated.getAs[String]("title") shouldBe "lesson1"
          lessonCreated.getAs[Long]("boardId") shouldBe 392027
          lessonCreated.getAs[Long]("gradeId") shouldBe 322135
          lessonCreated.getAs[Long]("subjectId") shouldBe 571671
          lessonCreated.getAs[Boolean]("skillable") shouldBe true
          lessonCreated.getAs[Long]("mloFrameworkId") shouldBe 1
          lessonCreated.getAs[String]("thumbnailFileName") shouldBe "lesson_Code1_thumb.jpeg"
          lessonCreated.getAs[String]("thumbnailContentType") shouldBe "image/jpeg"
          lessonCreated.getAs[Long]("thumbnailFileSize") shouldBe 10156
          lessonCreated.getAs[String]("thumbnailLocation") shouldBe "77/0c/0e/4128"
          lessonCreated.getAs[Long]("thumbnailUpdatedAt") shouldBe 1586848461741L
          lessonCreated.getAs[Long]("userId") shouldBe 30
          lessonCreated.getAs[Long]("academicYearId") shouldBe 2
          lessonCreated.getAs[String]("academicYear") shouldBe "2020"
          lessonCreated.getAs[String]("assessmentTool") shouldBe "ALEF"
          lessonCreated.getAs[String]("lessonType") shouldBe "INSTRUCTIONAL_LESSON"
          lessonCreated.getAs[String]("mloTemplateId") shouldBe null
          lessonCreated.getAs[String]("createdAt") shouldBe null
          lessonCreated.getAs[Long]("updatedAt") shouldBe 1586848461741L
          lessonCreated.getAs[List[Long]]("themeIds") shouldBe List(4, 5, 6)
          lessonCreated.getAs[Int]("duration") shouldBe 25
          lessonCreated.getAs[String]("loadtime") shouldBe "2019-04-20 16:23:46.609"
          lessonCreated.getAs[String]("eventDateDw") shouldBe "20200414"
          lessonCreated.getAs[String]("occurredOn") shouldBe "2020-04-14 07:14:21.743"
        }
      )
    }
  }

  test("handle CCL Lesson with contents published event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |   "headers":{
              |      "eventType":"LessonPublishedEvent"
              |   },
              |   "body":{
              |      "lessonId":2960,
              |      "lessonUuid":"f9f6c8e6-67b7-32eb-b232-000000002960",
              |      "publishedDate":"2020-04-14T07:19:40.529315",
              |      "publisherId":30,
              |      "publishedBefore":true,
              |      "maxStars":5,
              |      "lessonJSON":{
              |         "code":"MA6_MLO_187",
              |         "title":"Show My Learning - 4",
              |         "description":"Show My Learning - 4",
              |         "lessonType":"INSTRUCTIONAL_LESSON",
              |         "thumbnail":"MA6_MLO_187_thumb.png",
              |         "academic_year":"2020",
              |         "education_board":{
              |            "id":392027,
              |            "name":"UAE MOE"
              |         },
              |         "subject":{
              |            "id":571671,
              |            "name":"Math"
              |         },
              |         "grade":{
              |            "id":322135,
              |            "name":"6"
              |         },
              |         "contents":[
              |            {
              |               "id":"KEY_TERMS",
              |               "title":"Key Terms",
              |               "abbreviation":"KT",
              |               "type":"STATIC",
              |               "url":"/mloId/2960?digest=a6a7ec66025d4674e756d6222265ef4e",
              |               "description":"Show My Learning - 4"
              |            },
              |            {
              |               "id":"SUMMATIVE_ASSESSMENT",
              |               "title":"My Exit Ticket",
              |               "abbreviation":"SA",
              |               "type":"INTERACTIVE",
              |               "url":"b9fb4094663f9c2fa3e4b0c4ddd470e9",
              |               "description":"Show My Learning - 4"
              |            }
              |         ]
              |      },
              |      "occurredOn":1586848780663
              |   }
              | },
              | "timestamp": "2019-04-20 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val cclLessonPublishedSinks = sinks
            .find(_.name == "ccl-lesson-published-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-lesson-published-sink is not found"))

          cclLessonPublishedSinks.columns.toSet shouldBe lessonWithContentPublished
          val lessonPublished = cclLessonPublishedSinks.first()
          lessonPublished.getAs[String]("eventType") shouldBe "LessonPublishedEvent"
          lessonPublished.getAs[Long]("lessonId") shouldBe 2960
          lessonPublished.getAs[String]("lessonUuid") shouldBe "f9f6c8e6-67b7-32eb-b232-000000002960"
          lessonPublished.getAs[Long]("publisherId") shouldBe 30
          lessonPublished.getAs[Int]("maxStars") shouldBe 5
          lessonPublished.getAs[Boolean]("publishedBefore") shouldBe true
          lessonPublished.getAs[String]("publishedDate") shouldBe "2020-04-14T07:19:40.529315"
          lessonPublished.getAs[String]("loadtime") shouldBe "2019-04-20 16:23:46.609"
          lessonPublished.getAs[String]("eventDateDw") shouldBe "20200414"
          lessonPublished.getAs[String]("occurredOn") shouldBe "2020-04-14 07:19:40.663"

          val lessonJsonDf = cclLessonPublishedSinks.select("lessonJSON.*")
          val lessonJson = lessonJsonDf.first()
          lessonJson.getAs[String]("code") shouldBe "MA6_MLO_187"
          lessonJson.getAs[String]("title") shouldBe "Show My Learning - 4"
          lessonJson.getAs[String]("description") shouldBe "Show My Learning - 4"
          lessonJson.getAs[String]("lessonType") shouldBe "INSTRUCTIONAL_LESSON"
          lessonJson.getAs[String]("academic_year") shouldBe "2020"

          val educationBoard = lessonJsonDf.select("education_board.*").first()
          educationBoard.getAs[Int]("id") shouldBe 392027
          educationBoard.getAs[String]("name") shouldBe "UAE MOE"

          val subject = lessonJsonDf.select("subject.*").first()
          subject.getAs[Int]("id") shouldBe 571671
          subject.getAs[String]("name") shouldBe "Math"

          val grade = lessonJsonDf.select("grade.*").first()
          grade.getAs[Int]("id") shouldBe 322135
          grade.getAs[String]("name") shouldBe "6"

          val content = lessonJsonDf.select(explode(col("contents"))).select("col.*").first()
          content.getAs[String]("id") shouldBe "KEY_TERMS"
          content.getAs[String]("title") shouldBe "Key Terms"
          content.getAs[String]("abbreviation") shouldBe "KT"
          content.getAs[String]("type") shouldBe "STATIC"
          content.getAs[String]("url") shouldBe "/mloId/2960?digest=a6a7ec66025d4674e756d6222265ef4e"
          content.getAs[String]("description") shouldBe "Show My Learning - 4"
        }
      )
    }
  }

  test("handle CCL Lesson published event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |  "headers": {
              |    "eventType": "LessonPublishedEvent"
              |  },
              |  "body": {
              |    "lessonId": 234567,
              |    "lessonUuid": "995afcba-e214-4d3c-a84a-000000234567",
              |    "publisherId": 1,
              |    "publishedDate": "2020-04-14T07:19:40.529315",
              |    "publishedBefore": true,
              |    "occurredOn": 1586848780663,
              |    "lessonJSON": {
              |      "code": "AR7_SML_077",
              |      "title": "مراجعة عامّة - 2",
              |      "description": "مراجعة عامّة - 2",
              |      "academic_year": "2019",
              |      "education_board": {
              |        "id": 392027,
              |        "name": "UAE MOE"
              |      },
              |      "subject": {
              |        "id": 352071,
              |        "name": "Arabic"
              |      },
              |      "grade": {
              |        "id": 596550,
              |        "name": "7"
              |      },
              |      "lessonType": "EXPERIENTIAL_LESSON",
              |      "thumbnail": "/dc/82/d6/607/unnamed.png",
              |      "link": "index.html?digest=344fb95b2d9e225af3b03d8dc78b8566",
              |      "type": "STATIC"
              |    }
              |  }
              |},
              | "timestamp": "2019-04-20 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val cclLessonPublishedSinks = sinks
            .find(_.name == "ccl-lesson-published-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-lesson-published-sink is not found"))

          cclLessonPublishedSinks.columns.toSet shouldBe lessonWithContentPublished
          val lessonPublished = cclLessonPublishedSinks.first()
          lessonPublished.getAs[String]("eventType") shouldBe "LessonPublishedEvent"
          lessonPublished.getAs[Long]("lessonId") shouldBe 234567
          lessonPublished.getAs[String]("lessonUuid") shouldBe "995afcba-e214-4d3c-a84a-000000234567"
          lessonPublished.getAs[Long]("publisherId") shouldBe 1
          lessonPublished.getAs[String]("maxStars") shouldBe null
          lessonPublished.getAs[Boolean]("publishedBefore") shouldBe true
          lessonPublished.getAs[String]("publishedDate") shouldBe "2020-04-14T07:19:40.529315"

          val lessonJsonDf = cclLessonPublishedSinks.select("lessonJSON.*")
          val lessonJson = lessonJsonDf.first()
          lessonJson.getAs[String]("code") shouldBe "AR7_SML_077"
          lessonJson.getAs[String]("title") shouldBe "مراجعة عامّة - 2"
          lessonJson.getAs[String]("description") shouldBe "مراجعة عامّة - 2"
          lessonJson.getAs[String]("lessonType") shouldBe "EXPERIENTIAL_LESSON"
          lessonJson.getAs[String]("academic_year") shouldBe "2019"

          val educationBoard = lessonJsonDf.select("education_board.*").first()
          educationBoard.getAs[Int]("id") shouldBe 392027
          educationBoard.getAs[String]("name") shouldBe "UAE MOE"

          val subject = lessonJsonDf.select("subject.*").first()
          subject.getAs[Int]("id") shouldBe 352071
          subject.getAs[String]("name") shouldBe "Arabic"

          val grade = lessonJsonDf.select("grade.*").first()
          grade.getAs[Int]("id") shouldBe 596550
          grade.getAs[String]("name") shouldBe "7"

          lessonPublished.getAs[String]("loadtime") shouldBe "2019-04-20 16:23:46.609"
          lessonPublished.getAs[String]("eventDateDw") shouldBe "20200414"
          lessonPublished.getAs[String]("occurredOn") shouldBe "2020-04-14 07:19:40.663"
        }
      )
    }
  }

  test("handle CCL Lesson workflow status changed event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |   "headers":{
              |      "eventType":"LessonWorkflowStatusChangedEvent"
              |   },
              |   "body":{
              |      "lessonId":4119,
              |      "lessonUuid":"40fcbbf2-0406-3ad0-a095-000000004119",
              |      "code":"lesson_Code1",
              |      "academicYear": "2019",
              |      "status":"REVIEW",
              |      "publishedBefore":false,
              |      "occurredOn":1586869578032
              |   }
              | },
              | "timestamp": "2019-04-20 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val lessonWorkflowStatusChangedSinks = sinks
            .find(_.name == "ccl-lesson-workflow-status-changed-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-lesson-workflow-status-changed-sink is not found"))

          val expectedLessonCreatedColumns: Set[String] = Set(
            "lessonId",
            "lessonUuid",
            "eventType",
            "eventDateDw",
            "status",
            "loadtime",
            "publishedBefore",
            "code",
            "academicYear",
            "occurredOn"
          )

          lessonWorkflowStatusChangedSinks.columns.toSet shouldBe expectedLessonCreatedColumns
          val lessonWorkflowStatusChanged = lessonWorkflowStatusChangedSinks.first()
          lessonWorkflowStatusChanged.getAs[String]("eventType") shouldBe "LessonWorkflowStatusChangedEvent"
          lessonWorkflowStatusChanged.getAs[Long]("lessonId") shouldBe 4119
          lessonWorkflowStatusChanged.getAs[String]("lessonUuid") shouldBe "40fcbbf2-0406-3ad0-a095-000000004119"
          lessonWorkflowStatusChanged.getAs[String]("code") shouldBe "lesson_Code1"
          lessonWorkflowStatusChanged.getAs[String]("academicYear") shouldBe "2019"
          lessonWorkflowStatusChanged.getAs[Boolean]("publishedBefore") shouldBe false
          lessonWorkflowStatusChanged.getAs[String]("loadtime") shouldBe "2019-04-20 16:23:46.609"
          lessonWorkflowStatusChanged.getAs[String]("eventDateDw") shouldBe "20200414"
          lessonWorkflowStatusChanged.getAs[String]("occurredOn") shouldBe "2020-04-14 13:06:18.032"
        }
      )
    }
  }

  test("handle CCL Lesson deleted event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |   "headers":{
              |      "eventType":"LessonDeletedEvent"
              |   },
              |   "body":{
              |      "lessonId":4160,
              |      "lessonUuid":"ab59bca5-545e-47e1-b866-000000004160",
              |      "code":"lesson_Code1",
              |      "academicYear": "2019",
              |      "occurredOn":1587467297273
              |   }
              | },
              | "timestamp": "2019-04-20 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val lessonWorkflowStatusChangedSinks = sinks
            .find(_.name == "ccl-lesson-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-lesson-deleted-sink is not found"))

          val expectedLessonCreatedColumns: Set[String] = Set(
            "lessonId",
            "lessonUuid",
            "eventType",
            "eventDateDw",
            "loadtime",
            "code",
            "academicYear",
            "occurredOn"
          )

          lessonWorkflowStatusChangedSinks.columns.toSet shouldBe expectedLessonCreatedColumns
          val lessonWorkflowStatusChanged = lessonWorkflowStatusChangedSinks.first()
          lessonWorkflowStatusChanged.getAs[String]("eventType") shouldBe "LessonDeletedEvent"
          lessonWorkflowStatusChanged.getAs[Long]("lessonId") shouldBe 4160
          lessonWorkflowStatusChanged.getAs[String]("lessonUuid") shouldBe "ab59bca5-545e-47e1-b866-000000004160"
          lessonWorkflowStatusChanged.getAs[String]("code") shouldBe "lesson_Code1"
          lessonWorkflowStatusChanged.getAs[String]("academicYear") shouldBe "2019"
          lessonWorkflowStatusChanged.getAs[String]("loadtime") shouldBe "2019-04-20 16:23:46.609"
          lessonWorkflowStatusChanged.getAs[String]("eventDateDw") shouldBe "20200421"
          lessonWorkflowStatusChanged.getAs[String]("occurredOn") shouldBe "2020-04-21 11:08:17.273"
        }
      )
    }
  }

  test("handle CCL Content Created/Updated/Deleted/Published/OutcomeAttached/OutcomeDetached events") {

    implicit val transformer: CCLEvents = new CCLEvents(spark)
    val eventsJson =
      """
        | {"headers":{"eventType":"ContentUpdatedEvent"},"body":{"contentId":29530,"title":"test_zip","description":"test_zipa2","tags":"test_zip","organisation": "Create event org","fileName":"DOK2_1.zip","fileContentType":"application/zip","fileSize":5080741,"fileUpdatedAt":1584448773474,"conditionOfUse":"","knowledgeDimension":"FACTUAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L1","mediaType":"HTML","status":"PUBLISHED","contentLocation":"56/62/0d/45/29530","author":"Shashank","authoredDate":"2020-03-17","updatedAt":1586928018894,"contentLearningResourceTypes":["DOK1"],"cognitiveDimensions":["REMEMBERING"],"copyrights":[],"occurredOn":1586928018896}}
        | {"headers":{"eventType":"ContentCreatedEvent"},"body":{"contentId":29546,"title":"Samvel Test Content","description":"Samvel Test Content","tags":"Samvel Test Content,samvel,content,test","organisation": "Update event org","fileName":"PR.jpg","fileContentType":"image/jpeg","fileSize":55733,"fileUpdatedAt":1586940180172,"conditionOfUse":"MIT Open","knowledgeDimension":"CONCEPTUAL","difficultyLevel":"MEDIUM","language":"ENGLISH","lexicalLevel":"L3","mediaType":"IMAGE","status":"DRAFT","contentLocation":"2e/e2/b8/2a/29546","author":"Fahim","authoredDate":"2020-04-15","createdAt":1586940180172,"createdBy":33,"contentLearningResourceTypes":["DOK1"],"cognitiveDimensions":["UNDERSTANDING","REMEMBERING"],"copyrights":["ALEF","SHUTTERSTOCK","UAE_NATIONAL_ARCHIVES"],"occurredOn":1586940180326}}
        | {"headers":{"eventType":"ContentDeletedEvent"},"body":{"contentId":29546,"occurredOn":1586940514397}}
        | {"headers":{"eventType":"ContentPublishedEvent"},"body":{"contentId":29547,"publishedDate":"2020-04-15","publisherId":33,"publisherName":"Fahim","occurredOn":1586940704367}}
        | {"headers":{"eventType":"ContentOutcomeAttachedEvent"},"body":{"contentId":29548,"outcomeId":"substandard-103583","occurredOn":1586940811995}}
        | {"headers":{"eventType":"ContentOutcomeAttachedEvent"},"body":{"contentId":29546,"outcomeId":"substandard-66764","occurredOn":1586940180326}}
        | {"headers":{"eventType":"ContentOutcomeDetachedEvent"},"body":{"contentId":29550,"outcomeId":"substandard-611862","occurredOn":1587059485376}}
            """.stripMargin

    val fixtures = List(
      SparkFixture(
        key = source,
        value = buildKafkaEvents(eventsJson)
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        testSink(sinks, "ccl-content-created-sink", contentCreatedSchema)
        testSink(sinks, "ccl-content-updated-sink", contentUpdatedSchema)
        testSink(sinks, "ccl-content-deleted-sink", contentDeletedSchema)
        testSink(sinks, "ccl-content-published-sink", contentPublishedSchema)
        testSink(sinks, "ccl-content-outcome-attach-sink", contentOutcomeAttachSchema, countOfEvents = 3)

      }
    )
  }

  test("handle CCL Lesson content attached event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |   "headers":{
              |      "eventType":"LessonContentAttachedEvent"
              |   },
              |   "body":{
              |      "lessonId":7197,
              |      "lessonUuid":"6e19580d-8764-4b46-a781-000000007196",
              |      "contentId":33260,
              |      "stepId":1,
              |      "stepUuid":"7d19580d-8764-4b46-a781-76800000487",
              |      "occurredOn":1588076030227
              |   }
              | },
              | "timestamp": "2020-04-28 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val lessonContentAttachedSinks = sinks
            .find(_.name == "ccl-lesson-content-attach-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-lesson-content-attach-sink is not found"))

          lessonContentAttachedSinks.columns.toSet shouldBe expectedLessonContent
          val lessonContentAttached = lessonContentAttachedSinks.first()
          lessonContentAttached.getAs[String]("eventType") shouldBe "LessonContentAttachedEvent"
          lessonContentAttached.getAs[Long]("lessonId") shouldBe 7197
          lessonContentAttached.getAs[String]("lessonUuid") shouldBe "6e19580d-8764-4b46-a781-000000007196"
          lessonContentAttached.getAs[Long]("contentId") shouldBe 33260
          lessonContentAttached.getAs[Long]("stepId") shouldBe 1
          lessonContentAttached.getAs[String]("stepUuid") shouldBe "7d19580d-8764-4b46-a781-76800000487"
          lessonContentAttached.getAs[String]("loadtime") shouldBe "2020-04-28 16:23:46.609"
          lessonContentAttached.getAs[String]("eventDateDw") shouldBe "20200428"
          lessonContentAttached.getAs[String]("occurredOn") shouldBe "2020-04-28 12:13:50.227"
        }
      )
    }
  }

  test("handle CCL Lesson content detached event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |   "headers":{
              |      "eventType":"LessonContentDetachedEvent"
              |   },
              |   "body":{
              |      "lessonId":7197,
              |      "lessonUuid":"6e19580d-8764-4b46-a781-000000007196",
              |      "contentId":33260,
              |      "stepId":1,
              |      "stepUuid":"7d19580d-8764-4b46-a781-76800000487",
              |      "occurredOn":1588076030227
              |   }
              | },
              | "timestamp": "2020-04-28 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val lessonContentAttachedSinks = sinks
            .find(_.name == "ccl-lesson-content-attach-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-lesson-content-attach-sink is not found"))

          lessonContentAttachedSinks.columns.toSet shouldBe expectedLessonContent
          val lessonContentAttached = lessonContentAttachedSinks.first()
          lessonContentAttached.getAs[String]("eventType") shouldBe "LessonContentDetachedEvent"
          lessonContentAttached.getAs[Long]("lessonId") shouldBe 7197
          lessonContentAttached.getAs[String]("lessonUuid") shouldBe "6e19580d-8764-4b46-a781-000000007196"
          lessonContentAttached.getAs[Long]("contentId") shouldBe 33260
          lessonContentAttached.getAs[Long]("stepId") shouldBe 1
          lessonContentAttached.getAs[String]("stepUuid") shouldBe "7d19580d-8764-4b46-a781-76800000487"
          lessonContentAttached.getAs[String]("loadtime") shouldBe "2020-04-28 16:23:46.609"
          lessonContentAttached.getAs[String]("eventDateDw") shouldBe "20200428"
          lessonContentAttached.getAs[String]("occurredOn") shouldBe "2020-04-28 12:13:50.227"
        }
      )
    }
  }

  test("handle CCL Lesson outcome attached event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |   "headers":{
              |      "eventType":"LessonOutcomeAttachedEvent"
              |   },
              |   "body":{
              |      "lessonId":6330,
              |      "lessonUuid":"a5762582-4ae7-4554-a7bf-000000006330",
              |      "outcomeId":"substandard-793672",
              |      "occurredOn":1587978336177
              |   }
              | },
              | "timestamp": "2020-04-28 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val lessonOutcomeAttachedSinks = sinks
            .find(_.name == "ccl-lesson-outcome-attach-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-lesson-outcome-attach-sink is not found"))

          lessonOutcomeAttachedSinks.columns.toSet shouldBe expectedLessonContentAttached
          val lessonOutcomeAttached = lessonOutcomeAttachedSinks.first()
          lessonOutcomeAttached.getAs[String]("eventType") shouldBe "LessonOutcomeAttachedEvent"
          lessonOutcomeAttached.getAs[Long]("lessonId") shouldBe 6330
          lessonOutcomeAttached.getAs[String]("lessonUuid") shouldBe "a5762582-4ae7-4554-a7bf-000000006330"
          lessonOutcomeAttached.getAs[String]("outcomeId") shouldBe "substandard-793672"
          lessonOutcomeAttached.getAs[String]("loadtime") shouldBe "2020-04-28 16:23:46.609"
          lessonOutcomeAttached.getAs[String]("eventDateDw") shouldBe "20200427"
          lessonOutcomeAttached.getAs[String]("occurredOn") shouldBe "2020-04-27 09:05:36.177"
        }
      )
    }
  }

  test("handle CCL Lesson outcome detached event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |   "headers":{
              |      "eventType":"LessonOutcomeDetachedEvent"
              |   },
              |   "body":{
              |      "lessonId":6330,
              |      "lessonUuid":"a5762582-4ae7-4554-a7bf-000000006330",
              |      "outcomeId":"substandard-793672",
              |      "occurredOn":1587978336177
              |   }
              | },
              | "timestamp": "2020-04-28 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val lessonOutcomeAttachedSinks = sinks
            .find(_.name == "ccl-lesson-outcome-attach-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-lesson-outcome-attach-sink is not found"))

          lessonOutcomeAttachedSinks.columns.toSet shouldBe expectedLessonContentAttached
          val lessonOutcomeAttached = lessonOutcomeAttachedSinks.first()
          lessonOutcomeAttached.getAs[String]("eventType") shouldBe "LessonOutcomeDetachedEvent"
          lessonOutcomeAttached.getAs[Long]("lessonId") shouldBe 6330
          lessonOutcomeAttached.getAs[String]("lessonUuid") shouldBe "a5762582-4ae7-4554-a7bf-000000006330"
          lessonOutcomeAttached.getAs[String]("outcomeId") shouldBe "substandard-793672"
          lessonOutcomeAttached.getAs[String]("loadtime") shouldBe "2020-04-28 16:23:46.609"
          lessonOutcomeAttached.getAs[String]("eventDateDw") shouldBe "20200427"
          lessonOutcomeAttached.getAs[String]("occurredOn") shouldBe "2020-04-27 09:05:36.177"
        }
      )
    }
  }

  test("handle CCL Lesson Skill Linked event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |     "headers": {
                    |       "eventType": "LessonSkillLinkedEvent"
                    |     },
                    |     "body": {
                    |       "lessonId": 234567,
                    |       "lessonUuid": "995afcba-e214-4d3c-a84a-000000234567",
                    |       "skillId": "b42765ca-3f4c-44fb-8ec7-f96042c93703",
                    |       "derived": true,
                    |       "occurredOn": 1588076030227
                    |     }
                    |   },
                    | "timestamp": "2020-04-28 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-lesson-skill-link-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-lesson-skill-link-sink is not found"))

          sink.columns.toSet shouldBe expectedColsForCclLessonSkill
          val df = sink.first()
          df.getAs[String]("eventType") shouldBe LessonSkillLinkedEvent
          df.getAs[Long]("lessonId") shouldBe 234567
          df.getAs[String]("lessonUuid") shouldBe "995afcba-e214-4d3c-a84a-000000234567"
          df.getAs[String]("skillId") shouldBe "b42765ca-3f4c-44fb-8ec7-f96042c93703"
          df.getAs[Boolean]("derived") shouldBe true
          df.getAs[String]("loadtime") shouldBe "2020-04-28 16:23:46.609"
          df.getAs[String]("eventDateDw") shouldBe "20200428"
          df.getAs[String]("occurredOn") shouldBe "2020-04-28 12:13:50.227"
        }
      )
    }
  }

  test("handle CCL Lesson Skill Unlinked event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |     "headers": {
                    |       "eventType": "LessonSkillUnlinkedEvent"
                    |     },
                    |     "body": {
                    |       "lessonId": 234567,
                    |       "lessonUuid": "995afcba-e214-4d3c-a84a-000000234567",
                    |       "skillId": "b42765ca-3f4c-44fb-8ec7-f96042c93703",
                    |       "derived": true,
                    |       "occurredOn": 1588076030227
                    |     }
                    |   },
                    | "timestamp": "2020-04-28 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-lesson-skill-link-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-lesson-skill-link-sink is not found"))

          val expectedCols: Set[String] = Set(
            "eventType",
            "eventDateDw",
            "loadtime",
            "occurredOn",
            "derived",
            "skillId",
            "lessonUuid",
            "lessonId"
          )

          sink.columns.toSet shouldBe expectedColsForCclLessonSkill
          val df = sink.first()
          df.getAs[String]("eventType") shouldBe LessonSkillUnlinkedEvent
          df.getAs[Long]("lessonId") shouldBe 234567
          df.getAs[String]("lessonUuid") shouldBe "995afcba-e214-4d3c-a84a-000000234567"
          df.getAs[String]("skillId") shouldBe "b42765ca-3f4c-44fb-8ec7-f96042c93703"
          df.getAs[Boolean]("derived") shouldBe true
          df.getAs[String]("loadtime") shouldBe "2020-04-28 16:23:46.609"
          df.getAs[String]("eventDateDw") shouldBe "20200428"
          df.getAs[String]("occurredOn") shouldBe "2020-04-28 12:13:50.227"
        }
      )
    }
  }

  test("handle CCL Curriculum Created/GradeCreated/SubjectCreated/SubjectUpdated/SubjectDeleted/ events") {

    implicit val transformer: CCLEvents = new CCLEvents(spark)
    val eventsJson =
      """
        |{"headers":{"eventType":"CurriculumCreatedEvent"},"body":{"curriculumId":43,"name":"curiulum","organisation": "MORA","occurredOn":1588837592974}}
        |{"headers":{"eventType":"CurriculumGradeCreatedEvent"},"body":{"gradeId":123,"name":"Grade","occurredOn":1588837554151}}
        |{"headers":{"eventType":"CurriculumSubjectCreatedEvent"},"body":{"subjectId":123,"name":"Sam Subj","skillable":true,"occurredOn":1589105351351}}
        |{"headers":{"eventType":"CurriculumSubjectCreatedEvent"},"body":{"subjectId":1234,"name":"Sam Subj 2","skillable":true,"occurredOn":1589105363816}}
        |{"headers":{"eventType":"CurriculumSubjectCreatedEvent"},"body":{"subjectId":245,"name":"New Test SSubject1","skillable":true,"occurredOn":1589178405824}}
        |{"headers":{"eventType":"CurriculumSubjectUpdatedEvent"},"body":{"subjectId":1234,"name":"Sam Subj Test 2","skillable":true,"occurredOn":1589105378057}}
        |{"headers":{"eventType":"CurriculumSubjectUpdatedEvent"},"body":{"subjectId":123,"name":"Sam Subj12 test","skillable":true,"occurredOn":1589105402941}}
        |{"headers":{"eventType":"CurriculumSubjectUpdatedEvent"},"body":{"subjectId":245,"name":"New Test Subject1","skillable":true,"occurredOn":1589178418420}}
        |{"headers":{"eventType":"CurriculumSubjectDeletedEvent"},"body":{"subjectId":123,"occurredOn":1589105416151}}
        |{"headers":{"eventType":"CurriculumSubjectDeletedEvent"},"body":{"subjectId":245,"occurredOn":1589178426207}}
            """.stripMargin

    val fixtures = List(
      SparkFixture(
        key = source,
        value = buildKafkaEvents(eventsJson)
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        testSink(sinks, "ccl-curriculum-created-event-sink", CurriculumCreatedSchema)
        testSink(sinks, "ccl-curriculum-grade-created-event-sink", CurriculumGradeCreatedSchema)
        testSink(sinks, "ccl-curriculum-subject-created-event-sink", CurriculumSubjectCreatedSchema, countOfEvents = 3)
        testSink(sinks, "ccl-curriculum-subject-updated-event-sink", CurriculumSubjectUpdatedSchema, countOfEvents = 3)
        testSink(sinks, "ccl-curriculum-subject-deleted-event-sink", CurriculumSubjectDeletedSchema, countOfEvents = 2)

      }
    )
  }

  test("handle CCL Theme Created/Updated/Deleted events") {

    implicit val transformer: CCLEvents = new CCLEvents(spark)
    val eventsJson =
      """
        |{"headers":{"eventType":"ThemeCreatedEvent"},"body":{"id":21,"name":"DemoTheme","description":"DemoTheme Desc","boardId":392027,"gradeId":322135,"subjectId":571671,"thumbnailFileName":"21_thumb.png","thumbnailContentType":"image/png","thumbnailFileSize":344022,"thumbnailLocation":"3c/59/dc/21","thumbnailUpdatedAt":1586342333335,"createdAt":1586342333335,"occurredOn":1586342335109}}
        |{"headers":{"eventType":"ThemeUpdatedEvent"},"body":{"id":21,"name":"DemoTheme","description":"DemoTheme Desc","boardId":392027,"gradeId":322135,"subjectId":571671,"thumbnailFileName":"21_thumb.png","thumbnailContentType":"image/png","thumbnailFileSize":344022,"thumbnailLocation":"3c/59/dc/21","thumbnailUpdatedAt":1586342333335,"updatedAt":1586342333335,"occurredOn":1586342335109}}
        |{"headers":{"eventType":"ThemeDeletedEvent"},"body":{"id":21,"occurredOn":1586940514397}}
        |""".stripMargin

    val fixtures = List(
      SparkFixture(
        key = source,
        value = buildKafkaEvents(eventsJson)
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        testSink(sinks, "ccl-theme-created-sink", themeCreatedSchema)
        testSink(sinks, "ccl-theme-updated-sink", themeUpdatedSchema)
        testSink(sinks, "ccl-theme-deleted-sink", themeDeletedSchema)
      }
    )
  }

  test("handle CCL Template Created/Updated events") {

    implicit val transformer: CCLEvents = new CCLEvents(spark)
    val eventsJson =
      """
        |{"headers":{"eventType":"LessonTemplateCreatedEvent"},"body":{"id":21,"frameworkId":32,"title":"New Template","organisation":"shared-create","description":"Demo Template Desc","steps":[{"id":12,"displayName":"name","stepId":"ANTICIPATORY_CONTENT","abbreviation":"abbr"},{"id":14,"displayName":"name2","stepId":"PREREQUISITE","abbreviation":"abbr2"}],"createdAt":1586342333335,"occurredOn":1586342335109}}
        |{"headers":{"eventType":"LessonTemplateUpdatedEvent"},"body":{"id":21,"frameworkId":32,"title":"New Template","organisation":"shared-update","description":"Demo Template Desc","steps":[{"id":12,"displayName":"name","stepId":"SUMMATIVE_ASSESSMENT","abbreviation":"abbr"},{"id":14,"displayName":"name2","stepId":"REMEDIATION_2","abbreviation":"abbr2"}],"updatedAt":1586342333335,"occurredOn":1586342335109}}
        |""".stripMargin

    val fixtures = List(
      SparkFixture(
        key = source,
        value = buildKafkaEvents(eventsJson)
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        testSink(sinks, "ccl-lesson-template-created-sink", templateCreatedSchema)
        testSink(sinks, "ccl-lesson-template-updated-sink", templateUpdatedSchema)
      }
    )
  }

  test("handle CCL Question Created/Updated events") {

    implicit val transformer = new CCLEvents(spark)
    val eventsJson =
      """
        |[
        | {
        |   "key":"key1",
        |   "value":{"body":{"questionId":"f5c48a28-2998-407a-9cb5-453ee8cd9ff0","code":"TEST_POOL_2","version":0,"type":"MULTIPLE_CHOICE","variant":"EN_US","language":"EN_US","body":{"prompt":"<p>sdfhd</p>","choices":{"minChoice":1,"maxChoice":1,"layoutColumns":1,"shuffle":true,"listType":"NONE","choiceItems":[{"feedback":"","choiceId":170,"weight":100,"answer":"<p>gj</p>"},{"feedback":"","choiceId":0,"weight":0,"answer":"<p>dfgjfgj</p>"},{"feedback":"","choiceId":724,"weight":0,"answer":"<p>fdgjf</p>"},{"feedback":"","choiceId":820,"weight":0,"answer":"<p>fdgjgdf</p>"},{"feedback":"","choiceId":391,"weight":0,"answer":"<p>zxcbcxb</p>"}]},"hints":[],"generalFeedback":"","correctAnswerFeedback":"","wrongAnswerFeedback":"","_class":"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceBody"},"validation":{"validResponse":{"choiceIds":[170],"_class":"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidResponse"},"scoringType":"EXACT_MATCH","_class":"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidation"},"stage":"IN_PROGRESS","maxScore":1,"metadata":{"keywords":["dfgh"],"resourceType":"TEQ1","summativeAssessment":true,"difficultyLevel":"EASY","cognitiveDimensions":["UNDERSTANDING"],"knowledgeDimensions":"FACTUAL","lexileLevel":"K","lexileLevels":["1","2"],"copyrights":[],"conditionsOfUse":[],"formatType":"QUESTION","author":"Rashid","authoredDate":"2020-09-01T00:00:00.000","curriculumOutcomes":[{"type":"sub_standard","id":"2","name":"asdg","description":"sfdg","curriculum":"UAE MOE","grade":"6","subject":"Math"}],"cefrLevel":"A2","proficiency":"READING"},"createdBy":"rashid@alefeducation.com","createdAt":1598957337602,"updatedBy":"rashid@alefeducation.com","updatedAt":1598957337602,"occurredOn":1598957337602,"triggeredBy":"9904","eventType":"QuestionCreatedEvent","_class":"com.alefeducation.assessmentquestion.event.QuestionEvent","id":"ee27c2a2-b9d5-48cb-b10b-790d69680c97"}},
        |   "headers":[{"key":"eventType","value":"QuestionCreatedEvent"}],
        |   "timestamp":"2022-09-27 16:23:46.609"
        | },
        | {
        |   "key":"key1",
        |   "value":{"body":{"questionId":"0a910267-e296-49d5-8ac7-57ea98ac549d","code":"test123_1","version":1,"type":"MULTIPLE_CHOICE","variant":"EN_GB","language":"EN_GB","body":{"prompt":"<p>sdgg</p>","choices":{"minChoice":1,"maxChoice":1,"layoutColumns":1,"shuffle":true,"listType":"NONE","choiceItems":[{"feedback":"","choiceId":86,"weight":0.0,"answer":"<p>test</p>"},{"feedback":"","choiceId":0,"weight":100.0,"answer":"<p>test</p>"},{"feedback":"","choiceId":40,"weight":0.0,"answer":"<p>test</p>"},{"feedback":"","choiceId":118,"weight":0.0,"answer":"<p>test</p>"}]},"hints":[],"generalFeedback":"","correctAnswerFeedback":"","wrongAnswerFeedback":"","_class":"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceBody"},"validation":{"validResponse":{"choiceIds":[0],"_class":"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidResponse"},"scoringType":"EXACT_MATCH","_class":"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidation"},"stage":"ARCHIVED","maxScore":1,"metadata":{"keywords":["sdf"],"resourceType":"TEQ1","summativeAssessment":true,"difficultyLevel":"EASY","cognitiveDimensions":["ANALYZING"],"knowledgeDimensions":"CONCEPTUAL","lexileLevel":"K","lexileLevels":["1","2"],"copyrights":[],"conditionsOfUse":[],"formatType":"QUESTION","author":"test","authoredDate":"2020-08-25T00:00:00.000","curriculumOutcomes":[{"type":"sub_standard","id":"63434","name":"MATH.CONTENT.6.NS.A.1","description":"Interpret and compute quotients of fractions, and solve word problems involving division of fractions by fractions, e.g., by using visual fraction models and equations to represent the problem. For example, create a story context for (2/3) ÷ (3/4) and use a visual fraction model to show the quotient; use the relationship between multiplication and division to explain that (2/3) ÷ (3/4) = 8/9 because 3/4 of 8/9 is 2/3. (In general, (a/b) ÷ (c/d) = ad/bc.) How much chocolate will each person get if 3 people share 1/2 lb of chocolate equally? How many 3/4-cup servings are in 2/3 of a cup of yogurt? How wide is a rectangular strip of land with length 3/4 mi and area 1/2 square mi?.\r\n","curriculum":"UAE MOE","grade":"6","subject":"Math"}]},"createdBy":"test@alef.com","createdAt":1598360699538,"updatedBy":"test@alef.com","updatedAt":1598856307737,"occurredOn":1598856307737,"triggeredBy":"29","eventType":"QuestionUpdatedEvent","_class":"com.alefeducation.assessmentquestion.event.QuestionEvent","id":"25df0801-c0ed-40e2-8e0c-0dd31d8339c4"}},
        |   "headers":[{"key":"eventType","value":"QuestionUpdatedEvent"}],
        |   "timestamp":"2022-09-27 16:23:46.609"
        | }
        |]
        |""".stripMargin

    val fixtures = List(
      SparkFixture(
        key = source,
        value = eventsJson
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val sink = sinks
          .find(_.name == "ccl-question-mutated-sink")
          .map(_.output)
          .getOrElse(throw new RuntimeException("ccl-question-mutated-sink is not found"))
        val df = sink.first()

        val dfSchema = df.schema.fields.map(f => f.name)
        val questionBodySchema: StructType = schemaAlias[QuestionBody]
        val providedSchema = questionBodySchema.fields.map(f => f.name) ++ Seq("eventType", "eventDateDw", "loadtime", "_headers")

        dfSchema.diff(providedSchema) shouldBe empty
        providedSchema.diff(dfSchema) shouldBe empty
        sink.count() shouldBe 2
      }
    )
  }

  test("handle CCL Pool Created/Updated events") {

    implicit val transformer: CCLEvents = new CCLEvents(spark)
    val eventsJson =
      """
        |{"headers":{"eventType":"PoolCreatedEvent"},"body":{"triggeredBy": "29","name":"test123","questionCodePrefix":"test123_","status":"DRAFT","createdBy":"29","updatedBy":"29","createdAt":1598360607238,"updatedAt":1598360607238,"occurredOn":1598360607238,"poolId":"5f450c1f70b7d80001d3a128","eventType":"PoolCreatedEvent","partitionKey":"5f450c1f70b7d80001d3a128","_class":"com.alefeducation.assessmentlibrary.event.PoolCreatedEvent","id":"5085098c-7deb-41be-afa1-4225332ea52b"}}
        |{"headers":{"eventType":"PoolUpdatedEvent"},"body":{"triggeredBy": "29","name":"test123","questionCodePrefixNew":"test123_","status":"DRAFT","createdBy":"29","updatedBy":"29","createdAt":1598360607238,"updatedAt":1598360607238,"occurredOn":1598360608001,"poolId":"5f450c1f70b7d80001d3a128","eventType":"PoolUpdatedEvent","partitionKey":"5f450c1f70b7d80001d3a128","_class":"com.alefeducation.assessmentlibrary.event.PoolCreatedEvent","id":"5085098c-7deb-41be-afa1-4225332ea52b"}}
        |""".stripMargin

    val fixtures = List(
      SparkFixture(
        key = source,
        value = buildKafkaEvents(eventsJson)
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        testSink(sinks, "ccl-question-pool-mutated-sink", questionPoolSchema, 2)
      }
    )
  }

  test("handle CCL Questions To Pool Added/Removed events") {

    implicit val transformer: CCLEvents = new CCLEvents(spark)
    val eventsJson =
      """
        |{"headers":{"eventType":"QuestionsAddedToPoolEvent"},"body":{"triggeredBy":"29","questions":[{"code":"test123_1"}],"occurredOn":1598360766617,"poolId":"5f450c1f70b7d80001d3a128","eventType":"QuestionsRemovedFromPoolEvent","partitionKey":"5f450c1f70b7d80001d3a128","_class":"com.alefeducation.assessmentlibrary.event.QuestionsAddedToPoolEvent","id":"5f3578a1-e1c2-4b2f-817c-c29bb828a152"}}
        |{"headers":{"eventType":"QuestionsRemovedFromPoolEvent"},"body":{"triggeredBy":"29","questions":[{"code":"test123_1"}],"occurredOn":1598360766617,"poolId":"5f450c1f70b7d80001d3a128","eventType":"QuestionsRemovedFromPoolEvent","partitionKey":"5f450c1f70b7d80001d3a128","_class":"com.alefeducation.assessmentlibrary.event.QuestionsRemovedFromPoolEvent","id":"5f3578a1-e1c2-4b2f-817c-c29bb828a152"}}
        |""".stripMargin

    val fixtures = List(
      SparkFixture(
        key = source,
        value = buildKafkaEvents(eventsJson)
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        testSink(sinks, "ccl-question-pool-association-sink", questionPoolAssociationSchema, 2)
      }
    )
  }

  test("handle CCL Skill Created event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"SkillCreatedEvent"
                    |   },
                    |   "body":{
                    |      "id": "78657a5e-8a1f-49a3-8610-b579c9e6d520",
                    |      "code": "SK.2176",
                    |      "name": "test_new_skill_subject",
                    |      "description": "test_new_skill_subject",
                    |      "subjectId": 571671,
                    |      "translations": [
                    |         {"uuid": "6fb869d4-6373-4310-b58e-d9324fe01db5", "languageCode": "AR", "value": "سمي هو خان", "name": "سمي هو خان", "description": "translated description"}
                    |      ],
                    |      "occurredOn": 1577773315596
                    |   }
                    | },
                    | "timestamp": "2019-04-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val cclSkillCreatedSink = sinks
            .find(_.name == "ccl-skill-mutated-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-skill-mutated-sink is not found"))

          cclSkillCreatedSink.columns.toSet shouldBe expectedSkillCreatedColumns
        }
      )
    }
  }

  test("handle CCL Skill Updated event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"SkillUpdatedEvent"
                    |   },
                    |   "body":{
                    |      "id": "78657a5e-8a1f-49a3-8610-b579c9e6d520",
                    |      "code": "SK.2176",
                    |      "name": "test_new_skill_subject",
                    |      "description": "test_new_skill_subject",
                    |      "subjectId": 571671,
                    |      "translations": [
                    |         {"uuid": "6fb869d4-6373-4310-b58e-d9324fe01db5", "languageCode": "AR", "value": "سمي هو خان",  "name": "سمي هو خان", "description": "translated description"}
                    |      ],
                    |      "occurredOn": 1577773315596
                    |   }
                    | },
                    | "timestamp": "2019-04-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val cclSkillUpdatedSink = sinks
            .find(_.name == "ccl-skill-mutated-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-skill-mutated-sink is not found"))

          cclSkillUpdatedSink.columns.toSet shouldBe expectedSkillCreatedColumns
        }
      )
    }
  }

  test("handle CCL Skill Deleted event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"SkillDeletedEvent"
                    |   },
                    |   "body":{
                    |      "id": "78657a5e-8a1f-49a3-8610-b579c9e6d520",
                    |      "occurredOn": 1577773315596
                    |   }
                    | },
                    | "timestamp": "2019-04-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val cclSkillDeletedSink = sinks
            .find(_.name == "ccl-skill-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-skill-deleted-sink is not found"))

          val expectedSkillDeletedColumns: Set[String] = Set(
            "id",
            "loadtime",
            "occurredOn",
            "eventDateDw",
            "eventType"
          )

          cclSkillDeletedSink.columns.toSet shouldBe expectedSkillDeletedColumns
        }
      )
    }
  }

  test("handle CCL Skill Linked event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"SkillLinkedEvent"
                    |   },
                    |   "body":{
                    |      "skillId": "d9e8fd22-b20c-4699-8921-814f2bc90b1a",
                    |      "skillCode": "SK.1457",
                    |      "nextSkillId": "dc6606ce-3fc9-48aa-893a-da6faa979c77",
                    |      "nextSkillCode": "SK.1458",
                    |      "occurredOn": 1573027271
                    |   }
                    | },
                    | "timestamp": "2019-04-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val cclSkillLinkedSink = sinks
            .find(_.name == "ccl-skill-linked-unlinked-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-skill-linked-unlinked-sink is not found"))
          cclSkillLinkedSink.columns.toSet shouldBe expectedSkillLinkedColumns
        }
      )
    }
  }

  test("handle CCL Skill UnLinked event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"SkillUnLinkedEvent"
                    |   },
                    |   "body":{
                    |      "skillId": "d9e8fd22-b20c-4699-8921-814f2bc90b1a",
                    |      "skillCode": "SK.1457",
                    |      "nextSkillId": "dc6606ce-3fc9-48aa-893a-da6faa979c77",
                    |      "nextSkillCode": "SK.1458",
                    |      "occurredOn": 1573027271
                    |   }
                    | },
                    | "timestamp": "2019-04-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val cclSkillUnLinkedSink = sinks
            .find(_.name == "ccl-skill-linked-unlinked-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-skill-linked-unlinked-sink is not found"))

          cclSkillUnLinkedSink.columns.toSet shouldBe expectedSkillLinkedColumns
        }
      )
    }
  }

  test("handle CCL Skill Linked event V2") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"SkillLinkedEventV2"
                    |   },
                    |   "body":{
                    |      "skillId":"b961ee38-b4a2-4888-93e3-ae770d8a2d08",
                    |      "skillCode":"SK.169",
                    |      "previousSkillId":"4b453063-82db-4cbd-8fbc-9a98a6077220",
                    |      "previousSkillCode":"SK.155",
                    |      "occurredOn":1647237971299
                    |   }
                    | },
                    | "timestamp": "2019-04-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val cclSkillLinkedSink = sinks
            .find(_.name == "ccl-skill-linked-unlinked-v2-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-skill-linked-unlinked-v2-sink is not found"))

          cclSkillLinkedSink.columns.toSet shouldBe expectedSkillLinkedColumnsV2
        }
      )
    }
  }

  test("handle CCL Skill UnLinked event V2") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"SkillUnlinkedEventV2"
                    |   },
                    |   "body":{
                    |      "skillId":"9d792be4-0c86-4227-967c-f1c8e3fdbb56",
                    |      "skillCode":"SK.159",
                    |      "previousSkillId":"36bee00a-61ad-4b42-bd41-cb74e94f1192",
                    |      "previousSkillCode":"SK.156",
                    |      "occurredOn":1647235894114
                    |   }
                    | },
                    | "timestamp": "2019-04-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val cclSkillUnLinkedSink = sinks
            .find(_.name == "ccl-skill-linked-unlinked-v2-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-skill-linked-unlinked-v2-sink is not found"))

          cclSkillUnLinkedSink.columns.toSet shouldBe expectedSkillLinkedColumnsV2
        }
      )
    }
  }

  test("handle CCL Skill Category Linked event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"SkillCategoryLinkedEvent"
                    |   },
                    |   "body":{
                    |      "skillId": "d9e8fd22-b20c-4699-8921-814f2bc90b1a",
                    |      "skillCode": "SK.1457",
                    |      "categoryId": "2d515690-cf4b-4fff-ac29-20006d20a9f0",
                    |      "occurredOn": 1573027271
                    |   }
                    | },
                    | "timestamp": "2019-04-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val cclSkillCategoryLinkedSink = sinks
            .find(_.name == "ccl-skill-category-link-unlink-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-skill-category-link-unlink-sink is not found"))

          val expectedSkillCategoryLinkedColumns: Set[String] = Set(
            "skillId",
            "skillCode",
            "categoryId",
            "loadtime",
            "occurredOn",
            "eventDateDw",
            "eventType"
          )

          cclSkillCategoryLinkedSink.columns.toSet shouldBe expectedSkillCategoryLinkedColumns
        }
      )
    }
  }

  test("handle CCL Skill Category UnLinked event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"SkillCategoryUnLinkedEvent"
                    |   },
                    |   "body":{
                    |      "skillId": "d9e8fd22-b20c-4699-8921-814f2bc90b1a",
                    |      "skillCode": "SK.1457",
                    |      "categoryId": "2d515690-cf4b-4fff-ac29-20006d20a9f0",
                    |      "occurredOn": 1573027271
                    |   }
                    | },
                    | "timestamp": "2019-04-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val cclSkillCategoryUnLinkedSink = sinks
            .find(_.name == "ccl-skill-category-link-unlink-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-skill-category-link-unlink-sink is not found"))

          val expectedSkillCategoryUnLinkedColumns: Set[String] = Set(
            "skillId",
            "skillCode",
            "categoryId",
            "loadtime",
            "occurredOn",
            "eventDateDw",
            "eventType"
          )

          cclSkillCategoryUnLinkedSink.columns.toSet shouldBe expectedSkillCategoryUnLinkedColumns
        }
      )
    }
  }

  test("handle CCL Outcome skill Attached/Detached events") {

    implicit val transformer: CCLEvents = new CCLEvents(spark)
    val eventsJson =
      """
        |{"headers":{"eventType":"OutcomeSkillAttachedEvent"},"body":{"outcomeId":"sublevel-191590","skillId":"4fd6eed5-1bf7-40fc-9ae2-340fbebf7a9e","occurredOn":1600024651943}}
        |{"headers":{"eventType":"OutcomeSkillDetachedEvent"},"body":{"outcomeId":"sublevel-191590","skillId":"4fd6eed5-1bf7-40fc-9ae2-340fbebf7a9e","occurredOn":1600024673254}}
        |""".stripMargin

    val fixtures = List(
      SparkFixture(
        key = source,
        value = buildKafkaEvents(eventsJson)
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        testSink(sinks, "ccl-outcome-skill-attach-sink", outcomeSkillLinkSchema, 2)
      }
    )
  }

  test("handle CCL Outcome category Attached/Detached events") {

    implicit val transformer: CCLEvents = new CCLEvents(spark)
    val eventsJson =
      """
        |{"headers":{"eventType":"OutcomeCategoryAttachedEvent"},"body":{"outcomeId":"strand-983141","categoryId":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","occurredOn":1600024697334}}
        |{"headers":{"eventType":"OutcomeCategoryDetachedEvent"},"body":{"outcomeId":"strand-983141","categoryId":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","occurredOn":1600024709212}}
        |""".stripMargin

    val fixtures = List(
      SparkFixture(
        key = source,
        value = buildKafkaEvents(eventsJson)
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        testSink(sinks, "ccl-outcome-category-attach-sink", outcomeCategoryLinkSchema, 2)
      }
    )
  }

  test("handle Alef AssignmentCreatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "type",
        "metadata",
        "eventDateDw",
        "createdOn",
        "maxScore",
        "updatedOn",
        "isGradeable",
        "description",
        "schoolId",
        "loadtime",
        "publishedOn",
        "occurredOn",
        "id",
        "language",
        "status",
        "createdBy",
        "attachment",
        "title",
        "updatedBy",
        "attachmentRequired",
        "commentRequired"
      )
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"AssignmentCreatedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "id": "assignmentId_1",
                    |       "type":"TEACHER_ASSIGNMENT",
                    |       "title": "assignment-title1",
                    |       "description": "assignment-description1",
                    |       "maxScore": 100.0,
                    |       "attachment": {
                    |           "fileName": "file-name1.txt",
                    |           "path": "/upload-path1"
                    |        },
                    |        "isGradeable": true,
                    |        "schoolId": "school-UUID",
                    |        "language": "ENGLISH",
                    |        "status": "DRAFT",
                    |        "createdBy": "createdById",
                    |        "updatedBy": "updatedById",
                    |        "createdOn": 1582026526000,
                    |        "updatedOn": 1582026526000,
                    |        "publishedOn": 1582026526000,
                    |        "metadata":null,
                    |        "occurredOn": 1582026526000
                    |     }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.609"
                    |}
                    |,
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"AssignmentCreatedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "id": "assignmentId_2",
                    |       "title": "assignment-title2",
                    |       "type":"TEACHER_ASSIGNMENT",
                    |       "description": "assignment-description2",
                    |       "maxScore": 100.0,
                    |       "attachment": {
                    |           "fileName": "file-name2.txt",
                    |           "path": "/upload-path2"
                    |        },
                    |        "isGradeable": true,
                    |        "schoolId": "school-UUID",
                    |        "language": "ARABIC",
                    |        "status": "PUBLISHED",
                    |        "createdBy": "createdById",
                    |        "updatedBy": "updatedById",
                    |        "createdOn": 1582026527000,
                    |        "updatedOn": 1582026527000,
                    |        "publishedOn": 1582026527000,
                    |        "metadata":null,
                    |        "occurredOn": 1582026527000
                    |     }
                    | },
                    | "timestamp": "2020-02-20 16:23:56.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "ccl-assignment-mutated-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(2)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "AssignmentCreatedEvent"
            fst.getAs[String]("id") shouldBe "assignmentId_1"
            fst.getAs[String]("title") shouldBe "assignment-title1"
            fst.getAs[String]("description") shouldBe "assignment-description1"
            fst.getAs[Int]("maxScore") shouldBe 100.0
            fst.getAs[Boolean]("isGradeable") shouldBe true
            fst.getAs[String]("language") shouldBe "ENGLISH"
            fst.getAs[String]("status") shouldBe "DRAFT"
            fst.getAs[String]("createdBy") shouldBe "createdById"
            fst.getAs[Long]("createdOn") shouldBe 1582026526000L
            fst.getAs[Long]("updatedOn") shouldBe 1582026526000L
            fst.getAs[Long]("publishedOn") shouldBe 1582026526000L
            fst.getAs[String]("occurredOn") shouldBe "2020-02-18 11:48:46.000"

            val attachmentDf = df.select(col("attachment.*"))
            val attachmentRow = attachmentDf.first()
            attachmentRow.getAs[String]("fileName") shouldBe "file-name1.txt"
            attachmentRow.getAs[String]("path") shouldBe "/upload-path1"
          }
        }
      )
    }
  }

  test("handle Alef AssignmentUpdatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "type",
        "metadata",
        "eventDateDw",
        "createdOn",
        "maxScore",
        "updatedOn",
        "isGradeable",
        "description",
        "schoolId",
        "loadtime",
        "publishedOn",
        "occurredOn",
        "id",
        "language",
        "status",
        "createdBy",
        "attachment",
        "title",
        "updatedBy",
        "attachmentRequired",
        "commentRequired"
      )
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"AssignmentUpdatedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "id": "assignmentId_1",
                    |       "title": "assignment-title1",
                    |        "type":"TEACHER_ASSIGNMENT",
                    |       "description": "assignment-description1",
                    |       "maxScore": 100.0,
                    |       "attachment": {
                    |           "fileName": "file-name1.txt",
                    |           "path": "/upload-path1"
                    |        },
                    |        "isGradeable": true,
                    |        "schoolId": null,
                    |        "language": "ENGLISH",
                    |        "status": "DRAFT",
                    |        "createdBy": "createdById",
                    |        "updatedBy": "updatedById",
                    |        "createdOn": 1582026526000,
                    |        "updatedOn": 1582026526000,
                    |        "publishedOn": 1582026526000,
                    |        "metadata":null,
                    |        "occurredOn": 1582026526000
                    |     }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.609"
                    |}
                    |,
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"AssignmentUpdatedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "id": "assignmentId_2",
                    |       "title": "assignment-title2",
                    |        "type":"TEACHER_ASSIGNMENT",
                    |       "description": "assignment-description2",
                    |       "maxScore": 100.0,
                    |       "attachment": {
                    |           "fileName": "file-name2.txt",
                    |           "path": "/upload-path2"
                    |        },
                    |        "isGradeable": true,
                    |        "schoolId": "school-UUID",
                    |        "language": "ARABIC",
                    |        "status": "PUBLISHED",
                    |        "createdBy": "createdById",
                    |        "updatedBy": "updatedById",
                    |        "createdOn": 1582026527000,
                    |        "updatedOn": 1582026527000,
                    |        "publishedOn": 1582026527000,
                    |        "metadata":null,
                    |        "occurredOn": 1582026527000
                    |     }
                    | },
                    | "timestamp": "2020-02-20 16:23:56.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "ccl-assignment-mutated-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(2)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "AssignmentUpdatedEvent"
            fst.getAs[String]("id") shouldBe "assignmentId_1"
            fst.getAs[String]("title") shouldBe "assignment-title1"
            fst.getAs[String]("description") shouldBe "assignment-description1"
            fst.getAs[Int]("maxScore") shouldBe 100.0
            fst.getAs[Boolean]("isGradeable") shouldBe true
            fst.getAs[String]("language") shouldBe "ENGLISH"
            fst.getAs[String]("status") shouldBe "DRAFT"
            fst.getAs[String]("createdBy") shouldBe "createdById"
            fst.getAs[Long]("createdOn") shouldBe 1582026526000L
            fst.getAs[Long]("updatedOn") shouldBe 1582026526000L
            fst.getAs[Long]("publishedOn") shouldBe 1582026526000L
            fst.getAs[String]("schoolId") shouldBe null
            fst.getAs[String]("occurredOn") shouldBe "2020-02-18 11:48:46.000"

            val attachmentDf = df.select(col("attachment.*"))
            val attachmentRow = attachmentDf.first()
            attachmentRow.getAs[String]("fileName") shouldBe "file-name1.txt"
            attachmentRow.getAs[String]("path") shouldBe "/upload-path1"
          }
        }
      )
    }
  }

  test("handle Alef LessonAssessmentRuleAddedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "stepUuid",
        "loadtime",
        "rule",
        "occurredOn",
        "lessonUuid",
        "stepId",
        "lessonId"
      )
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"LessonAssessmentRuleAddedEvent"
                    |    },
                    |    "body":{
                    |       "lessonId": 234567,
                    |       "lessonUuid": "995afcba-e214-4d3c-a84a-000000234567",
                    |       "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |       "stepId": 1,
                    |       "rule": {
                    |           "id": "e07d4879-23be-4854-bd59-0a7647bd3f6b",
                    |           "poolId": "5f43c5f70e9eb20001907460",
                    |           "poolName": "pool 1",
                    |           "difficultyLevel": "EASY",
                    |           "resourceType": "TEQ1",
                    |           "questions": 4
                    |       },
                    |       "occurredOn": 1574256420357
                    |    }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "ccl-lesson-assessment-rule-association-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "LessonAssessmentRuleAddedEvent"
            fst.getAs[String]("lessonUuid") shouldBe "995afcba-e214-4d3c-a84a-000000234567"
            fst.getAs[String]("stepUuid") shouldBe "1ca1ab16-232c-42fc-9537-b9fe35fd207b"
            fst.getAs[Int]("stepId") shouldBe 1
            fst.getAs[String]("occurredOn") shouldBe "2019-11-20 13:27:00.357"

            val rule = df.select("rule.*").first()
            rule.getAs[String]("id") shouldBe "e07d4879-23be-4854-bd59-0a7647bd3f6b"
            rule.getAs[String]("poolId") shouldBe "5f43c5f70e9eb20001907460"
            rule.getAs[String]("poolName") shouldBe "pool 1"
            rule.getAs[String]("difficultyLevel") shouldBe "EASY"
            rule.getAs[String]("resourceType") shouldBe "TEQ1"
            rule.getAs[Int]("questions") shouldBe 4
          }
        }
      )
    }
  }

  test("handle Alef LessonAssessmentRuleUpdatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "stepUuid",
        "loadtime",
        "rule",
        "occurredOn",
        "lessonUuid",
        "stepId",
        "lessonId"
      )
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"LessonAssessmentRuleUpdatedEvent"
                    |    },
                    |    "body":{
                    |       "lessonId": 234567,
                    |       "lessonUuid": "995afcba-e214-4d3c-a84a-000000234567",
                    |       "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |       "stepId": 1,
                    |       "rule": {
                    |           "id": "e07d4879-23be-4854-bd59-0a7647bd3f6b",
                    |           "poolId": "5f43c5f70e9eb20001907460",
                    |           "poolName": "pool 2",
                    |           "difficultyLevel": "HARD",
                    |           "resourceType": "TEQ2",
                    |           "questions": 5
                    |       },
                    |       "occurredOn": 1574256420358
                    |    }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.709"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "ccl-lesson-assessment-rule-association-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "LessonAssessmentRuleUpdatedEvent"
            fst.getAs[String]("lessonUuid") shouldBe "995afcba-e214-4d3c-a84a-000000234567"
            fst.getAs[String]("stepUuid") shouldBe "1ca1ab16-232c-42fc-9537-b9fe35fd207b"
            fst.getAs[Int]("stepId") shouldBe 1
            fst.getAs[String]("occurredOn") shouldBe "2019-11-20 13:27:00.358"

            val rule = df.select("rule.*").first()
            rule.getAs[String]("id") shouldBe "e07d4879-23be-4854-bd59-0a7647bd3f6b"
            rule.getAs[String]("poolId") shouldBe "5f43c5f70e9eb20001907460"
            rule.getAs[String]("poolName") shouldBe "pool 2"
            rule.getAs[String]("difficultyLevel") shouldBe "HARD"
            rule.getAs[String]("resourceType") shouldBe "TEQ2"
            rule.getAs[Int]("questions") shouldBe 5
          }
        }
      )
    }
  }

  test("handle Alef LessonAssessmentRuleRemovedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "stepUuid",
        "loadtime",
        "rule",
        "occurredOn",
        "lessonUuid",
        "stepId",
        "lessonId"
      )
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"LessonAssessmentRuleRemovedEvent"
                    |    },
                    |    "body":{
                    |       "lessonId": 234567,
                    |       "lessonUuid": "995afcba-e214-4d3c-a84a-000000234567",
                    |       "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |       "stepId": 1,
                    |       "rule": {
                    |           "id": "e07d4879-23be-4854-bd59-0a7647bd3f6b",
                    |           "poolId": "5f43c5f70e9eb20001907460",
                    |           "poolName": "pool 2",
                    |           "difficultyLevel": "HARD",
                    |           "resourceType": "TEQ2",
                    |           "questions": 5
                    |       },
                    |       "occurredOn": 1574256420359
                    |    }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.709"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "ccl-lesson-assessment-rule-association-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "LessonAssessmentRuleRemovedEvent"
            fst.getAs[String]("lessonUuid") shouldBe "995afcba-e214-4d3c-a84a-000000234567"
            fst.getAs[String]("stepUuid") shouldBe "1ca1ab16-232c-42fc-9537-b9fe35fd207b"
            fst.getAs[Int]("stepId") shouldBe 1
            fst.getAs[String]("occurredOn") shouldBe "2019-11-20 13:27:00.359"

            val rule = df.select("rule.*").first()
            rule.getAs[String]("id") shouldBe "e07d4879-23be-4854-bd59-0a7647bd3f6b"
            rule.getAs[String]("poolId") shouldBe "5f43c5f70e9eb20001907460"
            rule.getAs[String]("poolName") shouldBe "pool 2"
            rule.getAs[String]("difficultyLevel") shouldBe "HARD"
            rule.getAs[String]("resourceType") shouldBe "TEQ2"
            rule.getAs[Int]("questions") shouldBe 5
          }
        }
      )
    }
  }

  test("handle Organization Created event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"OrganizationCreatedEvent"
                    |   },
                    |   "body":{
                    |       "id": "27aaaf29-4271-470f-9c16-871c8734ac43",
                    |	      "name": "testorg2sep21",
                    |	      "country": "AF",
                    |	      "code": "testorg2sep21",
                    |	      "tenantCode": "sharks",
                    |	      "isActive": true,
                    |	      "createdAt": "2022-09-21T13:08:52.286537831",
                    |	      "updatedAt": "2022-09-21T13:08:52.286537831",
                    |	      "createdBy": "99ef3952-e606-4be2-8a08-35b692a67219",
                    |	      "updatedBy": "99ef3952-e606-4be2-8a08-35b692a67219",
                    |       "occurredOn": 12345679012
                    |   }
                    | },
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val organizationCreatedSink = sinks
            .find(_.name == "ccl-organization-mutated-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-organization-mutated-sink is not found"))

          val expectedOrganizationColumns: Set[String] = Set(
            "id",
            "name",
            "country",
            "code",
            "tenantCode",
            "isActive",
            "createdAt",
            "updatedAt",
            "createdBy",
            "updatedBy",
            "occurredOn",
            "loadtime",
            "eventDateDw",
            "eventType"
          )

          organizationCreatedSink.columns.toSet shouldBe expectedOrganizationColumns
        }
      )
    }
  }

  test("handle InstructionlPlanPublished event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"InstructionalPlanPublishedEvent"
                    |   },
                    |   "body":{
                    |  "id": "15645134-4e2b-11ea-b77f-2e728ce88125",
                    |  "curriculumId": 392027,
                    |  "subjectId": 571671,
                    |  "gradeId": 333938,
                    |  "academicYearId": 2,
                    |  "organisationId": "25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
                    |  "status": "PUBLISHED",
                    |  "name": "UAEMOEMath7",
                    |  "code": "UAEMOEMath7code",
                    |  "academicYearName": "2021",
                    |  "createdOn": "2020-02-08T14:36:46.798+04:00",
                    |  "items": [
                    |    {
                    |      "lessonId": 10002,
                    |      "lessonUuid" : "485bdd28-4e2b-11ea-b77f-2e728ce88125",
                    |      "order": 1,
                    |      "weekId": "f391a340-4e2a-11ea-b77f-2e728ce88125",
                    |      "optional":false,
                    |      "instructorLed": true,
                    |      "defaultLocked": true
                    |    }],
                    |  "occurredOn": 1583916485652
                    |}
                    | },
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val instructionalPlanPublishedSink = sinks
            .find(_.name == "ccl-ip-published-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-ip-published-sink is not found"))

          val expectedIPColumns: Set[String] = Set(
            "eventType", "name", "eventDateDw", "createdOn", "loadtime", "items", "academicYearId", "contentRepositoryId", "subjectId", "occurredOn", "id", "organisationId", "status", "gradeId", "curriculumId", "code", "academicYearName"
          )

          instructionalPlanPublishedSink.columns.toSet shouldBe expectedIPColumns
        }
      )
    }
  }

  test("handle InstructionlPlanRePublished  event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"InstructionalPlanRePublishedEvent"
                    |   },
                    |   "body":{
                    |  "id": "1f97ef28-efaa-4330-9bdd-dbe490d727dc",
                    |  "curriculumId": 392027,
                    |  "subjectId": 571671,
                    |  "gradeId": 333938,
                    |  "academicYearId": 2,
                    |  "organisationId": "25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
                    |  "status": "PUBLISHED",
                    |  "name": "UAEMOEMath7",
                    |  "code": "UAEMOEMath7code",
                    |  "academicYearName": "2021",
                    |  "createdOn": "2020-03-16T14:38:28.318658+04:00",
                    |  "items": [
                    |    {
                    |      "lessonId": 10002,
                    |      "lessonUuid": "d3dc41d6-941d-41a1-bab2-4ce0e50c04f3",
                    |      "order": 1,
                    |      "weekId": "f391a340-4e2a-11ea-b77f-2e728ce88125",
                    |      "optional":false,
                    |      "instructorLed": true,
                    |      "defaultLocked": true
                    |    }
                    |  ],
                    |  "occurredOn": 1584355200660
                    |}
                    | },
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val instructionalPlanRePublishedSink = sinks
            .find(_.name == "ccl-ip-re-published-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-ip-re-published-sink is not found"))

          val expectedIPColumns: Set[String] = Set(
            "eventType", "name", "eventDateDw", "createdOn", "loadtime", "items", "academicYearId", "contentRepositoryId", "subjectId", "occurredOn", "id", "organisationId", "status", "gradeId", "curriculumId", "code", "academicYearName"
          )

          instructionalPlanRePublishedSink.columns.toSet shouldBe expectedIPColumns
        }
      )
    }
  }

  test("handle InstructionalPlanDeletedEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"InstructionalPlanDeletedEvent"
                    |   },
                    |   "body":{
                    |      "id": "1f97ef28-efaa-4330-9bdd-dbe490d727dc",
                    |       "items": [
                    |           "eb6bdafa-24ea-4409-bc5e-0a652637de0b",
                    |           "09ba865d-d9ff-4e07-8178-5f1fbcc135e5"
                    |       ],
                    |       "occurredOn": 1584355200660
                    |    }
                    | },
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val instructionalPlanDeletedSink = sinks
            .find(_.name == "ccl-ip-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-ip-deleted-sink is not found"))

          val expectedIPColumns: Set[String] = Set(
            "eventType", "eventDateDw", "loadtime", "items", "occurredOn", "id"
          )

          instructionalPlanDeletedSink.columns.toSet shouldBe expectedIPColumns
        }
      )
    }
  }

  test("handle termCreatedEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"TermCreatedEvent"
                    |   },
                    |   "body":{
                    |  "id": "c58ddf53-caa8-46f5-8281-ded48bd19cf0",
                    |  "number": 1,
                    |  "curriculumId": 392027,
                    |  "academicYearId": 3,
                    |  "organisationId": "25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
                    |  "contentRepositoryId": "25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
                    |  "startDate": "2021-01-02",
                    |  "endDate": "2021-01-28",
                    |  "weeks": [
                    |    {
                    |      "id": "badfce75-de01-4cf3-8e48-a0d2799847ac",
                    |      "number": 1,
                    |      "startDate": "2020-12-27"
                    |    }
                    |  ],
                    |  "createdOn": "2020-03-11T07:29:27.321825Z",
                    |  "occurredOn": 1583911767410
                    |}
                    | },
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val termCreatedSink = sinks
            .find(_.name == "ccl-term-created-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-term-created-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "number", "eventType", "eventDateDw", "createdOn", "endDate", "weeks", "loadtime", "academicYearId", "contentRepositoryId", "occurredOn", "id", "organisationId", "startDate", "curriculumId"
          )

          termCreatedSink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume contentRepositoryCreated events") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |        "id": "a1d2faac-8a27-4b07-a99f-59f87c9edc99",
                    |        "name": "MoRA",
                    |        "organisation": "shared",
                    |        "createdAt": "2022-10-05T11:19:54.2388148",
                    |        "createdById" : 1234,
                    |        "createdByName": "admin",
                    |        "description": "dummy desc",
                    |        "forSchool": "dummy school",
                    |        "contentTypes": ["ACTIVITY"],
                    |        "subjects": ["MATH", "ENGLISH"],
                    |        "grades": ["1", "2"],
                    |        "contentYear": "2024",
                    |        "occurredOn": 1664968794238
                    | },
                    | "headers": [
                    |     {
                    |       "key": "eventType",
                    |       "value": "ContentRepositoryCreatedEvent"
                    |     }
                    |  ],
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val contentRepositoryCreatedSink = sinks
            .find(_.name == "ccl-content-repository-created-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-content-repository-created-sink is not found"))

          val expectedContentRepositoryColumns: Set[String] = Set(
            "id",
            "name",
            "organisation",
            "createdAt",
            "occurredOn",
            "loadtime",
            "eventDateDw",
            "eventType",
            "createdById",
            "createdByName",
            "updatedAt",
            "updatedById",
            "updatedByName",
            "description",
            "forSchool",
            "contentTypes",
            "subjects",
            "grades",
            "contentYear",
            "_headers"
          )

          contentRepositoryCreatedSink.columns.toSet shouldBe expectedContentRepositoryColumns
        }
      )
    }
  }

  test("should consume contentRepositoryUpdated events") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |        "id": "a1d2faac-8a27-4b07-a99f-59f87c9edc99",
                    |        "name": "MoRA",
                    |        "organisation": "shared",
                    |        "updatedAt": "2022-10-05T11:19:54.2388148",
                    |        "occurredOn": 1664968794238,
                    |        "updatedById" : 1234,
                    |        "description": "dummy desc",
                    |        "forSchool": "dummy school",
                    |        "contentTypes": ["ACTIVITY"],
                    |        "subjects": ["MATH", "ENGLISH"],
                    |        "grades": ["1", "2"],
                    |        "contentYear": "2024",
                    |        "updatedByName": "admin"
                    | },
                    | "headers": [
                    |     {
                    |       "key": "eventType",
                    |       "value": "ContentRepositoryUpdatedEvent"
                    |     }
                    |  ],
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val contentRepositoryCreatedSink = sinks
            .find(_.name == "ccl-content-repository-updated-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-content-repository-updated-sink is not found"))

          val expectedContentRepositoryColumns: Set[String] = Set(
            "id",
            "name",
            "organisation",
            "updatedAt",
            "updatedById",
            "createdByName",
            "createdAt",
            "createdById",
            "updatedByName",
            "occurredOn",
            "loadtime",
            "eventDateDw",
            "eventType",
            "description",
            "forSchool",
            "contentTypes",
            "subjects",
            "grades",
            "contentYear",
            "_headers"
          )

          contentRepositoryCreatedSink.columns.toSet shouldBe expectedContentRepositoryColumns
        }
      )
    }
  }

  test("should consume contentRepositoryDeleted events") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |        "id": "a1d2faac-8a27-4b07-a99f-59f87c9edc99",
                    |        "name": "MoRA",
                    |        "organisation": "shared",
                    |        "updatedAt": "2022-10-05T11:19:54.2388148",
                    |        "occurredOn": 1664968794238,
                    |        "updatedById" : 1234,
                    |        "updatedByName": "admin"
                    | },
                    | "headers": [
                    |     {
                    |       "key": "eventType",
                    |       "value": "ContentRepositoryDeletedEvent"
                    |     }
                    |  ],
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val contentRepositoryCreatedSink = sinks
            .find(_.name == "ccl-content-repository-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-content-repository-deleted-sink is not found"))

          val expectedContentRepositoryColumns: Set[String] = Set(
            "id",
            "name",
            "organisation",
            "updatedAt",
            "updatedById",
            "updatedByName",
            "createdAt",
            "createdById",
            "createdByName",
            "occurredOn",
            "loadtime",
            "eventDateDw",
            "eventType",
            "_headers"
          )

          contentRepositoryCreatedSink.columns.toSet shouldBe expectedContentRepositoryColumns
        }
      )
    }
  }

  test("should consume contentRepositoryMaterialAttached events") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |        "id": "a1d2faac-8a27-4b07-a99f-59f87c9edc99",
                    |        "occurredOn": 1664968794238,
                    |        "addedMaterials": [
                    |        {"id": "a1d2faac-8a27-4b07-a99f-59f87c9edc00", "type": "COURSE"},
                    |        {"id": "a1d2faac-8a27-4b07-a99f-59f87c9edc01", "type": "InstructionalPlan"}
                    |        ]
                    | },
                    | "headers": [
                    |     {
                    |       "key": "eventType",
                    |       "value": "ContentRepositoryMaterialAttachedEvent"
                    |     }
                    |  ],
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val contentRepositoryMaterialAttachedSink = sinks
            .find(_.name == "ccl-content-repository-material-attached-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-content-repository-material-attached-sink is not found"))

          val expectedContentRepositoryMaterialsAttachedColumns: Set[String] = Set(
            "organisation", "_headers", "eventType", "updatedAt", "eventDateDw", "addedMaterials", "loadtime", "occurredOn", "id", "updatedById"
          )

          contentRepositoryMaterialAttachedSink.columns.toSet shouldBe expectedContentRepositoryMaterialsAttachedColumns
        }
      )
    }
  }

  test("should consume contentRepositoryMaterialDetached events") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |        "id": "a1d2faac-8a27-4b07-a99f-59f87c9edc99",
                    |        "occurredOn": 1664968794238,
                    |        "addedMaterials": [
                    |        {"id": "a1d2faac-8a27-4b07-a99f-59f87c9edc00", "type": "COURSE"},
                    |        {"id": "a1d2faac-8a27-4b07-a99f-59f87c9edc01", "type": "InstructionalPlan"}
                    |        ]
                    | },
                    | "headers": [
                    |     {
                    |       "key": "eventType",
                    |       "value": "ContentRepositoryMaterialDetachedEvent"
                    |     }
                    |  ],
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val contentRepositoryMaterialDetachedSink = sinks
            .find(_.name == "ccl-content-repository-material-detached-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-content-repository-material-detached-sink is not found"))

          val expectedContentRepositoryMaterialsDetachedColumns: Set[String] = Set(
            "organisation", "_headers", "eventType", "updatedAt", "eventDateDw", "removedMaterials", "loadtime", "occurredOn", "id", "updatedById"
          )

          contentRepositoryMaterialDetachedSink.columns.toSet shouldBe expectedContentRepositoryMaterialsDetachedColumns
        }
      )
    }
  }

  test("handle DraftPathwayCreatedEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value": {
              |	  "id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
              |	  "name": "Test Events - 1",
              |	  "code": "Test Events - 1",
              |	  "organisation": "shared",
              |	  "occurredOn": 1666192702464,
              |   "langCode": "EN_US",
              |   "configuration": {
              |     "programEnabled": true,
              |     "resourcesEnabled": false,
              |		  "placementType": "BY_ABILITY_TEST"
              |   },
              |   "curriculums": [
              |		{
              |			"curriculumId": 392027,
              |			"gradeId": 322135,
              |			"subjectId": 423412
              |		}
              |	]
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "DraftPathwayCreatedEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-pathway-draft-created-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-pathway-draft-created-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "eventDateDw", "loadtime", "id", "name", "code", "organisation", "configuration", "occurredOn", "langCode",
            "_headers", "curriculums"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("handle InReviewPathwayEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value": {
              |	  "id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
              |	  "organisation": "shared",
              |	  "name": "Test Events - 2",
              |	  "code": "Test Events - 1",
              |	  "subjectId": 754325,
              |   "subjectIds": [754325],
              |   "gradeIds": [322135],
              |	  "description": null,
              |	  "goal": null,
              |	  "occurredOn": 1666193245495,
              |   "langCode": "EN_US",
              |   "configuration": {
              |     "programEnabled": true,
              |     "resourcesEnabled": false,
              |		  "placementType": "BY_ABILITY_TEST"
              |   },
              |   "curriculums": [
              |		{
              |			"curriculumId": 392027,
              |			"gradeId": 322135,
              |			"subjectId": 423412
              |		}
              |	]
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "InReviewPathwayEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-pathway-in-review-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-pathway-in-review-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "eventDateDw", "loadtime", "id", "organisation", "configuration", "name", "code", "subjectId","subjectIds",
            "gradeIds", "description", "goal", "occurredOn", "langCode", "_headers", "curriculums"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("handle PathwayPublishedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value": {
              |	  "id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
              |	  "organisation": "shared",
              |	  "name": "Test Events - 2",
              |	  "createdOn": "2022-10-19T15:18:22.458",
              |	  "code": "Test Events - 1",
              |	  "subOrganisations": [
              |		  "25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
              |		  "53055d6d-ecf6-4596-88db-0c50cac72cd0",
              |		  "50445c7f-0f0d-45d4-aee1-26919a9b50f5"
              |	  ],
              |	  "subjectId": 754325,
              |   "subjectIds": [754325],
              |   "gradeIds": [322135],
              |	  "description": null,
              |	  "goal": null,
              |	  "modules": [
              |		  {
              |			  "id": "4478a0ef-d3c4-4fcc-a9eb-415c8ebc9cd1",
              |			  "maxAttempts": 1,
              |			  "activityId": {
              |			  	"uuid": "3d97d176-1cc1-4a54-b8ba-000000028407",
              |			  	"id": 28407
              |			  },
              |			  "settings": {
              |			  	"pacing": "LOCKED",
              |				  "hideWhenPublishing": false,
              |				  "isOptional": false
              |			  }
              |		  }
              |	  ],
              |	  "occurredOn": 1666193339145,
              |   "langCode": "EN_US",
              |   "configuration": {
              |     "programEnabled": true,
              |     "resourcesEnabled": false,
              |		  "placementType": "BY_ABILITY_TEST"
              |   },
              |   "curriculums": [
              |		{
              |			"curriculumId": 392027,
              |			"gradeId": 322135,
              |			"subjectId": 423412
              |		}
              |	]
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "PathwayPublishedEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-pathway-published-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-pathway-published-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "eventDateDw", "loadtime", "id", "organisation", "name", "createdOn", "configuration",
            "code", "subOrganisations", "subjectId", "subjectIds", "gradeIds", "description", "goal", "modules", "occurredOn", "langCode", "_headers", "curriculums"
          )
          sink.columns.toSet shouldBe expectedColumns

          sink.collect().length shouldBe 1
          val fst = sink.first()
          fst.getAs[String]("eventType") shouldBe "PathwayPublishedEvent"
          fst.getAs[List[String]]("subOrganisations") shouldBe List("25e9b735-6b6c-403b-a9f3-95e478e8f1ed", "53055d6d-ecf6-4596-88db-0c50cac72cd0", "50445c7f-0f0d-45d4-aee1-26919a9b50f5")

          sink.select(explode(col("modules"))).collect().length shouldBe 1
          val module = sink.select(explode(col("modules")).as("module")).select("module.*")
          module.first().getAs[String]("id") shouldBe "4478a0ef-d3c4-4fcc-a9eb-415c8ebc9cd1"

          val activityId = module.select("activityId.*")
          activityId.first().getAs[String]("uuid") shouldBe "3d97d176-1cc1-4a54-b8ba-000000028407"

          val settings = module.select("settings.*")
          settings.first().getAs[String]("pacing") shouldBe "LOCKED"

        }
      )
    }
  }

  test("handle PathwayDetailsUpdatedEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value": {
              |	  "id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
              |	  "status": "DRAFT",
              |	  "type": "PATHWAY",
              |	  "name": "Test Events - 2",
              |	  "code": "Test Events - 1",
              |	  "subjectId": 754325,
              |   "subjectIds": [754325],
              |   "gradeIds": [322135],
              |	  "occurredOn": 1666192861352,
              |   "langCode": "EN_US",
              |   "configuration": {
              |     "programEnabled": true,
              |     "resourcesEnabled": false,
              |		  "placementType": "BY_ABILITY_TEST"
              |   },
              |   "curriculums": [
              |		{
              |			"curriculumId": 392027,
              |			"gradeId": 322135,
              |			"subjectId": 423412
              |		}
              |	]
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "PathwayDetailsUpdatedEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-pathway-details-updated-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-pathway-details-updated-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "eventDateDw", "loadtime", "id", "status", "type", "name", "code", "subjectId",
            "subjectIds", "gradeIds", "configuration", "occurredOn", "langCode", "_headers", "courseStatus", "curriculums"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("handle PathwayDeletedEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value": {
              |	  "id": "779b3fa1-a037-4220-9efe-60cc0bb3e320",
              |	  "status": "DRAFT",
              |	  "occurredOn": 1666193574364
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "PathwayDeletedEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-pathway-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-pathway-deleted-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "eventDateDw", "loadtime", "id", "status", "occurredOn", "_headers", "courseStatus"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("handle LevelPublishedWithPathwayEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value": {
              |	  "id": "12374750-2b0e-4279-9788-def1007575e8",
              |	  "pathwayId": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
              |	  "index": 0,
              |	  "title": "Level-1",
              |   "isAccelerated": false,
              |	  "settings": {
              |		  "pacing": "SELF_PACED",
              |		  "hideWhenPublishing": false,
              |		  "isOptional": false
              |	  },
              |	  "items": [
              |		  {
              |			  "id": "03583155-b890-4ab6-a3c4-000000028179",
              |			  "settings": {
              |				  "pacing": "UN_LOCKED",
              |				  "hideWhenPublishing": false,
              |				  "isOptional": false
              |			  },
              |			  "type": "ACTIVITY",
              |       "mappedLearningOutcomes": [
              |         {
              |			      "id": 1849,
              |			      "type": "SKILL",
              |			      "curriculumId": 563622,
              |			      "gradeId": 333938,
              |			      "subjectId": 186926
              |		       }
              |	      ],
              |       "isJointParentActivity": false
              |		  },
              |		  {
              |			  "id": "a0b1bb0a-56c1-4bbe-b35f-000000028393",
              |			  "settings": {
              |				  "pacing": "UN_LOCKED",
              |				  "hideWhenPublishing": false,
              |				  "isOptional": false
              |			  },
              |			  "type": "ACTIVITY",
              |       "mappedLearningOutcomes": [],
              |       "isJointParentActivity": true
              |		  },
              |		  {
              |			  "id": "9a57d396-6566-4902-8038-000000028392",
              |			  "settings": {
              |				  "pacing": "UN_LOCKED",
              |				  "hideWhenPublishing": false,
              |				  "isOptional": false
              |			  },
              |			  "type": "ACTIVITY",
              |       "mappedLearningOutcomes": [],
              |       "isJointParentActivity": true
              |		  },
              |		  {
              |			  "id": "5c0bbf43-bc98-49d7-93ae-000000028400",
              |			  "settings": {
              |				  "pacing": "UN_LOCKED",
              |				  "hideWhenPublishing": false,
              |				  "isOptional": false
              |			  },
              |			  "type": "ACTIVITY"
              |		  },
              |		  {
              |			  "id": "043de3b9-b3e4-4e58-bc34-000000028395",
              |			  "settings": {
              |				  "pacing": "UN_LOCKED",
              |				  "hideWhenPublishing": false,
              |				  "isOptional": false
              |			  },
              |			  "type": "ACTIVITY"
              |		  },
              |		  {
              |			  "id": "069ccfdb-0c63-46c9-9c4e-f35f8512e66d",
              |			  "settings": {
              |				  "pacing": "UN_LOCKED",
              |				  "hideWhenPublishing": false,
              |				  "isOptional": false
              |			  },
              |			  "type": "INTERIM_CHECKPOINT"
              |		  }
              |	  ],
              |	  "longName": "Phantoms long name",
              |	  "description": "Level description",
              |	  "metadata": {
              |		  "tags": [
              |			  {
              |				  "key": "Grade",
              |				  "values": [
              |					  "KG",
              |					  "1"
              |				  ],
              |				  "type": "LIST"
              |			  },
              |			  {
              |				  "key": "Domain",
              |				  "values": [
              |					  {
              |						  "name": "Geometry",
              |						  "icon": "Geometry",
              |						  "color": "lightBlue"
              |					  },
              |					  {
              |						  "name": "Algebra and Algebraic Thinking",
              |						  "icon": "AlgebraAndAlgebraicThinking",
              |						  "color": "lightGreen"
              |					  }
              |				  ],
              |				  "type": "DOMAIN"
              |			  }
              |		  ]
              |	  },
              |	  "sequences": [
              |		  {
              |			  "domain": "Geometry",
              |			  "sequence": 1
              |		  },
              |		  {
              |		  	"domain": "Algebra and Algebraic Thinking",
              |	  		"sequence": 1
              |	  	}
              |	  ],
              |   "courseVersion": "228",
              |	  "occurredOn": 1666193339162
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "LevelPublishedWithPathwayEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-level-published-with-pathway-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-level-published-with-pathway-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "eventDateDw", "loadtime", "id", "pathwayId", "index", "title",
            "settings", "items", "longName", "description", "metadata", "sequences", "courseVersion", "occurredOn",
            "isAccelerated", "_headers"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume ActivityPlannedInCourseEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              | "activityId": {
              |		"uuid": "e68fa3fb-5072-452a-b03d-000000028364",
              |		"id": 28364
              |	 },
              |	"courseId": "8b8c2939-1f5e-4c60-a9f8-e8e426a67371",
              |	"courseType": "PATHWAY",
              |	"parentItemId": "61b741d1-b656-418c-835b-85a88fe12c2c",
              |	"index": 1,
              |	"courseVersion": "3.0",
              |	"settings": {
              |		"pacing": "UN_LOCKED",
              |		"hideWhenPublishing": false,
              |		"isOptional": false
              |	},
              |	"courseStatus": "PUBLISHED",
              |	"parentItemType": "LEVEL",
              |	"occurredOn": 1672669087383
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ActivityPlannedInCourseEvent"
              |     },
              |     {
              |       "key": "EVENTS_SOURCE",
              |       "value": "REAL_TIME"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |},
              |{
              | "key": "key2",
              | "value": {
              | "activityId": {
              |		"uuid": "e68fa3fb-5072-452a-b03d-000000028364",
              |		"id": 28364
              |	 },
              |	"courseId": "8b8c2939-1f5e-4c60-a9f8-e8e426a67371",
              |	"courseType": "PATHWAY",
              |	"parentItemId": "61b741d1-b656-418c-835b-85a88fe12c2c",
              |	"index": 1,
              |	"courseVersion": "3.0",
              |	"settings": {
              |		"pacing": "UN_LOCKED",
              |		"hideWhenPublishing": false,
              |		"isOptional": false
              |	},
              |"mappedLearningOutcomes": [
              |		{
              |			"id": 1849,
              |			"type": "SKILL",
              |			"curriculumId": 563622,
              |			"gradeId": 333938,
              |			"subjectId": 186926
              |		}
              |	],
              | "metadata": {
              |		  "tags": [
              |			  {
              |				  "key": "Grade",
              |				  "values": [
              |					  "KG",
              |					  "1"
              |				  ],
              |				  "type": "LIST"
              |			  },
              |			  {
              |				  "key": "Domain",
              |				  "values": [
              |					  {
              |						  "name": "Geometry",
              |						  "icon": "Geometry",
              |						  "color": "lightBlue"
              |					  },
              |					  {
              |						  "name": "Algebra and Algebraic Thinking",
              |						  "icon": "AlgebraAndAlgebraicThinking",
              |						  "color": "lightGreen"
              |					  }
              |				  ],
              |				  "type": "DOMAIN"
              |			  }
              |		  ]
              |	},
              |"isJointParentActivity": false,
              |	"courseStatus": "IN_REVIEW",
              |	"parentItemType": "LEVEL",
              |	"occurredOn": 1672669087383
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ActivityPlannedInCourseEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "IN_REVIEW"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val publishedSink = sinks
            .find(_.name == "ccl-published-activity-planned-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-activity-planned-in-course-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "courseType", "courseId", "parentItemType", "loadtime", "activityId", "occurredOn", "parentItemId", "courseVersion", "courseStatus", "settings", "index", "_headers", "mappedLearningOutcomes", "isJointParentActivity", "metadata")
          publishedSink.toDF().count() shouldBe 1
          publishedSink.columns.toSet shouldBe expectedColumns

          val inReviewSink = sinks
            .find(_.name == "ccl-in-review-activity-planned-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-in-review-activity-planned-in-course-sink is not found"))

          inReviewSink.toDF().count() shouldBe 1
        }
      )
    }
  }

  test("should consume ActivityUnplannedInCourseEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              | "activityId": {
              |		"uuid": "e68fa3fb-5072-452a-b03d-000000028361",
              |		"id": 28364
              |	 },
              |	"courseId": "8b8c2939-1f5e-4c60-a9f8-e8e426a67372",
              |	"courseType": "PATHWAY",
              |	"parentItemId": "61b741d1-b656-418c-835b-85a88fe12c26",
              |	"index": 2,
              |	"courseVersion": "4.0",
              |	"courseStatus": "PUBLISHED",
              |	"parentItemType": "LEVEL",
              |	"occurredOn": 1672669087382,
              | "mappedLearningOutcomes": [
              |		{
              |			"id": 1849,
              |			"type": "SKILL",
              |			"curriculumId": 563622,
              |			"gradeId": 333938,
              |			"subjectId": 186926
              |		}
              |	],
              | "metadata": {
              |		  "tags": [
              |			  {
              |				  "key": "Grade",
              |				  "values": [
              |					  "KG",
              |					  "1"
              |				  ],
              |				  "type": "LIST"
              |			  },
              |			  {
              |				  "key": "Domain",
              |				  "values": [
              |					  {
              |						  "name": "Geometry",
              |						  "icon": "Geometry",
              |						  "color": "lightBlue"
              |					  },
              |					  {
              |						  "name": "Algebra and Algebraic Thinking",
              |						  "icon": "AlgebraAndAlgebraicThinking",
              |						  "color": "lightGreen"
              |					  }
              |				  ],
              |				  "type": "DOMAIN"
              |			  }
              |		  ]
              |	},
              |"isJointParentActivity": false
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ActivityUnplannedInCourseEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-28 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val publishedSink = sinks
            .find(_.name == "ccl-published-activity-unplanned-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-activity-unplanned-in-course-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "courseType", "courseId",
            "parentItemType", "loadtime", "activityId", "occurredOn", "parentItemId", "courseVersion",
            "courseStatus", "index", "_headers", "isParentDeleted", "mappedLearningOutcomes", "isJointParentActivity", "metadata")
          publishedSink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume ActivityUpdatedInCourseEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              | "activityId": {
              |		"uuid": "e68fa3fb-5072-452a-b03d-000000028363",
              |		"id": 28364
              |	 },
              |	"courseId": "8b8c2939-1f5e-4c60-a9f8-e8e426a67374",
              |	"courseType": "PATHWAY",
              |	"parentItemId": "61b741d1-b656-418c-835b-85a88fe12c25",
              |	"index": 2,
              |	"courseVersion": "5.0",
              |	"courseStatus": "INREVIEW",
              |	"parentItemType": "LEVEL",
              |	"occurredOn": 1672669087381,
              | "mappedLearningOutcomes": [
              |		{
              |			"id": 1849,
              |			"type": "SKILL",
              |			"curriculumId": 563622,
              |			"gradeId": 333938,
              |			"subjectId": 186926
              |		}
              |	],
              | "metadata": {
              |		  "tags": [
              |			  {
              |				  "key": "Grade",
              |				  "values": [
              |					  "KG",
              |					  "1"
              |				  ],
              |				  "type": "LIST"
              |			  },
              |			  {
              |				  "key": "Domain",
              |				  "values": [
              |					  {
              |						  "name": "Geometry",
              |						  "icon": "Geometry",
              |						  "color": "lightBlue"
              |					  },
              |					  {
              |						  "name": "Algebra and Algebraic Thinking",
              |						  "icon": "AlgebraAndAlgebraicThinking",
              |						  "color": "lightGreen"
              |					  }
              |				  ],
              |				  "type": "DOMAIN"
              |			  }
              |		  ]
              |	},
              |"isJointParentActivity": false
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ActivityUpdatedInCourseEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-30 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-activity-updated-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-activity-updated-in-course-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "courseType", "courseId",
            "parentItemType", "loadtime", "activityId", "occurredOn", "parentItemId", "courseVersion",
            "courseStatus", "index", "settings", "_headers", "isParentDeleted", "mappedLearningOutcomes",
            "isJointParentActivity", "metadata")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume InterimCheckpointPublishedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"id": "4270574b-98fd-4bc7-a620-1dabb21e0bdc",
              |	"courseId": "8b8c2939-1f5e-4c60-a9f8-e8e426a67371",
              |	"title": "cp1-1",
              |	"language": "EN_US",
              |	"userTenant": "shared",
              |	"lessonsWithPools": [
              |		{
              |			"id": "07e0c841-c813-46c9-a8e2-000000028363",
              |			"poolIds": [
              |				"6304a3cfbfb63d7dec0f3093"
              |			]
              |		},
              |		{
              |			"id": "cc172ef8-ef96-4e27-954f-000000028362",
              |			"poolIds": [
              |				"6304a3cfbfb63d7dec0f3093"
              |			]
              |		}
              |	],
              |	"createdOn": "2023-01-02T14:17:11.195",
              |	"updatedOn": "2023-01-02T14:17:11.195",
              |	"settings": {
              |		"pacing": "UN_LOCKED",
              |		"hideWhenPublishing": false,
              |		"isOptional": false
              |	},
              |	"parentItemId": "b0fb3e7b-b9d0-48cf-9692-cc64eed85f30",
              |	"index": 2,
              |	"courseVersion": "3.0",
              |	"courseType": "PATHWAY",
              |	"parentItemType": "LEVEL",
              |	"courseStatus": "PUBLISHED",
              |	"occurredOn": 1672669087431
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "InterimCheckpointPublishedEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-interim-checkpoint-created-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-interim-checkpoint-created-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "createdOn", "updatedOn", "courseType", "courseId", "parentItemType", "loadtime", "occurredOn", "id", "userTenant", "language", "parentItemId", "courseVersion", "lessonsWithPools", "title", "courseStatus", "settings", "index", "_headers")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume InterimCheckpointUpdatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"id": "4270574b-98fd-4bc7-a620-1dabb21e0bdd",
              |	"courseId": "8b8c2939-1f5e-4c60-a9f8-e8e426a67372",
              |	"title": "cp1-2",
              |	"language": "EN_US",
              |	"userTenant": "private",
              |	"lessonsWithPools": [
              |		{
              |			"id": "07e0c841-c813-46c9-a8e2-000000028361",
              |			"poolIds": [
              |				"6304a3cfbfb63d7dec0f3094"
              |			]
              |		},
              |		{
              |			"id": "cc172ef8-ef96-4e27-954f-000000028378",
              |			"poolIds": [
              |				"6304a3cfbfb63d7dec0f3095"
              |			]
              |		}
              |	],
              |	"createdOn": "2023-01-02T14:17:11.194",
              |	"updatedOn": "2023-01-02T14:17:11.192",
              |	"settings": {
              |		"pacing": "UN_LOCKED"
              |	},
              |	"parentItemId": "b0fb3e7b-b9d0-48cf-9692-cc64eed85f31",
              |	"index": 3,
              |	"courseVersion": "0.0",
              |	"courseType": "PATHWAY",
              |	"parentItemType": "LEVEL",
              |	"courseStatus": "PUBLISHED",
              |	"occurredOn": 1672669087439,
              | "status": "draft"
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "InterimCheckpointUpdatedEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-interim-checkpoint-updated-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-interim-checkpoint-updated-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "createdOn", "updatedOn", "courseType", "courseId", "parentItemType", "loadtime", "occurredOn", "id", "userTenant", "language", "status", "parentItemId", "courseVersion", "lessonsWithPools", "title", "courseStatus", "settings", "index", "_headers", "isParentDeleted")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume InterimCheckpointDeletedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"id": "4270574b-98fd-4bc7-a620-1dabb21e0bd0",
              |	"userTenant": "private",
              |	"parentItemId": "b0fb3e7b-b9d0-48cf-9692-cc64eed85f38",
              |	"index": 3,
              | "courseId": "8fd413c0-7b40-4b92-8825-cfb9d21e7796",
              |	"courseVersion": "0.0",
              |	"parentItemType": "LEVEL",
              |	"courseStatus": "PUBLISHED",
              |	"occurredOn": 1672669087436
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "InterimCheckpointDeletedEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-interim-checkpoint-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-interim-checkpoint-deleted-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "parentItemType", "loadtime", "occurredOn", "id", "userTenant", "parentItemId", "courseVersion", "courseStatus", "index", "_headers", "isParentDeleted", "courseId")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume published LevelAddedInPathwayEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"id": "ec44b86b-b795-4437-a0a1-2845c180b303",
              |	"pathwayId": "acf33794-5dbe-44ff-b897-2b7b8328bc53",
              |	"index": 0,
              |	"title": "Level-2",
              |	"settings": {
              |		"pacing": "SELF_PACED",
              |		"hideWhenPublishing": false,
              |		"isOptional": false
              |	},
              |	"longName": "level three",
              |	"description": "",
              |	"metadata": {
              |		"tags": []
              |	},
              |	"sequences": [
              |		{
              |			"domain": null,
              |			"sequence": 1
              |		}
              |	],
              |	"courseVersion": "2.0",
              |	"courseStatus": "PUBLISHED",
              |	"occurredOn": 1672922363886,
              | "isAccelerated": false
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "LevelAddedInPathwayEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-level-added-in-pathway-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-level-added-in-pathway-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "description", "loadtime", "pathwayId",
            "occurredOn", "sequences", "id", "metadata", "courseVersion", "title", "courseStatus", "longName",
            "settings", "index", "isAccelerated","_headers")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume inReview LevelAddedInPathwayEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"id": "ec44b86b-b795-4437-a0a1-2845c180b303",
              |	"pathwayId": "acf33794-5dbe-44ff-b897-2b7b8328bc53",
              |	"index": 0,
              |	"title": "Level-2",
              |	"settings": {
              |		"pacing": "SELF_PACED",
              |		"hideWhenPublishing": false,
              |		"isOptional": false
              |	},
              |	"longName": "level three",
              |	"description": "",
              |	"metadata": {
              |		"tags": []
              |	},
              |	"sequences": [
              |		{
              |			"domain": null,
              |			"sequence": 1
              |		}
              |	],
              |	"courseVersion": "2.0",
              |	"courseStatus": "IN_REVIEW",
              |	"occurredOn": 1672922363886,
              | "isAccelerated": false
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "LevelAddedInPathwayEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-in-review-level-added-in-pathway-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-in-review-level-added-in-pathway-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "description", "loadtime", "pathwayId",
            "occurredOn", "sequences", "id", "metadata", "courseVersion", "title", "courseStatus", "longName",
            "settings", "index", "isAccelerated","_headers")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume published LevelUpdatedInPathwayEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"id": "ec44b86b-b795-4437-a0a1-2845c180b303",
              |	"pathwayId": "acf33794-5dbe-44ff-b897-2b7b8328bc53",
              |	"index": 0,
              |	"title": "Level-3",
              |	"settings": {
              |		"pacing": "SELF_PACED",
              |		"hideWhenPublishing": false,
              |		"isOptional": false
              |	},
              |	"longName": "level three",
              |	"description": "",
              |	"metadata": {
              |		"tags": []
              |	},
              |	"sequences": [
              |		{
              |			"domain": null,
              |			"sequence": 1
              |		}
              |	],
              |	"courseVersion": "2.0",
              |	"courseStatus": "PUBLISHED",
              |	"occurredOn": 1672922363886,
              | "isAccelerated": false
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "LevelUpdatedInPathwayEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-level-updated-in-pathway-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-level-updated-in-pathway-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "description", "loadtime", "pathwayId",
            "occurredOn", "sequences", "id", "metadata", "courseVersion", "title", "courseStatus", "longName",
            "settings", "index", "isAccelerated","_headers")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume inReview LevelUpdatedInPathwayEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"id": "ec44b86b-b795-4437-a0a1-2845c180b303",
              |	"pathwayId": "acf33794-5dbe-44ff-b897-2b7b8328bc53",
              |	"index": 0,
              |	"title": "Level-3",
              |	"settings": {
              |		"pacing": "SELF_PACED",
              |		"hideWhenPublishing": false,
              |		"isOptional": false
              |	},
              |	"longName": "level three",
              |	"description": "",
              |	"metadata": {
              |		"tags": []
              |	},
              |	"sequences": [
              |		{
              |			"domain": null,
              |			"sequence": 1
              |		}
              |	],
              |	"courseVersion": "2.0",
              |	"courseStatus": "IN_REVIEW",
              |	"occurredOn": 1672922363886,
              | "isAccelerated": false
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "LevelUpdatedInPathwayEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-in-review-level-updated-in-pathway-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-in-review-level-updated-in-pathway-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "description", "loadtime", "pathwayId",
            "occurredOn", "sequences", "id", "metadata", "courseVersion", "title", "courseStatus", "longName", "settings", "index","isAccelerated", "_headers")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume published LeveDeletedInPathwayEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"id": "95a1e620-b78b-4995-8e81-dd1afb451d4b",
              |	"pathwayId": "acf33794-5dbe-44ff-b897-2b7b8328bc53",
              |	"index": 0,
              |	"courseVersion": "2.0",
              |	"courseStatus": "PUBLISHED",
              |	"occurredOn": 1672922363885
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "LevelDeletedInPathwayEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-level-deleted-in-pathway-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-level-deleted-in-pathway-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "loadtime", "pathwayId", "occurredOn", "id", "courseVersion", "courseStatus", "index", "_headers")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume inReview LeveDeletedInPathwayEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"id": "95a1e620-b78b-4995-8e81-dd1afb451d4b",
              |	"pathwayId": "acf33794-5dbe-44ff-b897-2b7b8328bc53",
              |	"index": 0,
              |	"courseVersion": "2.0",
              |	"courseStatus": "IN_REVIEW",
              |	"occurredOn": 1672922363885
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "LevelDeletedInPathwayEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-in-review-level-deleted-in-pathway-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-in-review-level-deleted-in-pathway-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "loadtime", "pathwayId", "occurredOn", "id", "courseVersion", "courseStatus", "index", "_headers")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume published ADTPlannedInCourseEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"activityId": {
              |		"uuid": "bccfaa8b-89ca-40a6-9622-000000028439",
              |		"id": 28439
              |	},
              |	"courseId": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
              |	"courseType": "PATHWAY",
              |	"maxAttempts": 1,
              |	"settings": {
              |		"pacing": "LOCKED",
              |		"hideWhenPublishing": false,
              |		"isOptional": false
              |	},
              |	"parentItemId": "d4d2f7d9-a6f1-4b7a-bc22-4d410aed99bd",
              |	"index": 0,
              |	"courseVersion": "2.0",
              |	"courseStatus": "PUBLISHED",
              |	"parentItemType": "DIAGNOSTIC_TEST",
              |	"occurredOn": 1672923074081
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ADTPlannedInCourseEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-adt-planned-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-adt-planned-in-course-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "courseType", "courseId", "parentItemType", "loadtime", "activityId", "maxAttempts", "occurredOn", "parentItemId", "courseVersion", "courseStatus", "settings", "index", "_headers")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume inReview ADTPlannedInCourseEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"activityId": {
              |		"uuid": "bccfaa8b-89ca-40a6-9622-000000028439",
              |		"id": 28439
              |	},
              |	"courseId": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
              |	"courseType": "PATHWAY",
              |	"maxAttempts": 1,
              |	"settings": {
              |		"pacing": "LOCKED",
              |		"hideWhenPublishing": false,
              |		"isOptional": false
              |	},
              |	"parentItemId": "d4d2f7d9-a6f1-4b7a-bc22-4d410aed99bd",
              |	"index": 0,
              |	"courseVersion": "2.0",
              |	"courseStatus": "IN_REVIEW",
              |	"parentItemType": "DIAGNOSTIC_TEST",
              |	"occurredOn": 1672923074081
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ADTPlannedInCourseEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-in-review-adt-planned-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-in-review-adt-planned-in-course-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "courseType", "courseId", "parentItemType", "loadtime", "activityId", "maxAttempts", "occurredOn", "parentItemId", "courseVersion", "courseStatus", "settings", "index", "_headers")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume published ADTUnPlannedInCourseEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"activityId": {
              |		"uuid": "bccfaa8b-89ca-40a6-9622-000000028439",
              |		"id": 28439
              |	},
              |	"courseId": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
              |	"courseType": "PATHWAY",
              |	"parentItemId": "d4d2f7d9-a6f1-4b7a-bc22-4d410aed99bd",
              |	"index": -1,
              |	"courseVersion": "3.0",
              |	"courseStatus": "PUBLISHED",
              |	"parentItemType": "DIAGNOSTIC_TEST",
              |	"occurredOn": 1672923269302
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ADTUnPlannedInCourseEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-adt-unplanned-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-adt-unplanned-in-course-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "courseType", "courseId", "parentItemType", "loadtime", "activityId", "occurredOn", "parentItemId", "courseVersion", "courseStatus", "index", "_headers")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume inReview ADTUnPlannedInCourseEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"activityId": {
              |		"uuid": "bccfaa8b-89ca-40a6-9622-000000028439",
              |		"id": 28439
              |	},
              |	"courseId": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
              |	"courseType": "PATHWAY",
              |	"parentItemId": "d4d2f7d9-a6f1-4b7a-bc22-4d410aed99bd",
              |	"index": -1,
              |	"courseVersion": "3.0",
              |	"courseStatus": "IN_REVIEW",
              |	"parentItemType": "DIAGNOSTIC_TEST",
              |	"occurredOn": 1672923269302
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ADTUnPlannedInCourseEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-in-review-adt-unplanned-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-in-review-adt-unplanned-in-course-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "courseType", "courseId", "parentItemType", "loadtime", "activityId", "occurredOn", "parentItemId", "courseVersion", "courseStatus", "index", "_headers" )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume avatar created event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |    "body":{
                    |	        "id": "b95a7743-4e45-4ffc-986a-99de62396038",
                    |	        "type": "PREMIUM",
                    |	        "status": "ENABLED",
                    |	        "name": "meme.png",
                    |	        "fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
                    |	        "description": "some description",
                    |	        "validFrom": "2024-04-23T10:35:36.418102203",
                    |	        "validTill": "2024-04-29T20:00:00",
                    |	        "organizations": [
                    |	        	"MoE-AbuDhabi(Public)",
                    |	        	"MoE-North (Public)"
                    |	        ],
                    |	        "customization": null,
                    |	        "createdAt": "2024-04-23T10:35:36.41813526",
                    |	        "createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                    |	        "updatedAt": "2024-04-23T10:35:36.418140282",
                    |	        "updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                    |	        "starCost": 1,
                    |	        "genders": [
                    |	        	"MALE"
                    |	        ],
                    |	        "category": "CHARACTERS",
                    |	        "tenants": [
                    |	        	"e2e"
                    |	        ],
                    |	        "eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
                    |         "isEnabledForAllOrgs": true,
                    |	        "occurredOn": 1698296264207
                    |     }
                    | },
                    | "headers":[{
                    |       "key": "eventType",
                    |       "value": "AvatarCreatedEvent"
                    |    }],
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val avatarCreatedSink = sinks
            .find(_.name == "ccl-marketplace-avatar-created-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-marketplace-avatar-created-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "_headers",
            "eventType",
            "eventDateDw",
            "loadtime",
            "id",
            "type",
            "status",
            "name",
            "fileId",
            "description",
            "validFrom",
            "validTill",
            "organizations",
            "customization",
            "createdAt",
            "createdBy",
            "updatedAt",
            "updatedBy",
            "starCost",
            "genders",
            "category",
            "tenants",
            "eventId",
            "isEnabledForAllOrgs",
            "occurredOn"
          )

          avatarCreatedSink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume avatar-layer created event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |    "body":{
                    |         "id": "2abbb969-e361-4aca-ba8d-146054cc2f24",
                    |         "configType": "color",
                    |         "type": "STANDARD",
                    |         "key": "hairs",
                    |         "value": "#B25922",
                    |         "genders": [
                    |             "MALE"
                    |         ],
                    |         "cost": 0,
                    |         "tenants": [ "MOE" ],
                    |         "tags": [],
                    |         "excludedOrganizations": [],
                    |         "isEnabled": true,
                    |         "isDeleted": false,
                    |         "validFrom": null,
                    |         "validTill": null,
                    |         "order": 8,
                    |         "occurredOn": 1727855747787,
                    |         "eventId": "18a01073-ec5b-4376-bcba-a1a185564bd4"
                    |     }
                    | },
                    | "headers":[{
                    |       "key": "eventType",
                    |       "value": "AvatarLayerCreatedEvent"
                    |    }],
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val avatarLayerCreatedSink = sinks
            .find(_.name == "ccl-marketplace-avatar-layer-created-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-marketplace-avatar-layer-created-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "_headers",
            "eventType",
            "eventDateDw",
            "loadtime",
            "id",
            "configType",
            "type",
            "key",
            "value",
            "genders",
            "cost",
            "tenants",
            "tags",
            "excludedOrganizations",
            "isEnabled",
            "isDeleted",
            "validFrom",
            "validTill",
            "order",
            "eventId",
            "occurredOn"
          )

          avatarLayerCreatedSink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume avatar updated event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |    "body":{
                    |	        "id": "b95a7743-4e45-4ffc-986a-99de62396038",
                    |	        "type": "PREMIUM",
                    |	        "status": "ENABLED",
                    |	        "name": "meme.png",
                    |	        "fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
                    |	        "description": "some description",
                    |	        "validFrom": "2024-04-23T10:35:36.418102203",
                    |	        "validTill": "2024-04-29T20:00:00",
                    |	        "organizations": [
                    |	        	"MoE-AbuDhabi(Public)",
                    |	        	"MoE-North (Public)"
                    |	        ],
                    |	        "customization": null,
                    |	        "createdAt": "2024-04-23T10:35:36.41813526",
                    |	        "createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                    |	        "updatedAt": "2024-04-23T10:35:36.418140282",
                    |	        "updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                    |	        "starCost": 1,
                    |	        "genders": [
                    |	        	"MALE"
                    |	        ],
                    |	        "category": "CHARACTERS",
                    |	        "tenants": [
                    |	        	"e2e"
                    |	        ],
                    |	        "eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
                    |         "isEnabledForAllOrgs": true,
                    |	        "occurredOn": 1698296264207
                    |     }
                    | },
                    | "headers":[{
                    |       "key": "eventType",
                    |       "value": "AvatarUpdatedEvent"
                    |    }],
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val avatarUpdatedSink = sinks
            .find(_.name == "ccl-marketplace-avatar-updated-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-marketplace-avatar-updated-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "_headers",
            "eventType",
            "eventDateDw",
            "loadtime",
            "id",
            "type",
            "status",
            "name",
            "fileId",
            "description",
            "validFrom",
            "validTill",
            "organizations",
            "customization",
            "createdAt",
            "createdBy",
            "updatedAt",
            "updatedBy",
            "starCost",
            "genders",
            "category",
            "tenants",
            "eventId",
            "isEnabledForAllOrgs",
            "occurredOn"
          )

          avatarUpdatedSink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume avatar deleted event") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |    "body":{
                    |	        "id": "33beeb9a-4cbc-4913-959b-3f3692a76e5b",
                    |         "name": "name",
                    |         "fileId": "81dd2671-06f6-47b7-afb1-6929fb03d83b",
                    |         "deletedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                    |         "deletedAt": "2024-04-23T10:42:25.553661907",
                    |         "type": "PREMIUM",
                    |         "category": "OTHERS",
                    |         "description": "description",
                    |         "validFrom": "2024-04-23T10:35:36.418102203",
                    |	        "validTill": "2024-04-29T20:00:00",
                    |	        "organizations": [
                    |	        	"MoE-AbuDhabi(Public)",
                    |	        	"MoE-North (Public)"
                    |	        ],
                    |	        "customization": null,
                    |	        "starCost": 0,
                    |         "genders": [
                    |	        	"MALE"
                    |	        ],
                    |	        "category": "CHARACTERS",
                    |	        "tenants": [
                    |	        	"e2e"
                    |	        ],
                    |         "isEnabledForAllOrgs": true,
                    |         "eventId": "610dbdaa-8680-41f7-821a-1d42e65dc967",
                    |         "occurredOn": 1698296264207
                    |     }
                    | },
                    |  "headers":[{
                    |       "key": "eventType",
                    |       "value": "AvatarDeletedEvent"
                    |    }],
                    | "timestamp": "2022-09-27 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val avatarDeletedSink = sinks
            .find(_.name == "ccl-marketplace-avatar-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-marketplace-avatar-deleted-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "_headers",
            "eventType",
            "eventDateDw",
            "loadtime",
            "id",
            "fileId",
            "deletedBy",
            "deletedAt",
            "name",
            "type",
            "category",
            "description",
            "validFrom",
            "validTill",
            "organizations",
            "customization",
            "starCost",
            "genders",
            "tenants",
            "isEnabledForAllOrgs",
            "eventId",
            "occurredOn"
          )
          avatarDeletedSink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }
 }
