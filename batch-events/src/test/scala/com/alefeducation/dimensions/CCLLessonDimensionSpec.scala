package com.alefeducation.dimensions

import java.sql.Timestamp

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._

class CCLLessonDimensionSpec extends SparkSuite {

  trait Setup {
    implicit val transformer = new CCLLessonDimension(spark)
  }

  test("in Lesson create") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonMutatedSource,
          value = """
                    |{
                    |  "eventType": "LessonCreatedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "code":"lesson_Code1",
                    |  "status":"DRAFT",
                    |  "description":"lesson_desc",
                    |  "title":"lesson1",
                    |  "boardId":392027,
                    |  "gradeId":322135,
                    |  "subjectId":571671,
                    |  "skillable":true,
                    |  "mloFrameworkId":1,
                    |  "thumbnailFileName":"lesson_Code1_thumb.jpeg",
                    |  "thumbnailContentType":"image/jpeg",
                    |  "thumbnailFileSize":10156,
                    |  "thumbnailLocation":"77/0c/0e/4128",
                    |  "thumbnailUpdatedAt":1586848461741,
                    |  "userId":30,
                    |  "mloTemplateId":3,
                    |  "academicYearId":2,
                    |  "academicYear":"2020",
                    |  "assessmentTool":"ALEF",
                    |  "lessonType":"INSTRUCTIONAL_LESSON",
                    |  "createdAt":1586848461741,
                    |  "themeIds":[4,5,6],
                    |  "duration":25,
                    |  "lessonRoles": [
                    |      "REMEDIATION_LESSON"
                    |    ],
                    |  "occurredOn":"2020-07-20 02:40:00.0",
                    |  "organisation":"shared"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedRedshiftColumns = Set(
            "lo_dw_created_time",
            "lo_curriculum_id",
            "lo_duration",
            "lo_deleted_time",
            "lo_skillable",
            "lo_content_academic_year",
            "lo_status",
            "lo_curriculum_subject_id",
            "lo_code",
            "lo_assessment_tool",
            "lo_dw_updated_time",
            "lo_id",
            "lo_updated_time",
            "lo_user_id",
            "lo_ccl_id",
            "lo_content_academic_year_id",
            "lo_framework_id",
            "lo_action_status",
            "lo_type",
            "lo_template_id",
            "lo_title",
            "lo_curriculum_grade_id",
            "lo_created_time",
            "lo_organisation"
          )

          val expectedDeltaColumns = Set(
            "lo_dw_created_time",
            "lo_curriculum_id",
            "lo_duration",
            "lo_deleted_time",
            "lo_skillable",
            "lo_content_academic_year",
            "lo_status",
            "lo_curriculum_subject_id",
            "lo_code",
            "lo_assessment_tool",
            "lo_dw_updated_time",
            "lo_id",
            "lo_updated_time",
            "lo_user_id",
            "lo_ccl_id",
            "lo_content_academic_year_id",
            "lo_framework_id",
            "lo_action_status",
            "lo_type",
            "lo_theme_ids",
            "lo_template_id",
            "lo_description",
            "lo_title",
            "lo_curriculum_grade_id",
            "lo_created_time",
            "lo_max_stars",
            "lo_published_date",
            "lo_publisher_id",
            "lo_roles",
            "lo_organisation"
          )

          val parquetSink = sinks
            .find(_.name == ParquetCCLLessonMutatedSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonMutatedSource is not found"))

          assert[String](parquetSink, "code", "lesson_Code1")
          assert[String](parquetSink, "academicYear", "2020")
          assert[List[String]](parquetSink, "lessonRoles", List("REMEDIATION_LESSON"))

          val redshiftSink = sinks
            .find(_.name == RedshiftLearningObjectiveSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$RedshiftLearningObjectiveSink is not found"))

          assert(redshiftSink.columns.toSet === expectedRedshiftColumns)
          assert[String](redshiftSink, "lo_code", "lesson_Code1")
          assert[String](redshiftSink, "lo_content_academic_year", "2020")

          assert[String](redshiftSink, "lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](redshiftSink, "lo_ccl_id", 4128)
          assert[Int](redshiftSink, "lo_status", LessonActiveStatusVal)
          assert[Int](redshiftSink, "lo_action_status", LessonDraftActionStatusVal)
          assert[String](redshiftSink, "lo_title", "lesson1")
          assert[Long](redshiftSink, "lo_curriculum_id", 392027L)
          assert[Long](redshiftSink, "lo_curriculum_grade_id", 322135L)
          assert[Long](redshiftSink, "lo_curriculum_subject_id", 571671L)
          assert[Boolean](redshiftSink, "lo_skillable", true)
          assert[Long](redshiftSink, "lo_framework_id", 1)
          assert[Long](redshiftSink, "lo_user_id", 30L)
          assert[Long](redshiftSink, "lo_template_id", 3L)
          assert[Long](redshiftSink, "lo_content_academic_year_id", 2L)
          assert[String](redshiftSink, "lo_assessment_tool", "ALEF")
          assert[String](redshiftSink, "lo_type", "INSTRUCTIONAL_LESSON")
          assert[Int](redshiftSink, "lo_duration", 25)
          assert[String](redshiftSink, "lo_created_time", "2020-07-20 02:40:00.0")
          assert[String](redshiftSink, "lo_updated_time", null)
          assert[String](redshiftSink, "lo_deleted_time", null)
          assert[String](redshiftSink, "lo_organisation", "shared")

          val deltaSink = sinks
            .find(_.name == DeltaCCLLessonCreatedSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$DeltaCCLLessonCreatedSink is not found"))

          assert(deltaSink.columns.toSet === expectedDeltaColumns)
          assert[String](deltaSink, "lo_code", "lesson_Code1")
          assert[String](deltaSink, "lo_content_academic_year", "2020")

          assert[String](deltaSink, "lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](deltaSink, "lo_ccl_id", 4128)
          assert[Int](deltaSink, "lo_status", LessonActiveStatusVal)
          assert[Int](deltaSink, "lo_action_status", LessonDraftActionStatusVal)
          assert[String](deltaSink, "lo_title", "lesson1")
          assert[String](deltaSink, "lo_description", "lesson_desc")
          assert[Long](deltaSink, "lo_curriculum_id", 392027L)
          assert[Long](deltaSink, "lo_curriculum_grade_id", 322135L)
          assert[Long](deltaSink, "lo_curriculum_subject_id", 571671L)
          assert[Boolean](deltaSink, "lo_skillable", true)
          assert[Long](deltaSink, "lo_framework_id", 1)
          assert[Long](deltaSink, "lo_user_id", 30L)
          assert[Long](deltaSink, "lo_template_id", 3L)
          assert[Long](deltaSink, "lo_content_academic_year_id", 2L)
          assert[String](deltaSink, "lo_assessment_tool", "ALEF")
          assert[String](deltaSink, "lo_type", "INSTRUCTIONAL_LESSON")
          assert[List[Long]](deltaSink, "lo_theme_ids", List(4L, 5L, 6L))
          assert[Int](deltaSink, "lo_duration", 25)
          assert[String](deltaSink, "lo_max_stars", null)
          assert[String](deltaSink, "lo_published_date", null)
          assert[String](deltaSink, "lo_publisher_id", null)
          assert[String](deltaSink, "lo_created_time", "2020-07-20 02:40:00.0")
          assert[String](deltaSink, "lo_updated_time", null)
          assert[String](deltaSink, "lo_deleted_time", null)
          assert[List[String]](deltaSink, "lo_roles", List("REMEDIATION_LESSON"))
          assert[String](deltaSink, "lo_organisation", "shared")
        }
      )
    }
  }

  test("in Lesson update") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonMutatedSource,
          value = """
                    |{
                    |  "eventType":"LessonMetadataUpdatedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "code":"lesson_Code1",
                    |  "status": null,
                    |  "description":"lesson_desc_updated",
                    |  "title":"lesson1",
                    |  "boardId":392027,
                    |  "gradeId":322135,
                    |  "subjectId":571671,
                    |  "skillable":true,
                    |  "mloFrameworkId":1,
                    |  "thumbnailFileName":"lesson_Code1_thumb.jpeg",
                    |  "thumbnailContentType":"image/jpeg",
                    |  "thumbnailFileSize":10156,
                    |  "thumbnailLocation":"77/0c/0e/4128",
                    |  "thumbnailUpdatedAt":1586848461741,
                    |  "userId":30,
                    |  "mloTemplateId":3,
                    |  "academicYearId":2,
                    |  "academicYear":"2020",
                    |  "assessmentTool":"ALEF",
                    |  "lessonType":"INSTRUCTIONAL_LESSON",
                    |  "createdAt":null,
                    |  "updatedAt":1586848461741,
                    |  "themeIds":[4,5,6],
                    |  "duration":25,
                    |  "lessonRoles": [
                    |      "REMEDIATION_LESSON"
                    |    ],
                    |  "occurredOn":"2020-07-20 02:45:00.0",
                    |  "organisation":"shared"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "lo_dw_created_time",
            "lo_curriculum_id",
            "lo_duration",
            "lo_deleted_time",
            "lo_skillable",
            "lo_status",
            "lo_curriculum_subject_id",
            "lo_assessment_tool",
            "lo_dw_updated_time",
            "lo_updated_time",
            "lo_user_id",
            "lo_ccl_id",
            "lo_framework_id",
            "lo_type",
            "lo_template_id",
            "lo_description",
            "lo_title",
            "lo_curriculum_grade_id",
            "lo_created_time",
            "lo_organisation"
          )

          val parquetSink = sinks
            .find(_.name == ParquetCCLLessonMutatedSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonMutatedSource is not found"))

          assert[String](parquetSink, "code", "lesson_Code1")
          assert[String](parquetSink, "academicYear", "2020")

          val redshiftSink = sinks
            .find(_.name == RedshiftLearningObjectiveSink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$RedshiftLearningObjectiveSink is not found"))

          assert(redshiftSink.columns.toSet === expectedColumns.filterNot(_ == "lo_description"))

          assert[Long](redshiftSink, "lo_ccl_id", 4128)
          assert[Int](redshiftSink, "lo_status", LessonActiveStatusVal)
          assert[String](redshiftSink, "lo_title", "lesson1")
          assert[Long](redshiftSink, "lo_curriculum_id", 392027L)
          assert[Long](redshiftSink, "lo_curriculum_grade_id", 322135L)
          assert[Long](redshiftSink, "lo_curriculum_subject_id", 571671L)
          assert[Boolean](redshiftSink, "lo_skillable", true)
          assert[Long](redshiftSink, "lo_framework_id", 1)
          assert[Long](redshiftSink, "lo_user_id", 30L)
          assert[Long](redshiftSink, "lo_template_id", 3L)
          assert[String](redshiftSink, "lo_assessment_tool", "ALEF")
          assert[String](redshiftSink, "lo_type", "INSTRUCTIONAL_LESSON")
          assert[Int](redshiftSink, "lo_duration", 25)
          assert[String](redshiftSink, "lo_created_time", "2020-07-20 02:45:00.0")
          assert[String](redshiftSink, "lo_updated_time", null)
          assert[String](redshiftSink, "lo_deleted_time", null)
          assert[String](redshiftSink, "lo_organisation", "shared")

          val deltaSink = sinks
            .find(_.name == DeltaCCLLessonUpdatedSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$DeltaCCLLessonUpdatedSink is not found"))

          assert(deltaSink.columns.toSet === expectedColumns + "lo_roles" + "lo_theme_ids")

          assert[Long](deltaSink, "lo_ccl_id", 4128)
          assert[Int](deltaSink, "lo_status", LessonActiveStatusVal)
          assert[String](deltaSink, "lo_title", "lesson1")
          assert[String](deltaSink, "lo_description", "lesson_desc_updated")
          assert[Long](deltaSink, "lo_curriculum_id", 392027L)
          assert[Long](deltaSink, "lo_curriculum_grade_id", 322135L)
          assert[Long](deltaSink, "lo_curriculum_subject_id", 571671L)
          assert[Boolean](deltaSink, "lo_skillable", true)
          assert[Long](deltaSink, "lo_framework_id", 1)
          assert[Long](deltaSink, "lo_user_id", 30L)
          assert[Long](deltaSink, "lo_template_id", 3L)
          assert[String](deltaSink, "lo_assessment_tool", "ALEF")
          assert[String](deltaSink, "lo_type", "INSTRUCTIONAL_LESSON")
          assert[List[Long]](deltaSink, "lo_theme_ids", List(4L, 5L, 6L))
          assert[Int](deltaSink, "lo_duration", 25)
          assert[String](deltaSink, "lo_created_time", "2020-07-20 02:45:00.0")
          assert[String](deltaSink, "lo_updated_time", null)
          assert[String](deltaSink, "lo_deleted_time", null)
          assert[String](deltaSink, "lo_organisation", "shared")
        }
      )
    }
  }

  test("in Lesson delete") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonDeletedSource,
          value = """
                    |{
                    |  "eventType":"LessonDeletedEvent",
                    |  "lessonId":4160,
                    |  "lessonUuid":"ab59bca5-545e-47e1-b866-000000004160",
                    |  "code":"lesson_Code1",
                    |  "academicYear": "2020",
                    |  "occurredOn":"2020-07-20 02:45:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "lo_dw_created_time",
            "lo_deleted_time",
            "lo_status",
            "lo_dw_updated_time",
            "lo_updated_time",
            "lo_created_time",
            "lo_ccl_id"
          )

          val parquetSink = sinks
            .find(_.name == ParquetCCLLessonDeletedSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonDeletedSource is not found"))

          assert[String](parquetSink, "code", "lesson_Code1")
          assert[String](parquetSink, "academicYear", "2020")

          val redshiftSink = sinks
            .find(_.name == RedshiftLearningObjectiveSink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$RedshiftLearningObjectiveSink is not found"))

          assert(redshiftSink.columns.toSet === expectedColumns)
          assert[Int](redshiftSink, "lo_status", LessonDeletedStatusVal)
          assert[String](redshiftSink, "lo_created_time", "2020-07-20 02:45:00.0")
          assert[String](redshiftSink, "lo_deleted_time", "2020-07-20 02:45:00.0")
          assert[Int](redshiftSink, "lo_ccl_id", 4160)

          val deltaSink = sinks
            .find(_.name == DeltaCCLLessonDeletedSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$DeltaCCLLessonDeletedSink is not found"))

          assert(deltaSink.columns.toSet === expectedColumns)
          assert[Int](deltaSink, "lo_status", LessonDeletedStatusVal)
          assert[String](deltaSink, "lo_created_time", "2020-07-20 02:45:00.0")
          assert[Int](deltaSink, "lo_ccl_id", 4160)
        }
      )
    }
  }

  test("in Lesson status changed") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonWorkflowStatusChangedSource,
          value = """
                    |{
                    |  "eventType":"LessonWorkflowStatusChangedEvent",
                    |  "lessonId":4119,
                    |  "lessonUuid":"40fcbbf2-0406-3ad0-a095-000000004119",
                    |  "code":"lesson_Code1",
                    |  "academicYear": "2020",
                    |  "status":"REVIEW",
                    |  "publishedBefore":false,
                    |  "occurredOn":"2020-07-20 02:45:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "lo_dw_created_time",
            "lo_deleted_time",
            "lo_action_status",
            "lo_dw_updated_time",
            "lo_updated_time",
            "lo_created_time",
            "lo_ccl_id"
          )

          val parquetSink = sinks
            .find(_.name == ParquetCCLLessonWorkflowStatusChangedSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonWorkflowStatusChangedSource is not found"))

          assert[String](parquetSink, "code", "lesson_Code1")
          assert[String](parquetSink, "academicYear", "2020")

          val redshiftSink = sinks
            .find(_.name == RedshiftLearningObjectiveSink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$RedshiftLearningObjectiveSink is not found"))

          assert(redshiftSink.columns.toSet === expectedColumns)
          assert[Int](redshiftSink, "lo_action_status", LessonReviewActionStatusVal)
          assert[String](redshiftSink, "lo_created_time", "2020-07-20 02:45:00.0")
          assert[Int](redshiftSink, "lo_ccl_id", 4119)

          val deltaSink = sinks
            .find(_.name == DeltaCCLLessonWorkflowStatusChangedSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$DeltaCCLLessonWorkflowStatusChangedSink is not found"))

          assert(deltaSink.columns.toSet === expectedColumns)
          assert[Int](deltaSink, "lo_action_status", LessonReviewActionStatusVal)
          assert[String](deltaSink, "lo_created_time", "2020-07-20 02:45:00.0")
          assert[Int](deltaSink, "lo_ccl_id", 4119)
        }
      )
    }
  }

  test("in Lesson publish") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonPublishedSource,
          value = """
                    |{
                    |  "eventType":"LessonPublishedEvent",
                    |  "lessonId":2960,
                    |  "lessonUuid":"f9f6c8e6-67b7-32eb-b232-000000002960",
                    |  "publishedDate":"2020-04-24T07:19:40.529315",
                    |  "publisherId":30,
                    |  "publishedBefore":false,
                    |  "maxStars":5,
                    |  "lessonJSON":{
                    |     "code":"lesson_Code1",
                    |     "title":"Show My Learning - 4",
                    |     "description":"Show My Learning - 4",
                    |     "lessonType":"INSTRUCTIONAL_LESSON",
                    |     "thumbnail":"MA6_MLO_187_thumb.png",
                    |     "academic_year":"2020",
                    |     "education_board":{
                    |        "id":392027,
                    |        "name":"UAE MOE"
                    |     },
                    |     "subject":{
                    |        "id":571671,
                    |        "name":"Math"
                    |     },
                    |     "grade":{
                    |        "id":322135,
                    |        "name":"6"
                    |     },
                    |     "contents":[
                    |        {
                    |           "id":"KEY_TERMS",
                    |           "title":"Key Terms",
                    |           "abbreviation":"KT",
                    |           "type":"STATIC",
                    |           "url":"/mloId/2960?digest=a6a7ec66025d4674e756d6222265ef4e",
                    |           "description":"Show My Learning - 4"
                    |        },
                    |        {
                    |           "id":"SUMMATIVE_ASSESSMENT",
                    |           "title":"My Exit Ticket",
                    |           "abbreviation":"SA",
                    |           "type":"INTERACTIVE",
                    |           "url":"b9fb4094663f9c2fa3e4b0c4ddd470e9",
                    |           "description":"Show My Learning - 4"
                    |        }
                    |     ]
                    |  },
                    |  "occurredOn":"2020-07-20 02:45:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "lo_dw_created_time",
            "lo_deleted_time",
            "lo_dw_updated_time",
            "lo_updated_time",
            "lo_max_stars",
            "lo_published_date",
            "lo_publisher_id",
            "lo_action_status",
            "lo_created_time",
            "lo_ccl_id"
          )

          val parquetSink = sinks
            .find(_.name == ParquetCCLLessonPublishedSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonPublishedSource is not found"))

          val lessonJsonDf = parquetSink.select("lessonJSON.*")
          assert[String](lessonJsonDf, "code", "lesson_Code1")
          assert[String](lessonJsonDf, "academic_year", "2020")

          val redshiftSink = sinks
            .find(_.name == RedshiftLearningObjectiveSink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$RedshiftLearningObjectiveSink is not found"))

          assert(redshiftSink.columns.toSet === expectedColumns)
          assert[Int](redshiftSink, "lo_action_status", LessonPublishedActionStatusVal)
          assert[Int](redshiftSink, "lo_max_stars", 5)
          assert[Timestamp](redshiftSink, "lo_published_date", Timestamp.valueOf("2020-04-24 00:00:00"))
          assert[Long](redshiftSink, "lo_publisher_id", 30)
          assert[String](redshiftSink, "lo_created_time", "2020-07-20 02:45:00.0")
          assert[Int](redshiftSink, "lo_ccl_id", 2960)

          val deltaSink = sinks
            .find(_.name == DeltaCCLLessonPublishedSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$DeltaCCLLessonPublishedSink is not found"))

          assert(deltaSink.columns.toSet === expectedColumns)
          assert[Int](deltaSink, "lo_action_status", LessonPublishedActionStatusVal)
          assert[Int](deltaSink, "lo_max_stars", 5)
          assert[Timestamp](deltaSink, "lo_published_date", Timestamp.valueOf("2020-04-24 00:00:00"))
          assert[Long](deltaSink, "lo_publisher_id", 30)
          assert[String](deltaSink, "lo_created_time", "2020-07-20 02:45:00.0")
          assert[Int](deltaSink, "lo_ccl_id", 2960)
        }
      )
    }
  }

  test("in Lesson republish") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonPublishedSource,
          value = """
                    |{
                    |  "eventType":"LessonPublishedEvent",
                    |  "lessonId":2960,
                    |  "lessonUuid":"f9f6c8e6-67b7-32eb-b232-000000002960",
                    |  "publishedDate":"2020-04-24T07:19:40.529315",
                    |  "publisherId":30,
                    |  "publishedBefore":true,
                    |  "maxStars":5,
                    |  "lessonJSON":{
                    |     "code":"lesson_Code1",
                    |     "title":"Show My Learning - 4",
                    |     "description":"Show My Learning - 4",
                    |     "lessonType":"INSTRUCTIONAL_LESSON",
                    |     "thumbnail":"MA6_MLO_187_thumb.png",
                    |     "academic_year":"2020",
                    |     "education_board":{
                    |        "id":392027,
                    |        "name":"UAE MOE"
                    |     },
                    |     "subject":{
                    |        "id":571671,
                    |        "name":"Math"
                    |     },
                    |     "grade":{
                    |        "id":322135,
                    |        "name":"6"
                    |     },
                    |     "contents":[
                    |        {
                    |           "id":"KEY_TERMS",
                    |           "title":"Key Terms",
                    |           "abbreviation":"KT",
                    |           "type":"STATIC",
                    |           "url":"/mloId/2960?digest=a6a7ec66025d4674e756d6222265ef4e",
                    |           "description":"Show My Learning - 4"
                    |        },
                    |        {
                    |           "id":"SUMMATIVE_ASSESSMENT",
                    |           "title":"My Exit Ticket",
                    |           "abbreviation":"SA",
                    |           "type":"INTERACTIVE",
                    |           "url":"b9fb4094663f9c2fa3e4b0c4ddd470e9",
                    |           "description":"Show My Learning - 4"
                    |        }
                    |     ]
                    |  },
                    |  "occurredOn":"2020-07-20 02:50:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "lo_dw_created_time",
            "lo_deleted_time",
            "lo_dw_updated_time",
            "lo_updated_time",
            "lo_max_stars",
            "lo_published_date",
            "lo_publisher_id",
            "lo_action_status",
            "lo_created_time",
            "lo_ccl_id"
          )

          val parquetSink = sinks
            .find(_.name == ParquetCCLLessonPublishedSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonPublishedSource is not found"))

          val lessonJsonDf = parquetSink.select("lessonJSON.*")
          assert[String](lessonJsonDf, "code", "lesson_Code1")
          assert[String](lessonJsonDf, "academic_year", "2020")

          val redshiftSink = sinks
            .find(_.name == RedshiftLearningObjectiveSink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$RedshiftLearningObjectiveSink is not found"))

          assert(redshiftSink.columns.toSet === expectedColumns)
          assert[Int](redshiftSink, "lo_action_status", LessonRepublishedActionStatusVal)
          assert[Int](redshiftSink, "lo_max_stars", 5)
          assert[Timestamp](redshiftSink, "lo_published_date", Timestamp.valueOf("2020-04-24 00:00:00"))
          assert[Long](redshiftSink, "lo_publisher_id", 30)
          assert[String](redshiftSink, "lo_created_time", "2020-07-20 02:50:00.0")
          assert[Int](redshiftSink, "lo_ccl_id", 2960)

          val deltaSink = sinks
            .find(_.name == DeltaCCLLessonPublishedSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$DeltaCCLLessonPublishedSink is not found"))

          assert(deltaSink.columns.toSet === expectedColumns)
          assert[Int](deltaSink, "lo_action_status", LessonRepublishedActionStatusVal)
          assert[Int](deltaSink, "lo_max_stars", 5)
          assert[Timestamp](deltaSink, "lo_published_date", Timestamp.valueOf("2020-04-24 00:00:00"))
          assert[Long](deltaSink, "lo_publisher_id", 30)
          assert[String](deltaSink, "lo_created_time", "2020-07-20 02:50:00.0")
          assert[Int](deltaSink, "lo_ccl_id", 2960)
        }
      )
    }
  }

  test("in Lesson status change then publish") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonPublishedSource,
          value = """
                    |{
                    |  "eventType":"LessonPublishedEvent",
                    |  "lessonId":2960,
                    |  "lessonUuid":"f9f6c8e6-67b7-32eb-b232-000000002960",
                    |  "publishedDate":"2020-04-24T07:19:40.529315",
                    |  "publisherId":30,
                    |  "publishedBefore":false,
                    |  "maxStars":5,
                    |  "lessonJSON":{
                    |     "code":"lesson_Code1",
                    |     "title":"Show My Learning - 4",
                    |     "description":"Show My Learning - 4",
                    |     "lessonType":"INSTRUCTIONAL_LESSON",
                    |     "thumbnail":"MA6_MLO_187_thumb.png",
                    |     "academic_year":"2020",
                    |     "education_board":{
                    |        "id":392027,
                    |        "name":"UAE MOE"
                    |     },
                    |     "subject":{
                    |        "id":571671,
                    |        "name":"Math"
                    |     },
                    |     "grade":{
                    |        "id":322135,
                    |        "name":"6"
                    |     },
                    |     "contents":[
                    |        {
                    |           "id":"KEY_TERMS",
                    |           "title":"Key Terms",
                    |           "abbreviation":"KT",
                    |           "type":"STATIC",
                    |           "url":"/mloId/2960?digest=a6a7ec66025d4674e756d6222265ef4e",
                    |           "description":"Show My Learning - 4"
                    |        },
                    |        {
                    |           "id":"SUMMATIVE_ASSESSMENT",
                    |           "title":"My Exit Ticket",
                    |           "abbreviation":"SA",
                    |           "type":"INTERACTIVE",
                    |           "url":"b9fb4094663f9c2fa3e4b0c4ddd470e9",
                    |           "description":"Show My Learning - 4"
                    |        }
                    |     ]
                    |  },
                    |  "occurredOn":"2020-07-20 02:48:00.0"
                    |}
                  """.stripMargin
        ),
        SparkFixture(
          key = ParquetCCLLessonWorkflowStatusChangedSource,
          value = """
                  |{
                  |  "eventType":"LessonWorkflowStatusChangedEvent",
                  |  "lessonId":2960,
                  |  "lessonUuid":"f9f6c8e6-67b7-32eb-b232-000000002960",
                  |  "code":"lesson_Code1",
                  |  "academicYear": "2020",
                  |  "status":"REVIEW",
                  |  "publishedBefore":false,
                  |  "occurredOn":"2020-07-20 02:47:00.0"
                  |}
                """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshiftSink = sinks
            .filter(_.name == RedshiftLearningObjectiveSink)
            .last
            .output

          assert[Int](redshiftSink, "lo_action_status", LessonPublishedActionStatusVal)

          val deltaSink = sinks
            .find(_.name == DeltaCCLLessonWorkflowStatusChangedSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$DeltaCCLLessonPublishedSink is not found"))

          assert[Int](deltaSink, "lo_action_status", LessonPublishedActionStatusVal)
        }
      )
    }
  }
}
