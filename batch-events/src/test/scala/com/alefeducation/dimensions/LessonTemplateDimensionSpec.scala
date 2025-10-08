package com.alefeducation.dimensions

import com.alefeducation.bigdata.batch.delta.{DeltaIwhSink, DeltaUpdateSink}
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.Encoders
import org.scalatest.matchers.must.Matchers

class LessonTemplateDimensionSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: LessonTemplateDimension = LessonTemplateDimension(spark)
  }

  val DeltaLessonTemplateMutatedExpectedFields = Set(
    "template_id",
    "template_title",
    "template_description",
    "template_framework_id",
    "template_status",
    "template_step_id",
    "template_step_display_name",
    "template_step_code",
    "template_step_abbreviation",
    "template_organisation"
  ) ++ dimDateCols("template").toSet -- Set("template_active_until")

  val DeltaLessonTemplateDeletedExpectedFields = Set(
    "template_id",
    "template_status"
  ) ++ dimDateCols("template").toSet -- Set("template_active_until")

  val RedshiftLessonTemplateMutatedExpectedFields = DeltaLessonTemplateMutatedExpectedFields -- Set(
    "template_description",
    "template_step_code",
    "template_step_abbreviation"
  )

  val RedshiftLessonTemplateDeletedExpectedFields = DeltaLessonTemplateDeletedExpectedFields

  test("LessonTemplateCreatedEvent And Updated") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "parquet-lesson-template-created",
          value = """
              |[
              |{
              |    "eventType": "LessonTemplateCreatedEvent",
              |    "id": 21,
              |    "frameworkId": 32,
              |    "title": "New Template",
              |    "description": "Demo Template Desc",
              |    "steps": [
              |      {
              |        "id": 12,
              |        "displayName": "name",
              |        "stepId": "ANTICIPATORY_CONTENT",
              |        "abbreviation": "abbr"
              |      },
              |      {
              |        "id": 14,
              |        "displayName": "name2",
              |        "stepId": "PREREQUISITE",
              |        "abbreviation": "abbr2"
              |      }
              |    ],
              |    "createdAt": 1586342333335,
              |    "occurredOn": "2020-07-08 05:18:02.053",
              |    "eventDateDw": "20200708",
              |    "loadtime": "2020-07-08T05:18:02.053Z",
              |    "organisation": "shared"
              |}
              |]
              |""".stripMargin
        ),
        SparkFixture(
          key = "parquet-lesson-template-updated",
          value = """
                    |[{
                    |    "eventType": "LessonTemplateUpdatedEvent",
                    |    "id": 21,
                    |    "frameworkId": 32,
                    |    "title": "New Template",
                    |    "description": "Demo Template Desc",
                    |    "steps": [
                    |      {
                    |        "id": 12,
                    |        "displayName": "name",
                    |        "stepId": "SUMMATIVE_ASSESSMENT",
                    |        "abbreviation": "abbr"
                    |      },
                    |      {
                    |        "id": 14,
                    |        "displayName": "name2",
                    |        "stepId": "REMEDIATION_2",
                    |        "abbreviation": "abbr2"
                    |      }
                    |    ],
                    |    "updatedAt": 1586342333335,
                    |    "eventDateDw": "20200708",
                    |    "occurredOn": "08-04-2020 10:38:55.109",
                    |    "loadtime": "2020-07-08T05:18:02.053Z",
                    |    "organisation": "shared"
                    |}]
                    |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val deltaCreatedSink = sinks.find { s =>
            s.name == "delta-lesson-template-sink" && s.eventType == "LessonTemplateCreatedEvent"
          }.get
          assert(deltaCreatedSink.output.columns.toSet === DeltaLessonTemplateMutatedExpectedFields)
          assert[Int](deltaCreatedSink.output, "template_status", 1)

          val deltaIWHSink = sinks.collectFirst { case s: DeltaUpdateSink if s.name == "delta-lesson-template-sink" => s }.get
          deltaIWHSink.updateFields.keySet mustNot contain("template_created_at")

          val redshiftCreatedSink =
            sinks.find { s =>
              s.name == "redshift-lesson-template-sink" && s.eventType == "LessonTemplateCreatedEvent"
            }.get
          assert(redshiftCreatedSink.output.columns.toSet === RedshiftLessonTemplateMutatedExpectedFields)
          assert[Int](redshiftCreatedSink.output, "template_status", 1)

          val parquetMutatedSinks = sinks.filter(_.name == "parquet-lesson-template-created")
          val parquetCreatedDf = parquetMutatedSinks.head.output.where("eventType = 'LessonTemplateCreatedEvent'")
          assert(!parquetCreatedDf.isEmpty)
        }
      )
    }
  }

  test("LessonTemplateUpdatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "parquet-lesson-template-updated",
          value = """
                    [{
                    |    "eventType": "LessonTemplateUpdatedEvent",
                    |    "id": 21,
                    |    "frameworkId": 32,
                    |    "title": "New Template",
                    |    "description": "Demo Template Desc",
                    |    "steps": [
                    |      {
                    |        "id": 12,
                    |        "displayName": "name",
                    |        "stepId": "SUMMATIVE_ASSESSMENT",
                    |        "abbreviation": "abbr"
                    |      },
                    |      {
                    |        "id": 14,
                    |        "displayName": "name2",
                    |        "stepId": "REMEDIATION_2",
                    |        "abbreviation": "abbr2"
                    |      }
                    |    ],
                    |    "updatedAt": 1586342333335,
                    |    "eventDateDw": "20200708",
                    |    "occurredOn": "08-04-2020 10:38:55.109",
                    |    "loadtime": "2020-07-08T05:18:02.053Z",
                    |    "organisation": "shared"
                    |}]
                    |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          sinks.find { s =>
            s.name == "delta-lesson-template-sink" && s.eventType == "LessonTemplateCreatedEvent"
          } mustBe empty

          val deltaIWHSink = sinks.collectFirst { case s: DeltaUpdateSink if s.name == "delta-lesson-template-sink" => s }.get
          deltaIWHSink.updateFields.keySet mustNot contain("template_created_at")
          deltaIWHSink.updateFields must contain("template_status" -> "2")
          assert(deltaIWHSink.output.columns.toSet === DeltaLessonTemplateMutatedExpectedFields - "template_deleted_time")
          assert[Int](deltaIWHSink.output, "template_status", 1)

          val redshiftIWHSink =
            sinks.find { s =>
              s.name == "redshift-lesson-template-sink" && s.eventType == "LessonTemplateUpdatedEvent"
            }.get
          assert(redshiftIWHSink.output.columns.toSet === RedshiftLessonTemplateMutatedExpectedFields - "template_deleted_time")
          assert[Int](redshiftIWHSink.output, "template_status", 1)

          val parquetMutatedSinks = sinks.filter(_.name == "parquet-lesson-template-updated")
          val parquetUpdatedDf = parquetMutatedSinks.head.output.where("eventType = 'LessonTemplateUpdatedEvent'")
          assert(!parquetUpdatedDf.isEmpty)

        }
      )
    }
  }

  test("LessonTemplate Updated create update and then update") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "parquet-lesson-template-created",
          value = """
                    |[
                    |{
                    |    "eventType": "LessonTemplateCreatedEvent",
                    |    "id": 21,
                    |    "frameworkId": 32,
                    |    "title": "New Template",
                    |    "description": "Demo Template Desc",
                    |    "steps": [
                    |      {
                    |        "id": 12,
                    |        "displayName": "name",
                    |        "stepId": "ANTICIPATORY_CONTENT",
                    |        "abbreviation": "abbr"
                    |      },
                    |      {
                    |        "id": 14,
                    |        "displayName": "name2",
                    |        "stepId": "PREREQUISITE",
                    |        "abbreviation": "abbr2"
                    |      }
                    |    ],
                    |    "createdAt": 1586342333335,
                    |    "occurredOn": "2020-07-08 05:18:02.053",
                    |    "eventDateDw": "20200708",
                    |    "loadtime": "2020-07-08T05:18:02.053Z",
                    |    "organisation": "created-org"
                    |},
                    |{
                    |    "eventType": "LessonTemplateCreatedEvent",
                    |    "id": 22,
                    |    "frameworkId": 32,
                    |    "title": "New Template",
                    |    "description": "Demo Template Desc",
                    |    "steps": [
                    |      {
                    |        "id": 12,
                    |        "displayName": "name",
                    |        "stepId": "ANTICIPATORY_CONTENT",
                    |        "abbreviation": "abbr"
                    |      }
                    |    ],
                    |    "createdAt": 1586342333335,
                    |    "occurredOn": "2020-07-09 05:18:02.053",
                    |    "eventDateDw": "20200709",
                    |    "loadtime": "2020-07-09T05:18:02.053Z",
                    |    "organisation": "created-org2"
                    |}
                    |]
                    |""".stripMargin
        ),
        SparkFixture(
          key = "parquet-lesson-template-updated",
          value = """
                    |[{
                    |    "eventType": "LessonTemplateUpdatedEvent",
                    |    "id": 21,
                    |    "frameworkId": 32,
                    |    "title": "New Template",
                    |    "description": "Demo Template Desc",
                    |    "steps": [
                    |      {
                    |        "id": 12,
                    |        "displayName": "name",
                    |        "stepId": "SUMMATIVE_ASSESSMENT",
                    |        "abbreviation": "abbr"
                    |      },
                    |      {
                    |        "id": 14,
                    |        "displayName": "name2",
                    |        "stepId": "REMEDIATION_2",
                    |        "abbreviation": "abbr2"
                    |      }
                    |    ],
                    |    "updatedAt": 1586342333335,
                    |    "eventDateDw": "20200708",
                    |    "occurredOn": "08-04-2020 10:38:55.109",
                    |    "loadtime": "2020-07-08T05:18:02.053Z",
                    |    "organisation": "org-updated"
                    |},
                    |{
                    |    "eventType": "LessonTemplateUpdatedEvent",
                    |    "id": 22,
                    |    "frameworkId": 32,
                    |    "title": "New Template New Again",
                    |    "description": "Demo Template Desc0",
                    |    "steps": [
                    |      {
                    |        "id": 12,
                    |        "displayName": "name",
                    |        "stepId": "ANTICIPATORY_CONTENT",
                    |        "abbreviation": "abbr"
                    |      },
                    |      {
                    |        "id": 14,
                    |        "displayName": "name2",
                    |        "stepId": "REMEDIATION_2",
                    |        "abbreviation": "abbr2"
                    |      }
                    |    ],
                    |    "createdAt": 1586342333335,
                    |    "occurredOn": "2020-07-09 05:18:02.053",
                    |    "eventDateDw": "20200709",
                    |    "loadtime": "2020-07-09T05:18:02.053Z",
                    |    "organisation": "org-updated2"
                    |},
                    |{
                    |    "eventType": "LessonTemplateUpdatedEvent",
                    |    "id": 22,
                    |    "frameworkId": 32,
                    |    "title": "New Template Agian",
                    |    "description": "Demo Template Desc1",
                    |    "steps": [
                    |      {
                    |        "id": 14,
                    |        "displayName": "name2",
                    |        "stepId": "REMEDIATION_2",
                    |        "abbreviation": "abbr2"
                    |      }
                    |    ],
                    |    "createdAt": 1586342333335,
                    |    "occurredOn": "2020-07-09 05:18:10.053",
                    |    "eventDateDw": "20200709",
                    |    "loadtime": "2020-07-09T05:18:10.053Z",
                    |    "organisation": "shared"
                    |}]
                    |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val deltaCreatedSink = sinks.find { s =>
            s.name == "delta-lesson-template-sink" && s.eventType == "LessonTemplateCreatedEvent"
          }.get
          assert(deltaCreatedSink.output.columns.toSet === DeltaLessonTemplateMutatedExpectedFields)
          assert[Int](deltaCreatedSink.output, "template_status", 1)

          val deltaIWHSink = sinks.collectFirst { case s: DeltaIwhSink if s.name == "delta-lesson-template-sink" => s }.get
          deltaIWHSink.updateFields.keySet mustNot contain("template_created_at")
          val deltaIWHSDf = deltaIWHSink.output.where("template_id = 22")
          val deltaStepIds = deltaIWHSDf.select("template_step_id").as(Encoders.STRING).collectAsList()
          assert(deltaStepIds.toArray.toSet == Set("12", "14"))

          val redshiftCreatedSink =
            sinks.find { s =>
              s.name == "redshift-lesson-template-sink" && s.eventType == "LessonTemplateCreatedEvent"
            }.get
          assert(redshiftCreatedSink.output.columns.toSet === RedshiftLessonTemplateMutatedExpectedFields)
          assert[Int](redshiftCreatedSink.output, "template_status", 1)

          val redshiftIWHSink = sinks.find { s =>
            s.name == "redshift-lesson-template-sink" && s.eventType == "LessonTemplateUpdatedEvent"
          }.get
          assert(redshiftIWHSink.output.columns.toSet === RedshiftLessonTemplateMutatedExpectedFields - "template_deleted_time")
          val redshiftIWHSDf = redshiftIWHSink.output.where("template_id = 22")
          assert(redshiftIWHSDf.columns.toSet === RedshiftLessonTemplateMutatedExpectedFields - "template_deleted_time")

          val redshiftStepIds = redshiftIWHSDf.select("template_step_id").as(Encoders.STRING).collectAsList()
          assert(redshiftStepIds.toArray.toSet == Set("12", "14"))

          assert[Int](redshiftIWHSDf, "template_status", 1)
          assert[Int](redshiftIWHSDf, "template_status", 1)

          val parquetMutatedSinks = sinks.filter(_.name == "parquet-lesson-template-created")
          val parquetCreatedDf = parquetMutatedSinks.head.output.where("eventType = 'LessonTemplateCreatedEvent'")
          assert(!parquetCreatedDf.isEmpty)
        }
      )
    }
  }

}
