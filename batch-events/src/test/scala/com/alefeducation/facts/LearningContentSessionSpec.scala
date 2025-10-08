package com.alefeducation.facts

import com.alefeducation.bigdata.batch.delta.DeltaCreateSink
import com.alefeducation.bigdata.commons.testutils.{ExpectedFields, SparkSuite}
import com.alefeducation.facts.LearningContentSession.{
  ParquetLearningContentFinishedSource,
  ParquetLearningContentSkippedSource,
  ParquetLearningContentStartedSource,
  RedshiftLearningContentSessionSink
}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, TimestampType}
import org.scalatest.matchers.must.Matchers

class LearningContentSessionSpec extends SparkSuite with Matchers {

  import ExpectedFields._

  trait Setup {
    implicit val transformer = LearningContentSession(spark)
  }

  val startedSkippedExpectedColumns = Set(
    "fcs_created_time",
    "fcs_dw_created_time",
    "fcs_date_dw_id",
    "fcs_id",
    "fcs_event_type",
    "fcs_is_start",
    "fcs_ls_id",
    "fcs_content_id",
    "fcs_lo_id",
    "fcs_student_id",
    "fcs_class_id",
    "fcs_grade_id",
    "fcs_tenant_id",
    "fcs_school_id",
    "fcs_ay_id",
    "fcs_section_id",
    "fcs_lp_id",
    "fcs_ip_id",
    "fcs_outside_of_school",
    "fcs_content_academic_year"
  )

  val finishedExpectedColumns: Set[String] = startedSkippedExpectedColumns ++ Set(
    "fcs_app_timespent",
    "fcs_app_score"
  )

  test("transform learning content started successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetLearningContentStartedSource,
          value = """
              |{
              |   "uuid":"3a04e4d7-8868-445b-93b2-09f6530f597e",
              |   "eventType": "ContentStartedEvent",
              |   "tenantId": "tenant-id",
              |   "learningSessionId":"3ffe993b-86fc-492e-8836-51b98005692a",
              |   "learningObjectiveId":"d0924a0e-4734-45b0-997a-22f4eb3f72b4",
              |   "studentId":"9bb01709-3856-4529-a22e-2ffedf0c0221",
              |   "studentK12Grade":6,
              |   "studentGradeId":"c362ac41-c90f-4ac3-8ead-74e199603aba",
              |   "studentSection":"7a91a97b-4643-4a26-95f6-3eaa481a0032",
              |   "learningPathId":"d5d10524-6b73-4fe7-8a9b-27b8209f2725",
              |   "instructionalPlanId":"e1ea5911-6e31-4839-be17-59f248810cda",
              |   "subjectCode":"SOCIAL_STUDIES",
              |   "contentId":"08cec393-3158-4776-b527-37f5900d46f2",
              |   "subjectName":"Gen Subject",
              |   "schoolId":"9e10cd3c-077b-44b7-ab89-127e893954bc",
              |   "academicYearId":"44b110a3-5839-4036-874e-f16995bcdc03",
              |   "contentAcademicYear":"2019",
              |   "classId":"af8266eb-f65c-4830-a0eb-fedc15ea0f6a",
              |   "outsideOfSchool":false,
              |   "occurredOn":"2021-02-21T12:31:32.055"
              |}
      """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val redshiftSink = sinks.find(_.name == RedshiftLearningContentSessionSink).get
          val redshiftSinkDF = redshiftSink.output
          assert(redshiftSinkDF.columns.toSet === startedSkippedExpectedColumns)
          assert[Int](redshiftSinkDF, "fcs_event_type", 1)
          assert[Boolean](redshiftSinkDF, "fcs_is_start", true)
          assert[String](redshiftSinkDF, "fcs_tenant_id", "tenant-id")
          assert[Int](redshiftSinkDF, "fcs_date_dw_id", 20210221)
          assert[String](redshiftSinkDF, "fcs_content_id", "08cec393-3158-4776-b527-37f5900d46f2")

          val deltaSink = sinks.collectFirst { case s: DeltaCreateSink => s }.get
          val deltaSinkDF = deltaSink.output
          deltaSinkDF.columns.toSet must contain allElementsOf startedSkippedExpectedColumns.map(_.replace("_uuid", "_id"))

          val expectedFields = List(
            ExpectedField(name = "fcs_event_type", dataType = IntegerType),
            ExpectedField(name = "fcs_content_id", dataType = StringType),
            ExpectedField(name = "fcs_date_dw_id", dataType = IntegerType),
            ExpectedField(name = "fcs_created_time", dataType = TimestampType),
            ExpectedField(name = "fcs_ip_id", dataType = StringType)
          )
          assertExpectedFields(
            deltaSinkDF
              .select(
                col("fcs_event_type"),
                col("fcs_created_time"),
                col("fcs_content_id"),
                col("fcs_date_dw_id"),
                col("fcs_ip_id")
              )
              .schema
              .fields
              .toList,
            expectedFields
          )
          assert[Int](deltaSinkDF, "fcs_event_type", 1)
          assert[Boolean](deltaSinkDF, "fcs_is_start", true)
          assert[String](deltaSinkDF, "fcs_tenant_id", "tenant-id")
          assert[Int](deltaSinkDF, "fcs_date_dw_id", 20210221)
          assert[String](deltaSinkDF, "fcs_content_id", "08cec393-3158-4776-b527-37f5900d46f2")
          assert[String](deltaSinkDF, "fcs_ip_id", "e1ea5911-6e31-4839-be17-59f248810cda")
        }
      )
    }
  }

  test("transform learning content skipped successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetLearningContentSkippedSource,
          value = """
              |{
              |   "uuid":"3a04e4d7-8868-445b-93b2-09f6530f597e",
              |   "eventType": "ContentSkippedEvent",
              |   "tenantId": "tenant-id",
              |   "learningSessionId":"3ffe993b-86fc-492e-8836-51b98005692a",
              |   "learningObjectiveId":"d0924a0e-4734-45b0-997a-22f4eb3f72b4",
              |   "studentId":"9bb01709-3856-4529-a22e-2ffedf0c0221",
              |   "studentK12Grade":6,
              |   "studentGradeId":"c362ac41-c90f-4ac3-8ead-74e199603aba",
              |   "studentSection":"7a91a97b-4643-4a26-95f6-3eaa481a0032",
              |   "learningPathId":"d5d10524-6b73-4fe7-8a9b-27b8209f2725",
              |   "instructionalPlanId":"e1ea5911-6e31-4839-be17-59f248810cda",
              |   "subjectCode":"SOCIAL_STUDIES",
              |   "contentId":"08cec393-3158-4776-b527-37f5900d46f2",
              |   "subjectName":"Gen Subject",
              |   "schoolId":"9e10cd3c-077b-44b7-ab89-127e893954bc",
              |   "academicYearId":"44b110a3-5839-4036-874e-f16995bcdc03",
              |   "contentAcademicYear":"2019",
              |   "classId":"af8266eb-f65c-4830-a0eb-fedc15ea0f6a",
              |   "outsideOfSchool":false,
              |   "occurredOn":"2021-02-21T12:31:32.055"
              |}
      """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val redshiftSink = sinks.find(_.name == RedshiftLearningContentSessionSink).get
          val redshiftSinkDF = redshiftSink.output
          assert(redshiftSinkDF.columns.toSet === startedSkippedExpectedColumns)
          assert[Int](redshiftSinkDF, "fcs_event_type", 2)
          assert[Boolean](redshiftSinkDF, "fcs_is_start", false)
          assert[String](redshiftSinkDF, "fcs_tenant_id", "tenant-id")
          assert[Int](redshiftSinkDF, "fcs_date_dw_id", 20210221)
          assert[String](redshiftSinkDF, "fcs_content_id", "08cec393-3158-4776-b527-37f5900d46f2")

          val deltaSink = sinks.collectFirst { case s: DeltaCreateSink => s }.get
          val deltaSinkDF = deltaSink.output
          deltaSinkDF.columns.toSet must contain allElementsOf startedSkippedExpectedColumns.map(_.replace("_uuid", "_id"))

          val expectedFields = List(
            ExpectedField(name = "fcs_event_type", dataType = IntegerType),
            ExpectedField(name = "fcs_content_id", dataType = StringType),
            ExpectedField(name = "fcs_date_dw_id", dataType = IntegerType),
            ExpectedField(name = "fcs_created_time", dataType = TimestampType),
            ExpectedField(name = "fcs_ip_id", dataType = StringType)
          )
          assertExpectedFields(
            deltaSinkDF
              .select(
                col("fcs_event_type"),
                col("fcs_created_time"),
                col("fcs_content_id"),
                col("fcs_date_dw_id"),
                col("fcs_ip_id")
              )
              .schema
              .fields
              .toList,
            expectedFields
          )
          assert[Int](deltaSinkDF, "fcs_event_type", 2)
          assert[Boolean](deltaSinkDF, "fcs_is_start", false)
          assert[String](deltaSinkDF, "fcs_tenant_id", "tenant-id")
          assert[Int](deltaSinkDF, "fcs_date_dw_id", 20210221)
          assert[String](deltaSinkDF, "fcs_content_id", "08cec393-3158-4776-b527-37f5900d46f2")
          assert[String](deltaSinkDF, "fcs_ip_id", "e1ea5911-6e31-4839-be17-59f248810cda")
        }
      )
    }
  }

  test("transform learning content finished successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetLearningContentFinishedSource,
          value = """
              |{
              |   "uuid":"3a04e4d7-8868-445b-93b2-09f6530f597e",
              |   "eventType": "ContentFinishedEvent",
              |   "tenantId": "tenant-id",
              |   "learningSessionId":"3ffe993b-86fc-492e-8836-51b98005692a",
              |   "learningObjectiveId":"d0924a0e-4734-45b0-997a-22f4eb3f72b4",
              |   "studentId":"9bb01709-3856-4529-a22e-2ffedf0c0221",
              |   "studentK12Grade":6,
              |   "studentGradeId":"c362ac41-c90f-4ac3-8ead-74e199603aba",
              |   "studentSection":"7a91a97b-4643-4a26-95f6-3eaa481a0032",
              |   "learningPathId":"d5d10524-6b73-4fe7-8a9b-27b8209f2725",
              |   "instructionalPlanId":"e1ea5911-6e31-4839-be17-59f248810cda",
              |   "subjectCode":"SOCIAL_STUDIES",
              |   "contentId":"08cec393-3158-4776-b527-37f5900d46f2",
              |   "subjectName":"Gen Subject",
              |   "schoolId":"9e10cd3c-077b-44b7-ab89-127e893954bc",
              |   "academicYearId":"44b110a3-5839-4036-874e-f16995bcdc03",
              |   "contentAcademicYear":"2019",
              |   "classId":"af8266eb-f65c-4830-a0eb-fedc15ea0f6a",
              |   "outsideOfSchool":false,
              |   "occurredOn":"2021-02-21T12:31:32.055",
              |   "vendorData" : {
              |      "score" : 100.00,
              |      "timeSpent": 120.00
              |   }
              |}
      """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val redshiftSink = sinks.find(_.name == RedshiftLearningContentSessionSink).get
          val redshiftSinkDF = redshiftSink.output
          assert(redshiftSinkDF.columns.toSet === finishedExpectedColumns)
          assert[Int](redshiftSinkDF, "fcs_event_type", 3)
          assert[Boolean](redshiftSinkDF, "fcs_is_start", false)
          assert[String](redshiftSinkDF, "fcs_tenant_id", "tenant-id")
          assert[Double](redshiftSinkDF, "fcs_app_score", 100.00)
          assert[Double](redshiftSinkDF, "fcs_app_timespent", 120.00)
          assert[Int](redshiftSinkDF, "fcs_date_dw_id", 20210221)
          assert[String](redshiftSinkDF, "fcs_content_id", "08cec393-3158-4776-b527-37f5900d46f2")

          val deltaSink = sinks.collectFirst { case s: DeltaCreateSink => s }.get
          val deltaSinkDF = deltaSink.output
          deltaSinkDF.columns.toSet must contain allElementsOf finishedExpectedColumns.map(_.replace("_uuid", "_id"))

          val expectedFields = List(
            ExpectedField(name = "fcs_event_type", dataType = IntegerType),
            ExpectedField(name = "fcs_content_id", dataType = StringType),
            ExpectedField(name = "fcs_date_dw_id", dataType = IntegerType),
            ExpectedField(name = "fcs_created_time", dataType = TimestampType),
            ExpectedField(name = "fcs_app_timespent", dataType = DoubleType),
            ExpectedField(name = "fcs_app_score", dataType = DoubleType),
            ExpectedField(name = "fcs_ip_id", dataType = StringType)
          )
          assertExpectedFields(
            deltaSinkDF
              .select(
                col("fcs_event_type"),
                col("fcs_created_time"),
                col("fcs_content_id"),
                col("fcs_date_dw_id"),
                col("fcs_ip_id"),
                col("fcs_app_timespent"),
                col("fcs_app_score")
              )
              .schema
              .fields
              .toList,
            expectedFields
          )
          assert[Int](deltaSinkDF, "fcs_event_type", 3)
          assert[Boolean](deltaSinkDF, "fcs_is_start", false)
          assert[String](deltaSinkDF, "fcs_tenant_id", "tenant-id")
          assert[Double](deltaSinkDF, "fcs_app_score", 100.00)
          assert[Double](deltaSinkDF, "fcs_app_timespent", 120.00)
          assert[Int](deltaSinkDF, "fcs_date_dw_id", 20210221)
          assert[String](deltaSinkDF, "fcs_content_id", "08cec393-3158-4776-b527-37f5900d46f2")
          assert[String](deltaSinkDF, "fcs_ip_id", "e1ea5911-6e31-4839-be17-59f248810cda")
        }
      )
    }
  }
}
