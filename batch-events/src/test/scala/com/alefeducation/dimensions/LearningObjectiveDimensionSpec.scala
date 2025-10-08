package com.alefeducation.dimensions

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers._

class LearningObjectiveDimensionSpec extends SparkSuite {

  trait Setup {
    implicit val transformer = new LearningObjectiveDimensionTransformer(LearningObjectiveDimensionName, spark)
  }

  test("in LearningObjective dimension") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetLearningObjectiveCreatedSource,
          value = """
              |{
              |  "eventType": "learningObjective.create",
              |  "id": "lo-id",
              |  "type": "lo-type",
              |  "code": "lo-code",
              |  "title": "lo-title",
              |  "curriculumId": "Moe",
              |  "curriculumSubjectId": "Math",
              |  "curriculumGradeId": "7",
              |  "academicYear": "2020",
              |  "order": "1",
              |  "occurredOn": "1970-07-14 02:40:00.0",
              |  "tenantId": "tenant-id"
              |}
                """.stripMargin
        ),
        SparkFixture(
          key = ParquetLearningObjectiveMutatedSource,
          value = """
                    |{
                    |  "eventType": "LearningObjectiveUpdatedEvent",
                    |  "id": "lo-id",
                    |  "type": "lo-type",
                    |  "code": "lo-code",
                    |  "title": "lo-title",
                    |  "curriculumId": "Moe",
                    |  "curriculumSubjectId": "Math",
                    |  "curriculumGradeId": "7",
                    |  "academicYear": "2020",
                    |  "order": "1",
                    |  "occurredOn": "1970-07-14 02:40:00.0",
                    |  "tenantId": "tenant-id"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "lo_created_time",
            "lo_updated_time",
            "lo_deleted_time",
            "lo_dw_created_time",
            "lo_dw_updated_time",
            "lo_id",
            "lo_title",
            "lo_code",
            "lo_type",
            "lo_status",
            "lo_curriculum_id",
            "lo_curriculum_subject_id",
            "lo_curriculum_grade_id",
            "lo_content_academic_year",
            "lo_order"
          )

          assert(sinks.size == 4)

          val s3Sink = sinks.head
          val redshiftSink = sinks.last.output

          assert(redshiftSink.count === 1)
          assert(redshiftSink.columns.toSet === expectedColumns)
          assert[String](redshiftSink, "lo_id", "lo-id")
          assert[Int](redshiftSink, "lo_status", 1)

          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "1970-07-14")
        }
      )
    }
  }

  test("in LearningObjective updated") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetLearningObjectiveMutatedSource,
          value = """
                    |{
                    |  "eventType": "LearningObjectiveUpdatedEvent",
                    |  "id": "lo-id",
                    |  "type": "lo-type",
                    |  "code": "lo-code",
                    |  "title": "lo-title",
                    |  "curriculumId": "Moe",
                    |  "curriculumSubjectId": "Math",
                    |  "curriculumGradeId": "7",
                    |  "academicYear": "2020",
                    |  "order": "1",
                    |  "occurredOn": "1970-07-14 02:40:00.0",
                    |  "tenantId": "tenant-id"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "lo_created_time",
            "lo_updated_time",
            "lo_deleted_time",
            "lo_dw_created_time",
            "lo_dw_updated_time",
            "lo_id",
            "lo_title",
            "lo_code",
            "lo_type",
            "lo_status",
            "lo_curriculum_id",
            "lo_curriculum_subject_id",
            "lo_curriculum_grade_id",
            "lo_content_academic_year",
            "lo_order"
          )

          assert(sinks.size == 2)

          val s3Sink = sinks.head
          val redshiftSink = sinks.last.output

          assert(redshiftSink.count === 1)
          assert(redshiftSink.columns.toSet === expectedColumns)
          assert[String](redshiftSink, "lo_id", "lo-id")
          assert[Int](redshiftSink, "lo_status", 1)

          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "1970-07-14")
        }
      )
    }
  }

  test("in LearningObjective deleted") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetLearningObjectiveMutatedSource,
          value = """
                    |{
                    |  "eventType": "LearningObjectiveDeletedEvent",
                    |  "id": "lo-id",
                    |  "type": "lo-type",
                    |  "code": "lo-code",
                    |  "title": "lo-title",
                    |  "curriculumId": "Moe",
                    |  "curriculumSubjectId": "Math",
                    |  "curriculumGradeId": "7",
                    |  "academicYear": "2020",
                    |  "order": "1",
                    |  "occurredOn": "1970-07-14 02:40:00.0",
                    |  "tenantId": "tenant-id"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "lo_created_time",
            "lo_updated_time",
            "lo_deleted_time",
            "lo_dw_created_time",
            "lo_dw_updated_time",
            "lo_id",
            "lo_title",
            "lo_code",
            "lo_type",
            "lo_status",
            "lo_curriculum_id",
            "lo_curriculum_subject_id",
            "lo_curriculum_grade_id",
            "lo_content_academic_year",
            "lo_order"
          )

          assert(sinks.size == 2)

          val s3Sink = sinks.head
          val redshiftSink = sinks.last.output

          assert(redshiftSink.count === 1)
          assert(redshiftSink.columns.toSet === expectedColumns)
          assert[String](redshiftSink, "lo_id", "lo-id")
          assert[Int](redshiftSink, "lo_status", 4)

          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "1970-07-14")
        }
      )
    }
  }

}
