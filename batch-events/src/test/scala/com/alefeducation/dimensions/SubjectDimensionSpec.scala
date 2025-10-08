package com.alefeducation.dimensions

import com.alefeducation.bigdata.batch.{DeltaSink, Update, SubjectUpsert}
import com.alefeducation.bigdata.commons.testutils.{ExpectedFields, SparkSuite}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, TimestampType}
import org.scalatest.matchers.must.Matchers

class SubjectDimensionSpec extends SparkSuite with Matchers {

  import ExpectedFields._

  trait Setup {
    implicit val transformer = new SubjectDimensionTransformer(SubjectDimensionName, spark)
  }

  private val deltaExpectedFields = List(
    ExpectedField(name = "subject_name", dataType = StringType),
    ExpectedField(name = "grade_id", dataType = StringType),
    ExpectedField(name = "subject_online", dataType = BooleanType),
    ExpectedField(name = "subject_gen_subject", dataType = StringType),
    ExpectedField(name = "subject_id", dataType = StringType),
    ExpectedField(name = "eventdate", dataType = StringType),
    ExpectedField(name = "subject_status", dataType = IntegerType),
    ExpectedField(name = "subject_created_time", dataType = TimestampType),
    ExpectedField(name = "subject_dw_created_time", dataType = TimestampType),
    ExpectedField(name = "subject_updated_time", dataType = TimestampType),
    ExpectedField(name = "subject_deleted_time", dataType = TimestampType),
    ExpectedField(name = "subject_dw_updated_time", dataType = TimestampType)
  )

  private val deltaExpectedSelect = "delta.subject_id  = events.subject_id and  delta.subject_created_time <= events.subject_created_time "

  private val deltaExpectedUpdate = Map(
    "subject_name" -> "subject_name",
    "grade_id" -> "grade_id",
    "subject_status" -> "subject_status",
    "subject_online" -> "subject_online",
    "subject_gen_subject" -> "subject_gen_subject",
    "subject_updated_time" -> "subject_created_time",
    "subject_dw_updated_time" -> "subject_dw_created_time",
    "subject_id" -> "subject_id"
  )

  test("created event") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = SubjectParquetSource,
          value = """
              |{
              |  "eventType": "SubjectCreatedEvent",
              |  "uuid": "subject-uuid",
              |  "name": "subject-test",
              |  "online": true,
              |  "schoolId": "student-school-id",
              |  "schoolGradeUuid": "school-grade-uuid",
              |  "schoolGradeName": "school-grade-name",
              |   "genSubject": "Maths",
              |  "occurredOn": "1970-07-14 02:40:00.0",
              |  "tenantId": "tenant-id"
              |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "subject_created_time",
            "subject_updated_time",
            "subject_deleted_time",
            "subject_dw_created_time",
            "subject_dw_updated_time",
            "subject_id",
            "subject_name",
            "subject_online",
            "subject_gen_subject",
            "subject_status",
            "grade_id"
          )

          val redshiftSink = sinks.filter(_.name.equals("redshift-subject-dim")).head.input
          assert(redshiftSink.count === 1)
          assert(redshiftSink.columns.toSet === expectedColumns)
          assert[String](redshiftSink, "subject_id", "subject-uuid")
          assert[Int](redshiftSink, "subject_status", 1)
          assert[String](redshiftSink, "subject_created_time", "1970-07-14 02:40:00.0")
          assert[String](redshiftSink, "grade_id", "school-grade-uuid")

          val deltaSink = sinks.collectFirst { case s: DeltaSink if s.name == SubjectDimension.DeltaSinkName => s }.get
          deltaSink.config.operation mustBe a[SubjectUpsert]
          val deltaSinkDF = deltaSink.output.cache()
          assertExpectedFields(deltaSinkDF.schema.fields.toList, deltaExpectedFields)
          assert[Int](deltaSinkDF, s"${SubjectEntity}_status", ActiveEnabled)
          DeltaTests.assertDeltaSelectFields(deltaSink, deltaExpectedSelect)
          DeltaTests.assertDeltaUpdateFields(deltaSink, deltaExpectedUpdate)
        }
      )
    }
  }

  test("updated event") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = SubjectParquetSource,
          value = """
                    |{
                    |  "eventType": "SubjectUpdatedEvent",
                    |  "uuid": "subject-uuid",
                    |  "name": "subject-test",
                    |  "online": true,
                    |  "schoolId": "student-school-id",
                    |  "schoolGradeUuid": "school-grade-uuid",
                    |  "schoolGradeName": "school-grade-name",
                    |   "genSubject": "Maths",
                    |  "occurredOn": "1970-07-14 02:40:00.0",
                    |  "tenantId": "tenant-id"
                    |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val deltaSink = sinks.collectFirst { case s: DeltaSink if s.name == SubjectDimension.DeltaSinkName => s }.get
          deltaSink.config.operation mustBe a[SubjectUpsert]
          val deltaSinkDF = deltaSink.output.cache()
          assertExpectedFields(deltaSinkDF.schema.fields.toList, deltaExpectedFields)
          assert[Int](deltaSinkDF, s"${SubjectEntity}_status", ActiveEnabled)
          DeltaTests.assertDeltaSelectFields(deltaSink, deltaExpectedSelect)
          DeltaTests.assertDeltaUpdateFields(deltaSink, deltaExpectedUpdate)
        }
      )
    }
  }

  test("deleted event") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = SubjectParquetSource,
          value = """
                    |{
                    |  "eventType": "SubjectDeletedEvent",
                    |  "uuid": "subject-uuid",
                    |  "name": "subject-test",
                    |  "online": true,
                    |  "schoolId": "student-school-id",
                    |  "schoolGradeUuid": "school-grade-uuid",
                    |  "schoolGradeName": "school-grade-name",
                    |   "genSubject": "Maths",
                    |  "occurredOn": "1970-07-14 02:40:00.0",
                    |  "tenantId": "tenant-id"
                    |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val deltaSink = sinks.collectFirst { case s: DeltaSink if s.name == SubjectDimension.DeltaSinkName => s }.get
          deltaSink.config.operation mustBe a[Update]
          val deltaSinkDF = deltaSink.output.cache()
          assertExpectedFields(deltaSinkDF.schema.fields.toList, deltaExpectedFields)
          assert[Int](deltaSinkDF, s"${SubjectEntity}_status", Deleted)
          DeltaTests.assertDeltaSelectFields(deltaSink, deltaExpectedSelect)
          DeltaTests.assertDeltaDeleteFields(deltaSink, SubjectEntity)
        }
      )
    }
  }

}
