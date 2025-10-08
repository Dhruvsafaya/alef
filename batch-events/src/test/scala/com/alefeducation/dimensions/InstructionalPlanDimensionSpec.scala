package com.alefeducation.dimensions

import com.alefeducation.bigdata.batch.delta.{DeltaCreateSink, DeltaDeleteSink}
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.col

class InstructionalPlanDimensionSpec extends SparkSuite {

  trait Setup {
    implicit val transformer = InstructionalPlanDimension(spark)
  }

  val expectedColumns = Set(
    "instructional_plan_id",
    "instructional_plan_name",
    "instructional_plan_created_time",
    "instructional_plan_updated_time",
    "instructional_plan_deleted_time",
    "instructional_plan_dw_created_time",
    "instructional_plan_dw_updated_time",
    "instructional_plan_curriculum_id",
    "instructional_plan_curriculum_subject_id",
    "instructional_plan_curriculum_grade_id",
    "instructional_plan_content_academic_year_id",
    "instructional_plan_status",
    "lo_uuid",
    "instructional_plan_item_order",
    "week_uuid",
    "instructional_plan_item_ccl_lo_id",
    "instructional_plan_item_optional",
    "instructional_plan_item_default_locked",
    "instructional_plan_item_instructor_led",
    "instructional_plan_item_type",
    "ic_uuid",
    "instructional_plan_content_repository_id",
    "content_repository_uuid"
  )

  val deleteExpectedColumns = Set(
    "instructional_plan_id",
    "instructional_plan_created_time",
    "instructional_plan_updated_time",
    "instructional_plan_deleted_time",
    "instructional_plan_dw_created_time",
    "instructional_plan_dw_updated_time",
    "instructional_plan_status"
  )

  test("ip published") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetInstructionalPlanPublishedSource,
          value = """
              |{
              |  "eventType": "InstructionalPlanPublishedEvent",
              |  "id": "ip1",
              |  "name": "ip1Name",
              |  "curriculumId": 392027,
              |  "subjectId": 571671,
              |  "gradeId": 333938,
              |  "academicYearId": 2,
              |  "organisationId": "25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
              |  "status": "PUBLISHED",
              |  "contentRepositoryId": "25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
              |  "items": [
              |  {
              |    "itemType": "LESSON",
              |    "order": 1,
              |    "weekId": "f391a340-4e2a-11ea-b77f-2e728ce88125",
              |    "lessonId": 10002,
              |    "lessonUuid": "0456d946-5251-11ea-8d77-2e728ce88125",
              |    "optional":false,
              |    "instructorLed": true,
              |    "defaultLocked": true
              |  },
              |  {
              |    "itemType":"TEST",
              |    "checkpointUuid":"99ae8e22-b71a-470e-b28d-f26a4cae6cff",
              |    "order": 2,
              |    "weekId": "9236d946-5251-11ea-8d77-2e728ce88125",
              |    "optional":false,
              |    "instructorLed": false,
              |    "defaultLocked": false
              |  }
              |  ],
              |  "createdOn":  "2020-03-14 02:40:00.0",
              |  "occurredOn": "2020-03-14 02:40:00.0",
              |  "eventDateDw":20200314
              |}
    """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 3)
          val s3Sink = sinks.filter(_.name == ParquetInstructionalPlanPublishedSource).head
          assert[String](s3Sink.output, "eventdate", "2020-03-14")

          val redshiftSink = sinks.filter(_.name == RedshiftInstructionalPlanSink).head.output
          assert(redshiftSink.count === 2)
          assert(redshiftSink.columns.toSet === expectedColumns)

          val lessonTypeDf = redshiftSink.filter(col("instructional_plan_item_type") === "LESSON")
          assert(!lessonTypeDf.isEmpty)
          assert[Long](lessonTypeDf, "instructional_plan_item_ccl_lo_id", 10002)
          assert[String](lessonTypeDf, "lo_uuid", "0456d946-5251-11ea-8d77-2e728ce88125")
          assert(lessonTypeDf, "ic_uuid", null)

          val testTypeDf = redshiftSink.filter(col("instructional_plan_item_type") === "TEST")
          assert(!testTypeDf.isEmpty)
          assert(testTypeDf, "instructional_plan_item_ccl_lo_id", null)
          assert(testTypeDf, "lo_uuid", null)
          assert[String](testTypeDf, "ic_uuid", "99ae8e22-b71a-470e-b28d-f26a4cae6cff")

          val deltaSink = sinks.filter(_.name == InstructionalPlanDeltaSink).head.output
          assert(deltaSink.count === 2)
          assert(deltaSink.columns.toSet === expectedColumns)

        }
      )
    }
  }

  test("ip published without content repository id") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetInstructionalPlanPublishedSource,
          value = """
                    |{
                    |  "eventType": "InstructionalPlanPublishedEvent",
                    |  "id": "ip1",
                    |  "name": "ip1Name",
                    |  "curriculumId": 392027,
                    |  "subjectId": 571671,
                    |  "gradeId": 333938,
                    |  "academicYearId": 2,
                    |  "organisationId": "25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
                    |  "status": "PUBLISHED",
                    |  "items": [
                    |  {
                    |    "itemType": "LESSON",
                    |    "order": 1,
                    |    "weekId": "f391a340-4e2a-11ea-b77f-2e728ce88125",
                    |    "lessonId": 10002,
                    |    "lessonUuid": "0456d946-5251-11ea-8d77-2e728ce88125",
                    |    "optional":false,
                    |    "instructorLed": true,
                    |    "defaultLocked": true
                    |  },
                    |  {
                    |    "itemType":"TEST",
                    |    "checkpointUuid":"99ae8e22-b71a-470e-b28d-f26a4cae6cff",
                    |    "order": 2,
                    |    "weekId": "9236d946-5251-11ea-8d77-2e728ce88125",
                    |    "optional":false,
                    |    "instructorLed": false,
                    |    "defaultLocked": false
                    |  }
                    |  ],
                    |  "createdOn":  "2020-03-14 02:40:00.0",
                    |  "occurredOn": "2020-03-14 02:40:00.0",
                    |  "eventDateDw":20200314
                    |}
    """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 3)
          val s3Sink = sinks.filter(_.name == ParquetInstructionalPlanPublishedSource).head
          assert[String](s3Sink.output, "eventdate", "2020-03-14")

          val redshiftSink = sinks.filter(_.name == RedshiftInstructionalPlanSink).head.output
          assert(redshiftSink.count === 2)
          assert(redshiftSink.columns.toSet === expectedColumns)

          val lessonTypeDf = redshiftSink.filter(col("instructional_plan_item_type") === "LESSON")
          assert(!lessonTypeDf.isEmpty)
          assert[Long](lessonTypeDf, "instructional_plan_item_ccl_lo_id", 10002)
          assert[String](lessonTypeDf, "lo_uuid", "0456d946-5251-11ea-8d77-2e728ce88125")
          assert(lessonTypeDf, "ic_uuid", null)

          val testTypeDf = redshiftSink.filter(col("instructional_plan_item_type") === "TEST")
          assert(!testTypeDf.isEmpty)
          assert(testTypeDf, "instructional_plan_item_ccl_lo_id", null)
          assert(testTypeDf, "lo_uuid", null)
          assert[String](testTypeDf, "ic_uuid", "99ae8e22-b71a-470e-b28d-f26a4cae6cff")

          val deltaSink = sinks.filter(_.name == InstructionalPlanDeltaSink).head.output
          assert(deltaSink.count === 2)
          assert(deltaSink.columns.toSet === expectedColumns)

        }
      )
    }
  }

  test("ip re-published") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetInstructionalPlanRePublishedSource,
          value = """
                    |{
                    |  "eventType": "InstructionalPlanRePublishedEvent",
                    |  "id": "ip2",
                    |  "name": "ip2Name",
                    |  "curriculumId": 392027,
                    |  "subjectId": 571671,
                    |  "gradeId": 333938,
                    |  "academicYearId": 2,
                    |  "organisationId": "25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
                    |  "contentRepositoryId": "25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
                    |  "status": "PUBLISHED",
                    |  "items": [
                    |  {
                    |    "itemType": "LESSON",
                    |    "lessonUuid": "mlo1",
                    |    "order": 1,
                    |    "weekId": "f391a340-4e2a-11ea-b77f-2e728ce88125",
                    |    "lessonId": 10002,
                    |    "optional":false,
                    |    "instructorLed": true,
                    |    "defaultLocked": true
                    |  },
                    |  {
                    |    "itemType":"TEST",
                    |    "checkpointUuid":"99ae8e22-b71a-470e-b28d-f26a4cae6cff",
                    |    "order": 2,
                    |    "weekId": "9236d946-5251-11ea-8d77-2e728ce88125",
                    |    "optional":false,
                    |    "instructorLed": false,
                    |    "defaultLocked": false
                    |  }
                    |  ],
                    |  "occurredOn": "2020-03-14 02:40:00.0",
                    |  "eventDateDw": 20200314
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>


          assert(sinks.size == 5)
          val s3Sink = sinks.filter(_.name == ParquetInstructionalPlanRePublishedSource).head
          assert[String](s3Sink.output, "eventdate", "2020-03-14")

          val redshiftSinks: List[DataSink] = sinks.collect({ case sink: DataSink if sink.name == RedshiftInstructionalPlanSink => sink })

          val redshiftDeleleForRepublish = redshiftSinks.find(_.eventType.contains(InstructionalPlanDeletedEvent)).get.dataFrame
          assert(redshiftDeleleForRepublish.count === 1)
          assert(redshiftDeleleForRepublish.columns.toSet === deleteExpectedColumns)
          assert[String](redshiftDeleleForRepublish, "instructional_plan_id", "ip2")
          assert[Integer](redshiftDeleleForRepublish, "instructional_plan_status", 4)

          val redshiftRePublished = redshiftSinks.find(_.eventType.contains(InstructionalPlanRePublishedEvent)).get.dataFrame
          assert(redshiftRePublished.count === 2)
          assert(redshiftRePublished.columns.toSet === expectedColumns)
          assert[String](redshiftRePublished, "instructional_plan_id", "ip2")
          assert[String](redshiftRePublished, "instructional_plan_name", "ip2Name")
          assert[Integer](redshiftRePublished, "instructional_plan_curriculum_id", 392027)
          assert[Integer](redshiftRePublished, "instructional_plan_status", 1)

          val lessonTypeDf = redshiftRePublished.filter(col("instructional_plan_item_type") === "LESSON")
          assert(!lessonTypeDf.isEmpty)
          assert[Long](lessonTypeDf, "instructional_plan_item_ccl_lo_id", 10002)
          assert[String](lessonTypeDf, "lo_uuid", "mlo1")
          assert(lessonTypeDf, "ic_uuid", null)

          val testTypeDf = redshiftRePublished.filter(col("instructional_plan_item_type") === "TEST")
          assert(!testTypeDf.isEmpty)
          assert(testTypeDf, "instructional_plan_item_ccl_lo_id", null)
          assert(testTypeDf, "lo_uuid", null)
          assert[String](testTypeDf, "ic_uuid", "99ae8e22-b71a-470e-b28d-f26a4cae6cff")

          val deltaDeletedSink = sinks.collect({ case sink: DeltaDeleteSink => sink }).head.output
          assert(deltaDeletedSink.columns.toSet === deleteExpectedColumns)
          assert[String](deltaDeletedSink, "instructional_plan_id", "ip2")
          assert[Integer](deltaDeletedSink, "instructional_plan_status", 4)

          val deltaCreatedSink = sinks.collect({ case sink: DeltaCreateSink => sink }).head.output
          assert(deltaCreatedSink.count === 2)
          assert(deltaCreatedSink.columns.toSet === expectedColumns)
          assert[String](deltaCreatedSink, "instructional_plan_id", "ip2")
          assert[String](deltaCreatedSink, "instructional_plan_name", "ip2Name")
          assert[Integer](deltaCreatedSink, "instructional_plan_curriculum_id", 392027)
          assert[Integer](deltaCreatedSink, "instructional_plan_status", 1)

          val deltaLessonTypeDf = deltaCreatedSink.filter(col("instructional_plan_item_type") === "LESSON")
          assert(!deltaLessonTypeDf.isEmpty)
          assert[Long](deltaLessonTypeDf, "instructional_plan_item_ccl_lo_id", 10002)
          assert[String](deltaLessonTypeDf, "lo_uuid", "mlo1")
          assert(deltaLessonTypeDf, "ic_uuid", null)

          val deltaTestTypeDf = deltaCreatedSink.filter(col("instructional_plan_item_type") === "TEST")
          assert(!deltaTestTypeDf.isEmpty)
          assert(deltaTestTypeDf, "instructional_plan_item_ccl_lo_id", null)
          assert(deltaTestTypeDf, "lo_uuid", null)
          assert[String](deltaTestTypeDf, "ic_uuid", "99ae8e22-b71a-470e-b28d-f26a4cae6cff")
        }
      )
    }
  }

  test("ip re-published without content repository id") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetInstructionalPlanRePublishedSource,
          value = """
                    |{
                    |  "eventType": "InstructionalPlanRePublishedEvent",
                    |  "id": "ip2",
                    |  "name": "ip2Name",
                    |  "curriculumId": 392027,
                    |  "subjectId": 571671,
                    |  "gradeId": 333938,
                    |  "academicYearId": 2,
                    |  "organisationId": "25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
                    |  "status": "PUBLISHED",
                    |  "items": [
                    |  {
                    |    "itemType": "LESSON",
                    |    "lessonUuid": "mlo1",
                    |    "order": 1,
                    |    "weekId": "f391a340-4e2a-11ea-b77f-2e728ce88125",
                    |    "lessonId": 10002,
                    |    "optional":false,
                    |    "instructorLed": true,
                    |    "defaultLocked": true
                    |  },
                    |  {
                    |    "itemType":"TEST",
                    |    "checkpointUuid":"99ae8e22-b71a-470e-b28d-f26a4cae6cff",
                    |    "order": 2,
                    |    "weekId": "9236d946-5251-11ea-8d77-2e728ce88125",
                    |    "optional":false,
                    |    "instructorLed": false,
                    |    "defaultLocked": false
                    |  }
                    |  ],
                    |  "occurredOn": "2020-03-14 02:40:00.0",
                    |  "eventDateDw": 20200314
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "instructional_plan_id",
            "instructional_plan_name",
            "instructional_plan_created_time",
            "instructional_plan_updated_time",
            "instructional_plan_deleted_time",
            "instructional_plan_dw_created_time",
            "instructional_plan_dw_updated_time",
            "instructional_plan_curriculum_id",
            "instructional_plan_curriculum_subject_id",
            "instructional_plan_curriculum_grade_id",
            "instructional_plan_content_academic_year_id",
            "instructional_plan_status",
            "lo_uuid",
            "instructional_plan_item_order",
            "week_uuid",
            "instructional_plan_item_ccl_lo_id",
            "instructional_plan_item_optional",
            "instructional_plan_item_default_locked",
            "instructional_plan_item_instructor_led",
            "instructional_plan_item_type",
            "ic_uuid",
            "instructional_plan_content_repository_id",
            "content_repository_uuid"
          )

          assert(sinks.size == 5)
          val s3Sink = sinks.filter(_.name == ParquetInstructionalPlanRePublishedSource).head
          assert[String](s3Sink.output, "eventdate", "2020-03-14")

          val redshiftSinks: List[DataSink] = sinks.collect({ case sink: DataSink if sink.name == RedshiftInstructionalPlanSink => sink })

          val redshiftDeleleForRepublish = redshiftSinks.find(_.eventType.contains(InstructionalPlanDeletedEvent)).get.dataFrame
          assert(redshiftDeleleForRepublish.count === 1)
          assert(redshiftDeleleForRepublish.columns.toSet === deleteExpectedColumns)
          assert[String](redshiftDeleleForRepublish, "instructional_plan_id", "ip2")
          assert[Integer](redshiftDeleleForRepublish, "instructional_plan_status", 4)

          val redshiftRePublished = redshiftSinks.find(_.eventType.contains(InstructionalPlanRePublishedEvent)).get.dataFrame
          assert(redshiftRePublished.count === 2)
          assert(redshiftRePublished.columns.toSet === expectedColumns)
          assert[String](redshiftRePublished, "instructional_plan_id", "ip2")
          assert[String](redshiftRePublished, "instructional_plan_name", "ip2Name")
          assert[Integer](redshiftRePublished, "instructional_plan_curriculum_id", 392027)
          assert[Integer](redshiftRePublished, "instructional_plan_status", 1)

          val lessonTypeDf = redshiftRePublished.filter(col("instructional_plan_item_type") === "LESSON")
          assert(!lessonTypeDf.isEmpty)
          assert[Long](lessonTypeDf, "instructional_plan_item_ccl_lo_id", 10002)
          assert[String](lessonTypeDf, "lo_uuid", "mlo1")
          assert(lessonTypeDf, "ic_uuid", null)

          val testTypeDf = redshiftRePublished.filter(col("instructional_plan_item_type") === "TEST")
          assert(!testTypeDf.isEmpty)
          assert(testTypeDf, "instructional_plan_item_ccl_lo_id", null)
          assert(testTypeDf, "lo_uuid", null)
          assert[String](testTypeDf, "ic_uuid", "99ae8e22-b71a-470e-b28d-f26a4cae6cff")

          val deltaDeletedSink = sinks.collect({ case sink: DeltaDeleteSink => sink }).head.output
          assert(deltaDeletedSink.columns.toSet === deleteExpectedColumns)
          assert[String](deltaDeletedSink, "instructional_plan_id", "ip2")
          assert[Integer](deltaDeletedSink, "instructional_plan_status", 4)

          val deltaCreatedSink = sinks.collect({ case sink: DeltaCreateSink => sink }).head.output
          assert(deltaCreatedSink.count === 2)
          assert(deltaCreatedSink.columns.toSet === expectedColumns)
          assert[String](deltaCreatedSink, "instructional_plan_id", "ip2")
          assert[String](deltaCreatedSink, "instructional_plan_name", "ip2Name")
          assert[Integer](deltaCreatedSink, "instructional_plan_curriculum_id", 392027)
          assert[Integer](deltaCreatedSink, "instructional_plan_status", 1)

          val deltaLessonTypeDf = deltaCreatedSink.filter(col("instructional_plan_item_type") === "LESSON")
          assert(!deltaLessonTypeDf.isEmpty)
          assert[Long](deltaLessonTypeDf, "instructional_plan_item_ccl_lo_id", 10002)
          assert[String](deltaLessonTypeDf, "lo_uuid", "mlo1")
          assert(deltaLessonTypeDf, "ic_uuid", null)

          val deltaTestTypeDf = deltaCreatedSink.filter(col("instructional_plan_item_type") === "TEST")
          assert(!deltaTestTypeDf.isEmpty)
          assert(deltaTestTypeDf, "instructional_plan_item_ccl_lo_id", null)
          assert(deltaTestTypeDf, "lo_uuid", null)
          assert[String](deltaTestTypeDf, "ic_uuid", "99ae8e22-b71a-470e-b28d-f26a4cae6cff")
        }
      )
    }
  }

  test("ip deleted") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetInstructionalPlanDeletedSource,
          value = """
              |{
              |  "eventType": "InstructionalPlanDeletedEvent",
              |  "id": "ip2",
              |  "items": ["id1",
              |  "id2"
              |  ],
              |  "occurredOn": "2020-03-14 02:40:00.0",
              |  "eventDateDw": 20200314
              |}
    """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "instructional_plan_id",
            "instructional_plan_created_time",
            "instructional_plan_updated_time",
            "instructional_plan_deleted_time",
            "instructional_plan_dw_created_time",
            "instructional_plan_dw_updated_time",
            "instructional_plan_status"
          )
          assert(sinks.size == 3)
          val s3Sink = sinks.filter(_.name == ParquetInstructionalPlanDeletedSource).head
          assert[String](s3Sink.output, "eventdate", "2020-03-14")

          val redshiftSink = sinks.filter(_.name == RedshiftInstructionalPlanSink).head.output
          assert(redshiftSink.count === 1)
          assert(redshiftSink.columns.toSet === expectedColumns)
          assert[String](redshiftSink, "instructional_plan_id", "ip2")
          assert[Int](redshiftSink, "instructional_plan_status", 4)

          val deltaSink = sinks.filter(_.name == InstructionalPlanDeltaSink).head.output
          assert(deltaSink.count === 1)
          assert(deltaSink.columns.toSet === expectedColumns)
          assert[String](deltaSink, "instructional_plan_id", "ip2")
          assert[Int](deltaSink, "instructional_plan_status", 4)
        }
      )
    }
  }
}
