package com.alefeducation.dimensions

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.TeacherGuardianFeedbackDimension.{
  DeltaTeacherFeedbackThreadSink,
  ParquetGuardianFeedbackReadSource,
  ParquetGuardianMessageSentSource,
  ParquetTeacherFeedbackDeletedSource,
  ParquetTeacherFeedbackSentSource,
  ParquetTeacherMessageDeletedSource,
  ParquetTeacherMessageSentSource,
  RedshiftTeacherFeedbackThreadSink,
  TeacherFeedbackEntityName
}
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType

class TeacherGuardianFeedbackDimensionSpec extends SparkSuite {

  trait Setup {
    implicit val transformer = TeacherGuardianFeedbackDimension(spark)
  }

  test("teacher feedback sent event") {
    val expRedshiftJson =
      """
        |[
        |{
        |"tft_thread_id":"f1",
        |"tft_feedback_type":3,
        |"tft_response_enabled": true,
        |"tft_guardian_id":"g1",
        |"tft_teacher_id":"t1",
        |"tft_student_id":"s1",
        |"tft_class_id":"c1",
        |"tft_status":1,
        |"tft_event_subject":1,
        |"tft_actor_type":1,
        |"tft_is_read":false,
        |"tft_created_time": "2021-08-05 09:09:49.0",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time" :null
        |},
        |{
        |"tft_thread_id":"f1",
        |"tft_feedback_type":3,
        |"tft_response_enabled": true,
        |"tft_guardian_id":"g2",
        |"tft_teacher_id":"t1",
        |"tft_student_id":"s1",
        |"tft_class_id":"c1",
        |"tft_status":1,
        |"tft_event_subject":1,
        |"tft_actor_type":1,
        |"tft_is_read":false,
        |"tft_created_time": "2021-08-05 09:09:49.0",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time" :null
        |},
        |{
        |"tft_thread_id":"f1",
        |"tft_feedback_type":3,
        |"tft_response_enabled": true,
        |"tft_guardian_id":"g3",
        |"tft_teacher_id":"t1",
        |"tft_student_id":"s1",
        |"tft_class_id":"c1",
        |"tft_status":1,
        |"tft_event_subject":1,
        |"tft_actor_type":1,
        |"tft_is_read":false,
        |"tft_created_time": "2021-08-05 09:09:49.0",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time" :null
        |}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)

    val expDeltaJson =
      """
        |[
        |{
        |"tft_thread_id":"f1",
        |"tft_feedback_type":3,
        |"tft_response_enabled": true,
        |"tft_guardian_id":"g1",
        |"tft_teacher_id":"t1",
        |"tft_student_id":"s1",
        |"tft_class_id":"c1",
        |"tft_status":1,
        |"tft_event_subject":1,
        |"tft_actor_type":1,
        |"tft_is_read":false,
        |"tft_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time" :null
        |},
        |{
        |"tft_thread_id":"f1",
        |"tft_feedback_type":3,
        |"tft_response_enabled": true,
        |"tft_guardian_id":"g2",
        |"tft_teacher_id":"t1",
        |"tft_student_id":"s1",
        |"tft_class_id":"c1",
        |"tft_status":1,
        |"tft_event_subject":1,
        |"tft_actor_type":1,
        |"tft_is_read":false,
        |"tft_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time" :null
        |},
        |{
        |"tft_thread_id":"f1",
        |"tft_feedback_type":3,
        |"tft_response_enabled": true,
        |"tft_guardian_id":"g3",
        |"tft_teacher_id":"t1",
        |"tft_student_id":"s1",
        |"tft_class_id":"c1",
        |"tft_status":1,
        |"tft_event_subject":1,
        |"tft_actor_type":1,
        |"tft_is_read":false,
        |"tft_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time" :null
        |}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherFeedbackSentSource,
          value = """
              |{
              |"feedbackThreadId":"f1",
              |"eventType": "TeacherFeedbackSentEvent",
              |"feedbackType":"OTHEr",
              |"responseEnabled": true,
              |"guardianIds":["g1", "g2", "g3"],
              |"teacherId":"t1",
              |"studentId":"s1",
              |"classId":"c1",
              |"occurredOn": "2021-08-05 09:09:49.0"
              |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshift = sinks.find(_.name === RedshiftTeacherFeedbackThreadSink).get.output
          assertSmallDatasetEquality(TeacherFeedbackEntityName, redshift, expRedshiftDf)

          val delta = sinks.find(_.name === DeltaTeacherFeedbackThreadSink).get.output
          assertSmallDatasetEquality(TeacherFeedbackEntityName, delta, expDeltaDf)
        }
      )
    }
  }

  test("teacher message sent event") {
    val expRedshiftJson =
      """
        |[
        |{
        |"tft_thread_id":"f1",
        |"tft_message_id":"m1",
        |"tft_is_first_of_thread":true,
        |"tft_status":1,
        |"tft_event_subject":2,
        |"tft_actor_type":1,
        |"tft_is_read":false,
        |"tft_created_time": "2021-08-05 09:09:49.0",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time" :null
        |}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)

    val expDeltaJson =
      """
        |[
        |{
        |"tft_thread_id":"f1",
        |"tft_message_id":"m1",
        |"tft_is_first_of_thread":true,
        |"tft_status":1,
        |"tft_event_subject":2,
        |"tft_actor_type":1,
        |"tft_is_read":false,
        |"tft_created_time": "2021-08-05 09:09:49.0",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time" :null
        |}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherMessageSentSource,
          value = """
                    |{
                    |"eventType": "TeacherMessageSentEvent",
                    |"messageId" : "m1",
                    |"feedbackThreadId" : "f1",
                    |"isFirstOfThread": true,
                    |"occurredOn": "2021-08-05 09:09:49.0"
                    |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshift = sinks.find(_.name === RedshiftTeacherFeedbackThreadSink).get.output
          assertSmallDatasetEquality(TeacherFeedbackEntityName, redshift, expRedshiftDf)

          val delta = sinks.find(_.name === DeltaTeacherFeedbackThreadSink).get.output
          assertSmallDatasetEquality(TeacherFeedbackEntityName, delta, expDeltaDf)
        }
      )
    }
  }

  test("guardian message sent event") {
    val expRedshiftJson =
      """
        |[
        |{
        |"tft_guardian_id":"g1",
        |"tft_thread_id":"f1",
        |"tft_message_id":"m1",
        |"tft_status":1,
        |"tft_event_subject":2,
        |"tft_actor_type":2,
        |"tft_is_read":false,
        |"tft_created_time": "2021-08-05 09:09:49.0",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time" :null
        |}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)

    val expDeltaJson =
      """
        |[
        |{
        |"tft_guardian_id":"g1",
        |"tft_thread_id":"f1",
        |"tft_message_id":"m1",
        |"tft_status":1,
        |"tft_event_subject":2,
        |"tft_actor_type":2,
        |"tft_is_read":false,
        |"tft_created_time": "2021-08-05 09:09:49.0",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time" :null
        |}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetGuardianMessageSentSource,
          value = """
                    |{
                    |"eventType": "GuardianMessageSentEvent",
                    |"guardianId": "g1",
                    |"messageId" : "m1",
                    |"feedbackThreadId" : "f1",
                    |"occurredOn": "2021-08-05 09:09:49.0"
                    |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshift = sinks.find(_.name === RedshiftTeacherFeedbackThreadSink).get.output
          assertSmallDatasetEquality(TeacherFeedbackEntityName, redshift, expRedshiftDf)

          val delta = sinks.find(_.name === DeltaTeacherFeedbackThreadSink).get.output
          assertSmallDatasetEquality(TeacherFeedbackEntityName, delta, expDeltaDf)
        }
      )
    }
  }

  test("teacher message deleted event") {
    val expRedshiftJson =
      """
        |[
        |{
        |"tft_message_id":"m1",
        |"tft_status":4,
        |"tft_created_time": "2021-08-05 09:09:49.0",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time": "2021-08-05 09:09:49.0"
        |}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)

    val expDeltaJson =
      """
        |[
        |{
        |"tft_message_id":"m1",
        |"tft_status":4,
        |"tft_created_time": "2021-08-05 09:09:49.0",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time": "2021-08-05 09:09:49.0"
        |}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherMessageDeletedSource,
          value = """
                    |{
                    |"eventType": "TeacherMessageDeletedEvent",
                    |"messageId" : "m1",
                    |"occurredOn": "2021-08-05 09:09:49.0"
                    |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshift = sinks.find(_.name === RedshiftTeacherFeedbackThreadSink).get.output
          assertSmallDatasetEquality(TeacherFeedbackEntityName, redshift, expRedshiftDf)

          val delta = sinks.find(_.name === DeltaTeacherFeedbackThreadSink).get.output
          assertSmallDatasetEquality(TeacherFeedbackEntityName, delta, expDeltaDf)
        }
      )
    }
  }

  test("teacher feedback deleted event") {
    val expRedshiftJson =
      """
        |[
        |{
        |"tft_thread_id":"f1",
        |"tft_status":4,
        |"tft_created_time": "2021-08-05 09:09:49.0",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time": "2021-08-05 09:09:49.0"
        |}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)

    val expDeltaJson =
      """
        |[
        |{
        |"tft_thread_id":"f1",
        |"tft_status":4,
        |"tft_created_time": "2021-08-05 09:09:49.0",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time": "2021-08-05 09:09:49.0"
        |}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherFeedbackDeletedSource,
          value = """
                    |{
                    |"eventType": "TeacherFeedbackDeletedEvent",
                    |"feedbackThreadId" : "f1",
                    |"occurredOn": "2021-08-05 09:09:49.0"
                    |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshift = sinks.find(_.name === RedshiftTeacherFeedbackThreadSink).get.output
          assertSmallDatasetEquality(TeacherFeedbackEntityName, redshift, expRedshiftDf)

          val delta = sinks.find(_.name === DeltaTeacherFeedbackThreadSink).get.output
          assertSmallDatasetEquality(TeacherFeedbackEntityName, delta, expDeltaDf)
        }
      )
    }
  }

  test("guardian feedback read event") {
    val expRedshiftJson =
      """
        |[
        |{
        |"tft_thread_id":"f1",
        |"tft_guardian_id":"g1",
        |"tft_is_read":true,
        |"tft_created_time": "2021-08-05 09:09:49.0",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time": null
        |}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)

    val expDeltaJson =
      """
        |[
        |{
        |"tft_thread_id":"f1",
        |"tft_guardian_id":"g1",
        |"tft_is_read":true,
        |"tft_created_time": "2021-08-05 09:09:49.0",
        |"tft_dw_created_time": "2021-08-05T09:09:49.000Z",
        |"tft_dw_updated_time": null,
        |"tft_updated_time": null,
        |"tft_deleted_time": null
        |}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetGuardianFeedbackReadSource,
          value = """
                    |{
                    |"eventType": "GuardianFeedbackReadEvent",
                    |"feedbackThreadId" : "f1",
                    |"guardianId": "g1",
                    |"occurredOn": "2021-08-05 09:09:49.0"
                    |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshift = sinks.find(_.name === RedshiftTeacherFeedbackThreadSink).get.output
          assertSmallDatasetEquality(TeacherFeedbackEntityName, redshift, expRedshiftDf)

          val delta = sinks.find(_.name === DeltaTeacherFeedbackThreadSink).get.output
          assertSmallDatasetEquality(TeacherFeedbackEntityName, delta, expDeltaDf)
        }
      )
    }
  }

  def createDfFromJsonWithTimeCols(spark: SparkSession, json: String): DataFrame = {
    createDfFromJson(spark, json)
      .withColumn(s"${TeacherFeedbackEntityName}_created_time", col(s"${TeacherFeedbackEntityName}_created_time").cast(TimestampType))
      .withColumn(s"${TeacherFeedbackEntityName}_updated_time", col(s"${TeacherFeedbackEntityName}_updated_time").cast(TimestampType))
      .withColumn(s"${TeacherFeedbackEntityName}_deleted_time", col(s"${TeacherFeedbackEntityName}_deleted_time").cast(TimestampType))
  }

}
