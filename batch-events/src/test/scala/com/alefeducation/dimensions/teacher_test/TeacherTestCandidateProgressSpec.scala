package com.alefeducation.dimensions.teacher_test

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.dimensions.generic_transform.GenericFactTransform
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class TeacherTestCandidateProgressSpec extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("transformation of candidate session completed event for fact teacher test progress") {
    val updatedValue =
      """
        |[{
        |     "eventType":"CandidateSessionRecorderMadeCompletedIntegrationEvent",
        |     "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |     "id": "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb",
        |     "candidateId": "b87d65a8-bc17-41f4-b1a4-cd38c1f10be6",
        |     "deliveryId": "3278a6e9-0b0b-437e-baae-024fa178b3b8",
        |     "assessmentId": "8d829b85-54f4-4d82-8301-8fa3f9ad10f2",
        |     "items": [
        |        {
        |           "questionId": "q1",
        |           "questionCode": "QC001",
        |           "submissions": [
        |              {
        |                 "attemptNumber": 1,
        |                 "questionVersion": 3,
        |                 "answer": {
        |                    "type": "MCQ",
        |                    "choiceIds": [1, 2],
        |                    "answerItems": [],
        |                    "selectedBlankChoices": []
        |                 },
        |                 "result": {
        |                    "correct": true,
        |                    "score": 2.5,
        |                    "validResponse": {
        |                       "type": "MCQ",
        |                       "choiceIds": [1],
        |                       "answerMapping": [],
        |                       "validBlankChoices": []
        |                    },
        |                    "answerResponse": {
        |                       "correctAnswers": {
        |                          "type": "MCQ",
        |                          "choiceIds": [1],
        |                          "answerItems": [],
        |                          "selectedBlankChoices": []
        |                       },
        |                       "wrongAnswers": [],
        |                       "unattendedCorrectAnswers": []
        |                    }
        |                 },
        |                 "timeSpent": 45.5,
        |                 "hintUsed": false,
        |                 "timestamp": "2024-02-12T12:45:23.123"
        |              }
        |           ],
        |           "createdAt": "2024-02-10T09:00:00.000",
        |           "updatedAt": "2024-02-10T09:30:00.000"
        |        }
        |     ],
        |     "score": 8.5,
        |     "awardedStars": 4,
        |     "createdAt": "2024-02-10T09:00:00.000",
        |     "updatedAt": "2024-02-10T09:30:00.000",
        |     "status": "COMPLETED",
        |     "aggregateIdentifier": "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb",
        |     "occurredOn": "2021-06-23 05:33:25.921"
        |}]
        |""".stripMargin

    val expectedColumns = Set(
      "fttcp_dw_id",
      "fttcp_session_id",
      "fttcp_candidate_id",
      "fttcp_test_delivery_id",
      "fttcp_assessment_id",
      "fttcp_score",
      "fttcp_stars_awarded",
      "fttcp_status",
      "fttcp_updated_at",
      "fttcp_created_at",
      "fttcp_created_time",
      "fttcp_dw_created_time",
      "fttcp_date_dw_id",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform(sprk, service, "teacher-test-candidate-transform")

    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    when(service.readOptional("parquet-teacher-test-candidate-made-in-progress-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-teacher-test-candidate-made-in-completed-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDf))
    when(service.readOptional("parquet-teacher-test-candidate-archived-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.getStartIdUpdateStatus("fact_teacher_test_candidate_progress")).thenReturn(1001)

    val sinks = transformer.transform()

    val df = sinks.head.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    assert[Int](df, "fttcp_dw_id", 1001)
    assert[String](df, "fttcp_session_id", "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb")
    assert[String](df, "fttcp_candidate_id", "b87d65a8-bc17-41f4-b1a4-cd38c1f10be6")
    assert[String](df, "fttcp_test_delivery_id", "3278a6e9-0b0b-437e-baae-024fa178b3b8")
    assert[String](df, "fttcp_assessment_id", "8d829b85-54f4-4d82-8301-8fa3f9ad10f2")
    assert[Double](df, "fttcp_score", 8.5)
    assert[Int](df, "fttcp_stars_awarded", 4)
    assert[String](df, "fttcp_status", "COMPLETED")
    assert[String](df, "fttcp_created_at", "2024-02-10 09:00:00.0")
    assert[String](df, "fttcp_updated_at", "2024-02-10 09:30:00.0")
    assert[String](df, "fttcp_created_time", "2021-06-23 05:33:25.921")
    assert[String](df, "fttcp_date_dw_id", "20210623")
    assert[String](df, "eventdate", "2021-06-23")
  }

  test("transformation of candidate session in-progress event for fact teacher test progress") {
    val updatedValue =
      """
        |[{
        |     "eventType":"CandidateSessionRecorderMadeInProgressIntegrationEvent",
        |     "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |     "id": "f3490bfc-7ab5-4623-b582-89a69dfe2a9c",
        |     "candidateId": "6bbdf826-f97d-44f4-8f79-682bb4798e25",
        |     "deliveryId": "0c3d6782-202b-482d-b8c7-ff9c5fe70b17",
        |     "assessmentId": "a79e9bc4-c5c3-4a4e-bd9f-c536d5d4c7d6",
        |     "createdAt": "2024-02-10T15:30:00.000",
        |     "status": "IN_PROGRESS",
        |     "aggregateIdentifier": "f3490bfc-7ab5-4623-b582-89a69dfe2a9c",
        |     "occurredOn": "2021-06-23 05:33:25.921"
        |}]
        |""".stripMargin

    val expectedColumns = Set(
      "fttcp_dw_id",
      "fttcp_session_id",
      "fttcp_candidate_id",
      "fttcp_test_delivery_id",
      "fttcp_assessment_id",
      "fttcp_score",
      "fttcp_stars_awarded",
      "fttcp_status",
      "fttcp_updated_at",
      "fttcp_created_at",
      "fttcp_date_dw_id",
      "fttcp_created_time",
      "fttcp_dw_created_time",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform(sprk, service, "teacher-test-candidate-transform")

    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    when(service.readOptional("parquet-teacher-test-candidate-made-in-progress-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDf))
    when(service.readOptional("parquet-teacher-test-candidate-made-in-completed-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-teacher-test-candidate-archived-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.getStartIdUpdateStatus("fact_teacher_test_candidate_progress")).thenReturn(1001)

    val sinks = transformer.transform()

    val df = sinks.head.output
    assert(df.schema("fttcp_score").dataType == FloatType)
    assert(df.schema("fttcp_stars_awarded").dataType == IntegerType)
    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    assert[Int](df, "fttcp_dw_id", 1001)
    assert[String](df, "fttcp_session_id", "f3490bfc-7ab5-4623-b582-89a69dfe2a9c")
    assert[String](df, "fttcp_candidate_id", "6bbdf826-f97d-44f4-8f79-682bb4798e25")
    assert[String](df, "fttcp_test_delivery_id", "0c3d6782-202b-482d-b8c7-ff9c5fe70b17")
    assert[String](df, "fttcp_assessment_id", "a79e9bc4-c5c3-4a4e-bd9f-c536d5d4c7d6")
    assert[String](df, "fttcp_score", null)
    assert[String](df, "fttcp_stars_awarded", null)
    assert[String](df, "fttcp_status", "IN_PROGRESS")
    assert[String](df, "fttcp_created_at", "2024-02-10 15:30:00.0")
    assert[String](df, "fttcp_updated_at", null)
    assert[String](df, "fttcp_created_time", "2021-06-23 05:33:25.921")
    assert[String](df, "fttcp_date_dw_id", "20210623")
    assert[String](df, "eventdate", "2021-06-23")
  }

  test("transformation of fact teacher test progress when both in-progress & candidate session completed events") {

    val inProgressEvent =
      """
        |[{
        |     "eventType":"CandidateSessionRecorderMadeInProgressIntegrationEvent",
        |     "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |     "id": "f3490bfc-7ab5-4623-b582-89a69dfe2a9c",
        |     "candidateId": "6bbdf826-f97d-44f4-8f79-682bb4798e25",
        |     "deliveryId": "0c3d6782-202b-482d-b8c7-ff9c5fe70b17",
        |     "assessmentId": "a79e9bc4-c5c3-4a4e-bd9f-c536d5d4c7d6",
        |     "createdAt": "2024-02-10T15:30:00.000",
        |     "status": "IN_PROGRESS",
        |     "aggregateIdentifier": "f3490bfc-7ab5-4623-b582-89a69dfe2a9c",
        |     "occurredOn": "2021-06-23 05:33:25.921"
        |}]
        |""".stripMargin
    val completedEvent =
      """
        |[{
        |     "eventType":"CandidateSessionRecorderMadeCompletedIntegrationEvent",
        |     "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |     "id": "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb",
        |     "candidateId": "b87d65a8-bc17-41f4-b1a4-cd38c1f10be6",
        |     "deliveryId": "3278a6e9-0b0b-437e-baae-024fa178b3b8",
        |     "assessmentId": "8d829b85-54f4-4d82-8301-8fa3f9ad10f2",
        |     "items": [
        |        {
        |           "questionId": "q1",
        |           "questionCode": "QC001",
        |           "submissions": [
        |              {
        |                 "attemptNumber": 1,
        |                 "questionVersion": 3,
        |                 "answer": {
        |                    "type": "MCQ",
        |                    "choiceIds": [1, 2],
        |                    "answerItems": [],
        |                    "selectedBlankChoices": []
        |                 },
        |                 "result": {
        |                    "correct": true,
        |                    "score": 2.5,
        |                    "validResponse": {
        |                       "type": "MCQ",
        |                       "choiceIds": [1],
        |                       "answerMapping": [],
        |                       "validBlankChoices": []
        |                    },
        |                    "answerResponse": {
        |                       "correctAnswers": {
        |                          "type": "MCQ",
        |                          "choiceIds": [1],
        |                          "answerItems": [],
        |                          "selectedBlankChoices": []
        |                       },
        |                       "wrongAnswers": [],
        |                       "unattendedCorrectAnswers": []
        |                    }
        |                 },
        |                 "timeSpent": 45.5,
        |                 "hintUsed": false,
        |                 "timestamp": "2024-02-12T12:45:23.123"
        |              }
        |           ],
        |           "createdAt": "2024-02-10T09:00:00.000",
        |           "updatedAt": "2024-02-10T09:30:00.000"
        |        }
        |     ],
        |     "score": 8.5,
        |     "awardedStars": 4,
        |     "createdAt": "2024-02-10T09:00:00.000",
        |     "updatedAt": "2024-02-10T09:30:00.000",
        |     "status": "COMPLETED",
        |     "aggregateIdentifier": "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb",
        |     "occurredOn": "2021-06-23 05:33:25.921"
        |}]
        |""".stripMargin


    val expectedColumns = Set(
      "fttcp_dw_id",
      "fttcp_session_id",
      "fttcp_candidate_id",
      "fttcp_test_delivery_id",
      "fttcp_assessment_id",
      "fttcp_score",
      "fttcp_stars_awarded",
      "fttcp_status",
      "fttcp_updated_at",
      "fttcp_created_at",
      "fttcp_date_dw_id",
      "fttcp_created_time",
      "fttcp_dw_created_time",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform(sprk, service, "teacher-test-candidate-transform")

    val inProgressEventDf = spark.read.json(Seq(inProgressEvent).toDS())
    when(service.readOptional("parquet-teacher-test-candidate-made-in-progress-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inProgressEventDf))
    val completedEventDf = spark.read.json(Seq(completedEvent).toDS())
    when(service.readOptional("parquet-teacher-test-candidate-made-in-completed-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(completedEventDf))
    when(service.readOptional("parquet-teacher-test-candidate-archived-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.getStartIdUpdateStatus("fact_teacher_test_candidate_progress")).thenReturn(1001)

    val sinks = transformer.transform()

    val df = sinks.head.output
    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)
    assert(df.schema("fttcp_score").dataType == DoubleType)
    assert(df.schema("fttcp_stars_awarded").dataType == LongType)

    assert[Int](df, "fttcp_dw_id", 1001)
    assert[String](df, "fttcp_session_id", "f3490bfc-7ab5-4623-b582-89a69dfe2a9c")
    assert[String](df, "fttcp_candidate_id", "6bbdf826-f97d-44f4-8f79-682bb4798e25")
    assert[String](df, "fttcp_test_delivery_id", "0c3d6782-202b-482d-b8c7-ff9c5fe70b17")
    assert[String](df, "fttcp_assessment_id", "a79e9bc4-c5c3-4a4e-bd9f-c536d5d4c7d6")
    assert[String](df, "fttcp_score", null)
    assert[String](df, "fttcp_stars_awarded", null)
    assert[String](df, "fttcp_status", "IN_PROGRESS")
    assert[String](df, "fttcp_created_at", "2024-02-10 15:30:00.0")
    assert[String](df, "fttcp_updated_at", null)
    assert[String](df, "fttcp_created_time", "2021-06-23 05:33:25.921")
    assert[String](df, "fttcp_date_dw_id", "20210623")
    assert[String](df, "eventdate", "2021-06-23")
  }

  test("transformation of candidate session archived event for fact teacher test archived") {
    val updatedValue =
      """
        |[{
        |     "eventType":"CandidateSessionRecorderArchivedIntegrationEvent",
        |     "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |     "id": "f3490bfc-7ab5-4623-b582-89a69dfe2a9c",
        |     "candidateId": "6bbdf826-f97d-44f4-8f79-682bb4798e25",
        |     "deliveryId": "0c3d6782-202b-482d-b8c7-ff9c5fe70b17",
        |     "assessmentId": "a79e9bc4-c5c3-4a4e-bd9f-c536d5d4c7d6",
        |     "createdAt": "2024-02-10T15:30:00.000",
        |     "status": "DISCARDED",
        |     "aggregateIdentifier": "f3490bfc-7ab5-4623-b582-89a69dfe2a9c",
        |     "occurredOn": "2021-06-23 05:33:25.921"
        |}]
        |""".stripMargin

    val expectedColumns = Set(
      "fttcp_dw_id",
      "fttcp_session_id",
      "fttcp_candidate_id",
      "fttcp_test_delivery_id",
      "fttcp_assessment_id",
      "fttcp_score",
      "fttcp_stars_awarded",
      "fttcp_status",
      "fttcp_updated_at",
      "fttcp_created_at",
      "fttcp_date_dw_id",
      "fttcp_created_time",
      "fttcp_dw_created_time",
      "fttcp_date_dw_id",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform(sprk, service, "teacher-test-candidate-transform")

    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    when(service.readOptional("parquet-teacher-test-candidate-made-in-progress-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-teacher-test-candidate-made-in-completed-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-teacher-test-candidate-archived-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDf))
    when(service.getStartIdUpdateStatus("fact_teacher_test_candidate_progress")).thenReturn(1001)

    val sinks = transformer.transform()

    val df = sinks.head.output
    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    assert[String](df, "fttcp_status", "DISCARDED")

  }
}
