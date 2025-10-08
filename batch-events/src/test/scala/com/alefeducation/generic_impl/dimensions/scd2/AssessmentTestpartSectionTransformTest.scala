package com.alefeducation.generic_impl.dimensions.scd2

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.generic_impl.GenericSCD2Transformation
import org.apache.spark.sql.SparkSession
import org.scalatestplus.mockito.MockitoSugar.mock

class AssessmentTestpartSectionTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val updatedValue =
    """
      |[
      |{
      |  "_load_date": "20250430",
      |  "_load_time": 1746014156873,
      |  "_trace_id": "trace-id-1",
      |  "_source_type": "ALEF-KAFKA",
      |  "_app_tenant": "tenant-1",
      |  "_source_app": "SHARED",
      |  "_change_type": "insert",
      |  "_commit_version": 1,
      |  "_commit_timestamp": "2025-04-30T10:43:10.000Z",
      |  "eventType": "SectionPlannedInTestPartDataEvent",
      |  "timeLimitSeconds": 0,
      |  "navigationMode": "LINEAR",
      |  "testPartId": "test-part-1",
      |  "description": "",
      |  "items": "[{\"questionId\":\"q1\",\"questionCode\":\"q1Code\"}]",
      |  "shuffleQuestions": true,
      |  "occurredOn": 1745401038648,
      |  "id": "section-id-1",
      |  "testPartVersionId": "testpart-v1",
      |  "metadata": "{\"tags\":[]}",
      |  "title": "Introduction to Earth",
      |  "type": "FIXED_FORM",
      |  "submissionMode": "INDIVIDUAL"
      |},
      |{
      |  "_load_date": "20250430",
      |  "_load_time": 1746014156873,
      |  "_trace_id": "trace-id-2",
      |  "_source_type": "ALEF-KAFKA",
      |  "_app_tenant": "tenant-1",
      |  "_source_app": "SHARED",
      |  "_change_type": "insert",
      |  "_commit_version": 1,
      |  "_commit_timestamp": "2025-04-30T10:43:10.000Z",
      |  "eventType": "SectionPlannedInTestPartDataEvent",
      |  "timeLimitSeconds": 0,
      |  "navigationMode": "LINEAR",
      |  "testPartId": "test-part-1",
      |  "description": "",
      |  "items": "[{\"questionId\":\"q2\",\"questionCode\":\"q2Code\"}]",
      |  "shuffleQuestions": true,
      |  "occurredOn": 1745401038748,
      |  "id": "section-id-1",
      |  "testPartVersionId": "testpart-v2",
      |  "metadata": "{\"tags\":[]}",
      |  "title": "Introduction to Earth",
      |  "type": "FIXED_FORM",
      |  "submissionMode": "INDIVIDUAL"
      |}
      |]
      |""".stripMargin

  test("should construct pathway target dimension dataframe when Pathway events mutated event flows") {
    val expectedColumns = Set(
      "dw_id",
      "created_time",
      "dw_created_time",
      "active_until",
      "status",
      "_trace_id",
      "tenant_id",
      "event_type",
      "id",
      "testpart_id",
      "testpart_version_id",
      "description",
      "title",
      "metadata",
      "question_id",
      "question_code",
      "submission_mode",
      "navigation_mode",
      "time_limit_seconds",
      "shuffle_questions",
      "type"
    )

    val sprk = spark
    import sprk.implicits._
    val transformer = new GenericSCD2Transformation("assessment-testpart-section-mutated-transform")

    val updatedDF = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map("bronze-assessment-testpart-source" -> Some(updatedDF)), 0).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)
    assert[Int](df, "dw_id", 0)
    assert[String](df, "created_time", "2025-04-23 09:37:18.648")
    assert[String](df, "_trace_id", "trace-id-1")
    assert[String](df, "tenant_id", "tenant-1")
    assert[String](df, "event_type", "SectionPlannedInTestPartDataEvent")
    assert[String](df, "id", "section-id-1")
    assert[String](df, "testpart_id", "test-part-1")
    assert[String](df, "testpart_version_id", "testpart-v1")
    assert[String](df, "question_id", "q1")
    assert[String](df, "question_code", "q1Code")
    assert[String](df, "submission_mode", "INDIVIDUAL")
    assert[String](df, "navigation_mode", "LINEAR")
    assert[Int](df, "time_limit_seconds", 0)
    assert[Boolean](df, "shuffle_questions", true)
    assert[String](df, "type", "FIXED_FORM")
    assert[Int](df, "status", 2)
  }
}
