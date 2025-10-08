package com.alefeducation.generic_impl.facts

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.generic_impl.GenericFactTransform
import org.apache.spark.sql.SparkSession
import org.scalatestplus.mockito.MockitoSugar.mock

class AssessmentCandidateLockUnlockTest extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should transform fact when only Candidate Lock Unlock event is present") {
    val updatedValue =
      """
        |[
        |{
        |  "_load_date": "20250417",
        |  "_load_time": 1744919149809,
        |  "_trace_id": "b3bb100e-35a1-4ef8-9115-2f56c2854180",
        |  "_source_type": "ALEF-KAFKA",
        |  "_app_tenant": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "_source_app": "NEXTGEN",
        |  "_change_type": "insert",
        |  "_commit_version": 4,
        |  "_commit_timestamp": "2025-04-17T19:18:52.000Z",
        |  "eventType": "CandidateAssessmentUnlockedEvent",
        |  "sessionPeriodTag": "2025-2026",
        |  "candidateId": "3180ec9e-cf47-4099-a688-7931a8a276d2",
        |  "testLevel": "TEST_PART",
        |  "subject": "Arabic",
        |  "testLevelId": "431feb78-4c82-4a4a-b025-e188381e755c",
        |  "schoolId": "979a7f81-c253-4dfb-b1d0-86cddb4140a0",
        |  "uuid": "72cf40f3-ec9a-47db-8301-e99fcdef96c1",
        |  "occurredOn": 1744795913025,
        |  "testPartSessionId": "cf898e40-02ce-4b7a-b3b8-2fab2dee1954",
        |  "candidateGroupId": "16b54752-4df4-4897-ac2c-78849cfa25e5",
        |  "testLevelVersion": 2,
        |  "attemptNumber": 3,
        |  "skill": "Writing",
        |  "sessionPeriodId": "39cf8f5e-05a0-4448-8b38-8b01a8a1db46",
        |  "modifiedBy": "ae45a4ba-6522-4836-a27f-86c2084bfc73"
        |},
        |{
        |  "_load_date": "20250417",
        |  "_load_time": 1744919149809,
        |  "_trace_id": "b75aae5e-5351-4c49-abc9-10cc87c4c910",
        |  "_source_type": "ALEF-KAFKA",
        |  "_app_tenant": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "_source_app": "NEXTGEN",
        |  "_change_type": "insert",
        |  "_commit_version": 4,
        |  "_commit_timestamp": "2025-04-17T19:18:52.000Z",
        |  "eventType": "CandidateAssessmentLockedEvent",
        |  "sessionPeriodTag": "2025-2026",
        |  "candidateId": "b0d9a0f9-573a-4b30-966a-d6ac0beac9ad",
        |  "testLevel": "TEST_PART",
        |  "subject": "Arabic",
        |  "testLevelId": "513faece-4301-4988-a39a-38b577ead48e",
        |  "schoolId": "5660808a-ef85-4756-b45b-76b03e9679cf",
        |  "uuid": "c959c46e-a2c6-42fb-ba34-6ceec3ac64c2",
        |  "occurredOn": 1744865491382,
        |  "testPartSessionId": "759e2d2d-7ed0-4d0c-bce0-cbb3570f2ac9",
        |  "candidateGroupId": "37a17c73-dea5-43aa-a1d9-7fd0a260553e",
        |  "testLevelVersion": 2,
        |  "attemptNumber": 2,
        |  "skill": "Speaking",
        |  "sessionPeriodId": "be1f389f-e1da-43c5-bf3b-65531eb4fd11",
        |  "modifiedBy": "852a7fed-8c3e-4e07-aec0-04c2ae150a3d"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "dw_id",
      "event_type",
      "_trace_id",
      "tenant_id",
      "id",
      "created_time",
      "dw_created_time",
      "teacher_id",
      "candidate_id",
      "school_id",
      "class_id",
      "test_level_name",
      "test_part_session_id",
      "test_level_version",
      "test_level_id",
      "skill",
      "attempt",
      "academic_year_tag",
      "subject",
      "eventdate",
      "date_dw_id"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform("assessment-lock-unlock-transform")
    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map(
      "bronze-assessment-lock-unlock-source" -> Some(inputDf)
    ), 1001).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)
    val dfRow = df.first()
    assertRow[String](dfRow, "_trace_id", "b3bb100e-35a1-4ef8-9115-2f56c2854180")
    assertRow[String](dfRow, "event_type", "CandidateAssessmentUnlockedEvent")
    assertRow[String](dfRow, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assertRow[String](dfRow, "id", "72cf40f3-ec9a-47db-8301-e99fcdef96c1")
    assertRow[String](dfRow, "test_part_session_id", "cf898e40-02ce-4b7a-b3b8-2fab2dee1954")
    assertRow[Int](dfRow, "test_level_version", 2)
    assertRow[String](dfRow, "school_id", "979a7f81-c253-4dfb-b1d0-86cddb4140a0")
    assertRow[String](dfRow, "candidate_id", "3180ec9e-cf47-4099-a688-7931a8a276d2")
    assertRow[String](dfRow, "skill", "Writing")
    assertRow[Int](dfRow, "dw_id", 1001)
    assertRow[String](dfRow, "date_dw_id", "20250416")
    assertRow[String](dfRow, "eventdate", "2025-04-16")
  }

}
