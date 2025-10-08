package com.alefeducation.generic_impl.facts

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.generic_impl.GenericFactTransform
import org.apache.spark.sql.SparkSession
import org.scalatestplus.mockito.MockitoSugar.mock

class AssessmentCandidateAnswerSubmitTest extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should transform fact when answer submit event is trigger") {
    val updatedValue =
      """
        |[
        |{
        |  "_load_date": "20250226",
        |  "_app_tenant": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "_load_time": 1740571617563,
        |  "_source_type": "ALEF-KAFKA",
        |  "_source_app": "NEXTGEN",
        |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
        |  "eventType": "AnswerSubmittedDataEvent",
        |  "occurredOn": 1747312152376,
        |  "assessmentId": "9dcdb6e0-5038-4578-bd26-82df88bce236",
        |	 "referenceCode": "SpeakingGrader",
        |	 "candidateId": "a580f0ad-5fc6-4860-ae02-5f652277aaec",
        |	 "candidateGroupId": "cf520874-d72e-40b5-8328-e57241a4e6c3",
        |	 "subject": "Arabic",
        |	 "skill": "Speaking",
        |	 "grade": 7,
        |	 "gradeId": "02df766b-0382-4c6d-82aa-5d596dabcd51",
        |	 "questionId": "33a13a76-8ebf-439a-ad2d-de5677bee4bb",
        |	 "questionCode": "Speaking_Oct_G71",
        |	 "questionVersion": 0,
        |	 "attemptNumber": 3,
        |	 "answer": "{answer=1356cb8b-7759-4647-a006-90d403ffbdf1, type=AUDIO_RECORDER}",
        |	 "timeSpent": 10.0,
        |	 "sessionPeriodTag": "2025-2026",
        |	 "sessionPeriodId": "39cf8f5e-05a0-4448-8b38-8b01a8a1db46",
        |	 "schoolId": "979a7f81-c253-4dfb-b1d0-86cddb4140a0",
        |  "testLevel": "TEST_PART",
        |  "testLevelId": "65f8eaf6-e0f4-4711-8d81-c3ca68fbaf31",
        |  "testLevelVersion": 1,
        |  "testLevelSectionId": "9f58a3d8-9976-4a8b-97ac-1d69947a1b95",
        |  "testLevelSessionId": "be51582f-446c-47e9-a015-63295dff923d",
        |	 "materialType": "PATHWAY",
        |	 "language": "ar"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "dw_id",
      "created_time",
      "dw_created_time",
      "date_dw_id",
      "_trace_id",
      "tenant_id",
      "event_type",
      "assessment_id",
      "academic_year_tag",
      "school_id",
      "grade_id",
      "candidate_id",
      "class_id",
      "grade",
      "material_type",
      "attempt_number",
      "skill",
      "subject",
      "language",
      "answer",
      "test_level_session_id",
      "test_level_id",
      "test_level_version",
      "test_level_section_id",
      "test_level",
      "time_spent",
      "question_id",
      "question_code",
      "question_version",
      "reference_code",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform("assessment-answer-submit-transform")
    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map(
      "bronze-assessment-answer-submit-source" -> Some(inputDf)
    ), 1001).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    val dfRow = df.first()
    assertRow[String](dfRow, "_trace_id", "1dc711b3-96e1-47ab-8376-8427e211155c")
    assertRow[String](dfRow, "event_type", "AnswerSubmittedDataEvent")
    assertRow[String](dfRow, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assertRow[String](dfRow, "school_id", "979a7f81-c253-4dfb-b1d0-86cddb4140a0")
    assertRow[String](dfRow, "candidate_id", "a580f0ad-5fc6-4860-ae02-5f652277aaec")
    assertTimestampRow(dfRow, "created_time", "2025-05-15 12:29:12.376")
    assertRow[Int](dfRow, "dw_id", 1001)
    assertRow[String](dfRow, "date_dw_id", "20250515")
    assertRow[String](dfRow, "eventdate", "2025-05-15")
  }

}
