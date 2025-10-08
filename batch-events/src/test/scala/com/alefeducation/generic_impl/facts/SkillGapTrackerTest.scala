package com.alefeducation.generic_impl.facts

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.generic_impl.GenericFactTransform
import org.apache.spark.sql.SparkSession
import org.scalatestplus.mockito.MockitoSugar.mock

class SkillGapTrackerTest extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should transform fact when skill area of concern introduced") {
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
        |  "uuid": "9d11a768-37b4-4a93-8bf3-5f46d43c7212",
        |  "studentId": "6d4584eb-8d66-491e-9e2f-f08a4ad076b4",
        |  "classId": "3b2680bf-f4f1-4b06-b283-f8c54fe0509b",
        |  "schoolId": "3c4d2d1a-9e66-4aa8-bdbc-16a520fd3450",
        |  "pathwayId": "33bd1e82-2ab2-448e-a1c2-039bb622bb96",
        |  "levelId": "5bac56a6-0c26-43b8-b2cc-18e1e6f1a4a8",
        |  "academicYearTag": "2024-2025",
        |  "sessionId": "8ff238a6-5449-42a6-94e2-5667dfd8e071",
        |  "mlSessionId": "9063f074-b101-402f-b167-aa58e3a6f5a3",
        |  "levelProficiencyScore": 0.80,
        |  "levelProficiencyTier": "EXPLORING",
        |  "assessmentId": "0a74a826-bfa8-4fa7-a911-420d9b178dcd",
        |  "sessionAttempt": 2,
        |  "stars": 10,
        |  "timeSpent": 240,
        |  "skillId": "52043ed1-91d5-48b5-9c4b-a1263742b4e2",
        |  "skillProficiencyTier": "EXPLORING",
        |  "skillProficiencyScore": 0.4,
        |  "status" : "INTRODUCED",
        |  "occurredOn": "2024-03-11T09:27:08.452"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
        "dw_id",
        "event_type",
        "_trace_id",
        "tenant_id",
        "date_dw_id",
        "uuid",
        "created_time",
        "dw_created_time",
        "student_id",
        "class_id",
        "school_id",
        "pathway_id",
        "level_id",
        "academic_year_tag",
        "session_id",
        "ml_session_id",
        "level_proficiency_score",
        "level_proficiency_tier",
        "assessment_id",
        "session_attempt",
        "stars",
        "time_spent",
        "skill_proficiency_tier",
        "skill_proficiency_score",
        "skill_id",
        "status",
        "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform("skill-gap-tracker-transform")
    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map(
      "bronze-skill-gap-tracker-source" -> Some(inputDf)
    ), 1001).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    val dfRow = df.first()
    assertRow[String](dfRow, "_trace_id", "b3bb100e-35a1-4ef8-9115-2f56c2854180")
    assertRow[String](dfRow, "event_type", "CandidateAssessmentUnlockedEvent")
    assertRow[String](dfRow, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assertRow[String](dfRow, "uuid", "9d11a768-37b4-4a93-8bf3-5f46d43c7212")
  }

  test("should transform fact when proficiency change") {
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
        |  "uuid": "9d11a768-37b4-4a93-8bf3-5f46d43c7212",
        |  "studentId": "6d4584eb-8d66-491e-9e2f-f08a4ad076b4",
        |  "classId": "3b2680bf-f4f1-4b06-b283-f8c54fe0509b",
        |  "schoolId": "3c4d2d1a-9e66-4aa8-bdbc-16a520fd3450",
        |  "pathwayId": "33bd1e82-2ab2-448e-a1c2-039bb622bb96",
        |  "levelId": "5bac56a6-0c26-43b8-b2cc-18e1e6f1a4a8",
        |  "academicYearTag": "2024-2025",
        |  "sessionId": "8ff238a6-5449-42a6-94e2-5667dfd8e071",
        |  "mlSessionId": "9063f074-b101-402f-b167-aa58e3a6f5a3",
        |  "levelProficiencyScore": 0.80,
        |  "levelProficiencyTier": "PRACTICING",
        |  "assessmentId": "0a74a826-bfa8-4fa7-a911-420d9b178dcd",
        |  "sessionAttempt": 2,
        |  "stars": 10,
        |  "timeSpent": 240,
        |  "previousProficiencyTier": "EXPLORING",
        |  "status" : "UPGRADED",
        |  "occurredOn": "2024-03-11T09:27:08.452",
        |  "skillId": "52043ed1-91d5-48b5-9c4b-a1263742b4e2",
        |  "skillProficiencyTier": "EXPLORING",
        |  "skillProficiencyScore": 0.4
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "dw_id",
      "event_type",
      "_trace_id",
      "tenant_id",
      "student_id",
      "class_id",
      "pathway_id",
      "level_id",
      "school_id",
      "academic_year_tag",
      "session_id",
      "ml_session_id",
      "assessment_id",
      "session_attempt",
      "stars",
      "time_spent",
      "previous_proficiency_tier",
      "status",
      "uuid",
      "skill_proficiency_score",
      "skill_proficiency_tier",
      "level_proficiency_score",
      "level_proficiency_tier",
      "skill_id",
      "eventdate",
      "created_time",
      "dw_created_time",
      "date_dw_id"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform("student-proficiency-tracker-transform")
    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map(
      "bronze-student-proficiency-tracker-source" -> Some(inputDf)
    ), 1001).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    val dfRow = df.first()
    assertRow[String](dfRow, "_trace_id", "b3bb100e-35a1-4ef8-9115-2f56c2854180")
    assertRow[String](dfRow, "event_type", "CandidateAssessmentUnlockedEvent")
    assertRow[String](dfRow, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assertRow[String](dfRow, "uuid", "9d11a768-37b4-4a93-8bf3-5f46d43c7212")
  }
}
