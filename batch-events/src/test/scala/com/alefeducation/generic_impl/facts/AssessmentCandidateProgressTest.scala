package com.alefeducation.generic_impl.facts

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.generic_impl.GenericFactTransform
import org.apache.spark.sql.SparkSession
import org.scalatestplus.mockito.MockitoSugar.mock

class AssessmentCandidateProgressTest extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should transform fact when only Candidate progress events are present") {
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
        |  "eventType": "CandidateStartedAssessmentEvent",
        |  "occurredOn": 1709016913763,
        |  "assessmentId": "65dd8751d130a58ae9582819",
        |  "sessionPeriodTag": "2023-2024",
        |  "schoolId": "school_id",
        |  "gradeId": "grade_id",
        |  "grade": 5,
        |  "candidateId": "student_164247d6-395418",
        |  "candidateGroupId": "class-395418",
        |  "materialType": "PATHWAY",
        |  "attemptNumber": 1,
        |  "skill": "Speaking",
        |  "subject": "Arabic",
        |  "language": "EN_GB",
        |  "status": "RELEASED",
        |  "testLevel": "TEST_PART",
        |  "testLevelSessionId": "e44088f3-5bc1-412f-9c9f-3bc89319cee5",
        |  "testLevelId": "e44088f3-5bc1-412f-9c9f-3bc89319cee3",
        |  "testLevelVersion": 2,
        |  "testLevelSectionId": "e44088f3-5bc1-412f-9c9f-3bc89319cee4",
        |  "testId": "e44088f3-5bc1-412f-9c9f-3bc89319cee2"
        |},
        |{
        |  "_load_date": "20250226",
        |  "_app_tenant": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "_load_time": 1740571617563,
        |  "_source_type": "ALEF-KAFKA",
        |  "_source_app": "NEXTGEN",
        |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
        |  "eventType": "CandidateFinishedAssessmentEvent",
        |  "occurredOn": 1709016913863,
        |  "assessmentId": "65dd8751d130a58ae9582819",
        |  "sessionPeriodTag": "2023-2024",
        |  "schoolId": "school_id",
        |  "gradeId": "grade_id",
        |  "grade": 5,
        |  "candidateId": "student_164247d6-395418",
        |  "candidateGroupId": "class-395418",
        |  "materialType": "PATHWAY",
        |  "attemptNumber": 1,
        |  "skill": "Speaking",
        |  "subject": "Arabic",
        |  "language": "EN_GB",
        |  "status": "RELEASED",
        |  "testLevel": "TEST_PART",
        |  "testLevelSessionId": "e44088f3-5bc1-412f-9c9f-3bc89319cee5",
        |  "testLevelId": "e44088f3-5bc1-412f-9c9f-3bc89319cee3",
        |  "testLevelVersion": 2,
        |  "testLevelSectionId": "e44088f3-5bc1-412f-9c9f-3bc89319cee4",
        |  "testId": "e44088f3-5bc1-412f-9c9f-3bc89319cee2",
        |  "totalTimespent": 300
        |},
        |{
        |  "_load_date": "20250226",
        |  "_app_tenant": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "_load_time": 1740571617563,
        |  "_source_type": "ALEF-KAFKA",
        |  "_source_app": "NEXTGEN",
        |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
        |  "eventType": "CandidateReportGeneratedDataEvent",
        |  "occurredOn": 1709016913963,
        |  "assessmentId": "65dd8751d130a58ae9582819",
        |  "sessionPeriodTag": "2023-2024",
        |  "schoolId": "school_id",
        |  "gradeId": "grade_id",
        |  "grade": 5,
        |  "candidateId": "student_164247d6-395418",
        |  "candidateGroupId": "class-395418",
        |  "materialType": "PATHWAY",
        |  "attemptNumber": 1,
        |  "skill": "Speaking",
        |  "subject": "Arabic",
        |  "language": "EN_GB",
        |  "status": "RELEASED",
        |  "testLevel": "TEST_PART",
        |  "testLevelSessionId": "e44088f3-5bc1-412f-9c9f-3bc89319cee5",
        |  "testLevelId": "e44088f3-5bc1-412f-9c9f-3bc89319cee3",
        |  "testLevelVersion": 2,
        |  "testLevelSectionId": "e44088f3-5bc1-412f-9c9f-3bc89319cee4",
        |  "testId": "e44088f3-5bc1-412f-9c9f-3bc89319cee2",
        |  "testVersion": 2,
        |  "totalTimespent": 300,
        |  "reportId": "report-id",
        |  "finalScore": 484,
        |  "finalGrade": 4,
        |  "finalCategory": "APPROACHING",
        |  "finalUncertainty": 79,
        |  "framework": "alef",
        |  "timeToReturn": 3000,
        |  "domains":"[{\"finalScore\":570,\"finalUncertainty\":79,\"name\":\"Geometry\",\"finalGrade\":5,\"finalCategory\":\"MEETS\"}]"
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
      "grade",
      "class_id",
      "candidate_id",
      "material_type",
      "attempt_number",
      "skill",
      "subject",
      "language",
      "status",
      "test_level",
      "test_level_session_id",
      "test_level_id",
      "test_level_version",
      "test_level_section_id",
      "test_id",
      "test_version",
      "report_id",
      "total_timespent",
      "final_score",
      "final_grade",
      "final_category",
      "final_uncertainty",
      "framework",
      "time_to_return",
      "domains",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform("assessment-candidate-progress-transform")
    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer
      .transform(Map(
                   "bronze-assessment-candidate-progress-source" -> Some(inputDf)
                 ),
                 1001)
      .getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 3)

    val dfRow = df.first()
    assertRow[String](dfRow, "_trace_id", "1dc711b3-96e1-47ab-8376-8427e211155c")
    assertRow[String](dfRow, "event_type", "CandidateStartedAssessmentEvent")
    assertRow[String](dfRow, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assertRow[String](dfRow, "school_id", "school_id")
    assertRow[String](dfRow, "candidate_id", "student_164247d6-395418")
    assertRow[String](dfRow, "class_id", "class-395418")
    assertTimestamp(df, "created_time", "2024-02-27 06:55:13.763")
    assertRow[Int](dfRow, "dw_id", 1001)
    assertRow[String](dfRow, "date_dw_id", "20240227")
    assertRow[String](dfRow, "eventdate", "2024-02-27")
    assertRow[String](dfRow, "status", "RELEASED")
  }

  test("should transform fact when only Candidate Assessment started event is present") {
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
        |  "eventType": "CandidateStartedAssessmentEvent",
        |  "occurredOn": 1709016913763,
        |  "assessmentId": "65dd8751d130a58ae9582819",
        |  "sessionPeriodTag": "2023-2024",
        |  "schoolId": "school_id",
        |  "gradeId": "grade_id",
        |  "grade": 5,
        |  "candidateId": "student_164247d6-395418",
        |  "candidateGroupId": "class-395418",
        |  "materialType": "PATHWAY",
        |  "attemptNumber": 1,
        |  "skill": "Speaking",
        |  "subject": "Arabic",
        |  "language": "EN_GB",
        |  "status": "RELEASED",
        |  "testLevel": "TEST_PART",
        |  "testLevelSessionId": "e44088f3-5bc1-412f-9c9f-3bc89319cee5",
        |  "testLevelId": "e44088f3-5bc1-412f-9c9f-3bc89319cee3",
        |  "testLevelVersion": 2,
        |  "testLevelSectionId": "e44088f3-5bc1-412f-9c9f-3bc89319cee4",
        |  "testId": "e44088f3-5bc1-412f-9c9f-3bc89319cee2",
        |  "testVersion": 2
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
      "grade",
      "candidate_id",
      "class_id",
      "material_type",
      "attempt_number",
      "skill",
      "subject",
      "language",
      "status",
      "test_level",
      "test_level_session_id",
      "test_level_id",
      "test_level_version",
      "test_level_section_id",
      "test_id",
      "report_id",
      "test_version",
      "total_timespent",
      "final_score",
      "final_grade",
      "final_category",
      "final_uncertainty",
      "framework",
      "time_to_return",
      "domains",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform("assessment-candidate-progress-transform")
    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map(
      "bronze-assessment-candidate-progress-source" -> Some(inputDf)
    ), 1001).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    val dfRow = df.first()
    assertRow[String](dfRow, "_trace_id", "1dc711b3-96e1-47ab-8376-8427e211155c")
    assertRow[String](dfRow, "event_type", "CandidateStartedAssessmentEvent")
    assertRow[String](dfRow, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assertRow[String](dfRow, "school_id", "school_id")
    assertRow[String](dfRow, "candidate_id", "student_164247d6-395418")
    assertRow[String](dfRow, "class_id", "class-395418")
    assertRow[String](dfRow, "final_uncertainty", null)
    assertTimestamp(df, "created_time", "2024-02-27 06:55:13.763")
    assertRow[Int](dfRow, "dw_id", 1001)
    assertRow[String](dfRow, "date_dw_id", "20240227")
    assertRow[String](dfRow, "eventdate", "2024-02-27")
  }

  test("should transform fact when only report generated event is present") {
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
        |  "eventType": "CandidateReportGeneratedDataEvent",
        |  "occurredOn": 1709016913963,
        |  "assessmentId": "65dd8751d130a58ae9582819",
        |  "sessionPeriodTag": "2023-2024",
        |  "schoolId": "school_id",
        |  "gradeId": "grade_id",
        |  "grade": 5,
        |  "candidateId": "student_164247d6-395418",
        |  "candidateGroupId": "class-395418",
        |  "materialType": "PATHWAY",
        |  "attemptNumber": 1,
        |  "skill": "Speaking",
        |  "subject": "Arabic",
        |  "language": "EN_GB",
        |  "status": "RELEASED",
        |  "testLevel": "TEST_PART",
        |  "testLevelSessionId": "e44088f3-5bc1-412f-9c9f-3bc89319cee5",
        |  "testLevelId": "e44088f3-5bc1-412f-9c9f-3bc89319cee3",
        |  "testLevelVersion": 2,
        |  "testLevelSectionId": "e44088f3-5bc1-412f-9c9f-3bc89319cee4",
        |  "testId": "e44088f3-5bc1-412f-9c9f-3bc89319cee2",
        |  "testVersion": 2,
        |  "totalTimespent": 300,
        |  "reportId": "report-id",
        |  "finalScore": 484,
        |  "finalGrade": 4,
        |  "finalCategory": "APPROACHING",
        |  "finalUncertainty": 79,
        |  "framework": "alef",
        |  "timeToReturn": 3000,
        |  "domains":"[{\"finalScore\":570,\"finalUncertainty\":79,\"name\":\"Geometry\",\"finalGrade\":5,\"finalCategory\":\"MEETS\"}]"
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
      "grade",
      "candidate_id",
      "class_id",
      "material_type",
      "attempt_number",
      "skill",
      "subject",
      "language",
      "status",
      "test_level",
      "test_level_session_id",
      "test_level_id",
      "test_level_version",
      "test_level_section_id",
      "test_id",
      "test_version",
      "report_id",
      "total_timespent",
      "final_score",
      "final_grade",
      "final_category",
      "final_uncertainty",
      "framework",
      "time_to_return",
      "domains",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform("assessment-candidate-progress-transform")
    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map(
      "bronze-assessment-candidate-progress-source" -> Some(inputDf)
    ), 1001).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    val dfRow = df.first()

    assertRow[String](dfRow, "_trace_id", "1dc711b3-96e1-47ab-8376-8427e211155c")
    assertRow[String](dfRow, "event_type", "CandidateReportGeneratedDataEvent")
    assertRow[String](dfRow, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assertRow[String](dfRow, "school_id", "school_id")
    assertRow[String](dfRow, "candidate_id", "student_164247d6-395418")
    assertRow[String](dfRow, "class_id", "class-395418")
    assertRow[Int](dfRow, "final_uncertainty", 79)
    assertTimestamp(df, "created_time", "2024-02-27 06:55:13.963")
    assertRow[Int](dfRow, "dw_id", 1001)
    assertRow[String](dfRow, "date_dw_id", "20240227")
    assertRow[String](dfRow, "eventdate", "2024-02-27")
  }

}
