package com.alefeducation.facts.adt_student_report

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.adt_student_report.transform.AdtStudentReportTransform
import com.alefeducation.facts.adt_student_report.transform.AdtStudentReportTransform.{ADTStudentReportTransformedSink, ParquetStudentReportSource}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class AdtStudentReportTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedCols = Set(
    "fasr_created_time",
    "fasr_dw_created_time",
    "fasr_date_dw_id",
    "fasr_question_pool_id",
    "fasr_id",
    "fasr_final_score",
    "fasr_final_proficiency",
    "fasr_final_result",
    "fasr_total_time_spent",
    "fasr_academic_year",
    "fasr_academic_term",
    "fasr_test_id",
    "fasr_curriculum_subject_id",
    "fasr_curriculum_subject_name",
    "fasr_status",
    "fasr_final_uncertainty",
    "fasr_framework",
    "eventdate",
    "fasr_language",
    "fasr_final_standard_error",
    "fasr_attempt",
    "fasr_final_grade",
    "fasr_tenant_id",
    "fasr_student_id",
    "fasr_fle_ls_id",
    "fasr_school_id",
    "fasr_breakdown",
    "fasr_forecast_score",
    "fasr_final_category",
    "fasr_grade",
    "fasr_grade_id",
    "fasr_academic_year_id",
    "fasr_secondary_result",
    "fasr_class_subject_name",
    "fasr_skill"
  )

  test("transform in conversation occurred fact successfully") {
    val msg = makeMsg(
      """,
        |"language": "EN_GB",
        |"schoolId": "f8d4bc9a-0c75-4345-9a59-7a63f775ba4c",
        |"finalStandardError": 0.5
        |""".stripMargin)

    val sprk = spark
    import sprk.implicits._

    val adtStudentReportTransformer = new AdtStudentReportTransform(sprk, service)
    val adtStudentReportDF = spark.read.json(Seq(msg).toDS())
    when(service.readOptional(ParquetStudentReportSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(adtStudentReportDF))
    val sinks = adtStudentReportTransformer.transform()
    val transformDf = sinks.find(_.name == ADTStudentReportTransformedSink).get.output
    assert(transformDf.columns.toSet === expectedCols)

    assertCommonColsOfDf(transformDf)

    assert[String](transformDf, "fasr_language", "EN_GB")
    assert[String](transformDf, "fasr_school_id", "f8d4bc9a-0c75-4345-9a59-7a63f775ba4c")
    assert[Double](transformDf, "fasr_final_standard_error", 0.5)
    assert[Double](transformDf, "fasr_forecast_score", 9.5)
    assert[String](transformDf, "fasr_class_subject_name", null)

  }

  test("when class subject name is present transform conversation occurred fact successfully") {
    val msg = makeMsg(
      """,
        |"language": "EN_GB",
        |"schoolId": "f8d4bc9a-0c75-4345-9a59-7a63f775ba4c",
        |"finalStandardError": 0.5,
        |"classSubjectName": "test-subject"
        |""".stripMargin)

    val sprk = spark
    import sprk.implicits._

    val adtStudentReportTransformer = new AdtStudentReportTransform(sprk, service)
    val adtStudentReportDF = spark.read.json(Seq(msg).toDS())
    when(service.readOptional(ParquetStudentReportSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(adtStudentReportDF))
    val sinks = adtStudentReportTransformer.transform()
    val transformDf = sinks.find(_.name == ADTStudentReportTransformedSink).get.output
    assert(transformDf.columns.toSet === expectedCols)

    assertCommonColsOfDf(transformDf)

    assert[String](transformDf, "fasr_language", "EN_GB")
    assert[String](transformDf, "fasr_school_id", "f8d4bc9a-0c75-4345-9a59-7a63f775ba4c")
    assert[Double](transformDf, "fasr_final_standard_error", 0.5)
    assert[Double](transformDf, "fasr_forecast_score", 9.5)
    assert[String](transformDf, "fasr_class_subject_name", "test-subject")

  }

  test("process StudentReport without language, schoolId and finalStandardError") {
    val msg = makeMsg("")

    val sprk = spark
    import sprk.implicits._

    val adtStudentReportTransformer = new AdtStudentReportTransform(sprk, service)
    val adtStudentReportDF = spark.read.json(Seq(msg).toDS())
    when(service.readOptional(ParquetStudentReportSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(adtStudentReportDF))
    val sinks = adtStudentReportTransformer.transform()

    val transformedDf = sinks.find(_.name == ADTStudentReportTransformedSink).get.output
    assert(transformedDf.columns.toSet === expectedCols)

    assertCommonColsOfDf(transformedDf)

    assert[String](transformedDf, "fasr_final_standard_error", null)
    assert[String](transformedDf, "fasr_language", null)
    assert[String](transformedDf, "fasr_school_id", null)
  }

  private def assertCommonColsOfDf(redshiftDf: DataFrame) = {
    assert[String](redshiftDf, "fasr_id", "33617304-9917-4810-9bb2-e01f020f8359")
    assert[String](redshiftDf, "fasr_fle_ls_id", "99336173-0417-4810-9bb2-e01f020f8359")
    assert[String](redshiftDf, "fasr_student_id", "6e8ad42f-45e8-4a6d-97d9-20af34935494")
    assert[String](redshiftDf, "fasr_question_pool_id", "5e37b39d-0494-6600-015a-abe6abe6abe6")
    assert[Double](redshiftDf, "fasr_final_score", 20.0)
    assert[Double](redshiftDf, "fasr_final_proficiency", 1.0)
    assert[String](redshiftDf, "fasr_final_result", "40")
    assert[Int](redshiftDf, "fasr_total_time_spent", 20)
    assert[Int](redshiftDf, "fasr_academic_year", 2021)
    assert[Int](redshiftDf, "fasr_academic_term", 1)
    assert[String](redshiftDf, "fasr_test_id", "11815da2-aaad-4804-8fc6-a9e5dad33ff3")
    assert[Int](redshiftDf, "fasr_curriculum_subject_id", 423412)
    assert[String](redshiftDf, "fasr_curriculum_subject_name", "English_MOE")
    assert[Int](redshiftDf, "fasr_status", 1)
    assert[Double](redshiftDf, "fasr_final_uncertainty", 68.0)
    assert[String](redshiftDf, "fasr_framework", "quantile")
  }

  private def makeMsg(append: String): String =
    s"""
       |{
       |  "eventType": "StudentReport",
       |  "tenantId":"tenant-id",
       |  "id": "33617304-9917-4810-9bb2-e01f020f8359",
       |  "learningSessionId":"99336173-0417-4810-9bb2-e01f020f8359",
       |  "studentId":"6e8ad42f-45e8-4a6d-97d9-20af34935494",
       |  "questionPoolId":"5e37b39d-0494-6600-015a-abe6abe6abe6",
       |  "finalProficiency": 1.0,
       |  "finalScore": 20.0,
       |  "finalResult": "40",
       |  "totalTimespent":20,
       |  "academicYear": 2021,
       |  "academicTerm": 1,
       |  "testId": "11815da2-aaad-4804-8fc6-a9e5dad33ff3",
       |  "curriculumSubjectId": 423412,
       |  "curriculumSubjectName": "English_MOE",
       |  "finalUncertainty": 68.0,
       |  "finalLevel": "MEETS",
       |  "framework": "quantile",
       |  "breakdown": "{}",
       |  "eventDateDw": 20190115,
       |  "occurredOn": "2019-01-15 09:02:56.768",
       |  "forecastScore": 9.5,
       |  "finalCategory": "PROCESSING",
       |  "grade": 6,
       |  "gradeId": "grade-id-1",
       |  "academicYearId": "ay-1",
       |  "secondaryResult": "good",
       |  "skill": "listening"
       |  $append
       |}
       |""".stripMargin

}
