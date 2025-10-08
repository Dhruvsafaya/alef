package com.alefeducation.facts.adt_student_report.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class AdtStudentReportTransform(val session: SparkSession, val service: SparkBatchService) {
  import AdtStudentReportTransform._
  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {
    val studentReport = service.readOptional(ParquetStudentReportSource, session, extraProps = List(("mergeSchema", "true")))

    val transformedStudentReport =
      studentReport
        .map(
          _.withColumn("status", lit(1).cast(IntegerType))
            .withColumn("curriculumSubjectId", col("curriculumSubjectId").cast(LongType))
        )
        .map(_.transform(addColsIfNotExists()))
        .map(_.transformForInsertFact(AdtStudentReportColMapping, StudentReportEntity, ids = List("id")).cache())

    transformedStudentReport.map(DataSink(ADTStudentReportTransformedSink, _))
  }

  def addColsIfNotExists(): DataFrame => DataFrame =
    _.addColIfNotExists("language", StringType)
      .addColIfNotExists("schoolId", StringType)
      .addColIfNotExists("finalStandardError", FloatType)
      .addColIfNotExists("attempt", IntegerType)
      .addColIfNotExists("finalGrade", IntegerType)
      .addColIfNotExists("classSubjectName", StringType)
      .addColIfNotExists("skill", StringType)
}

object AdtStudentReportTransform {
  val StudentReportEntity = "fasr"

  val eventName = "StudentReport"

  val AdtStudentReportTransformService = "adt-student-report-transform"
  val ParquetStudentReportSource = "parquet-adt-student-report-source"

  val ADTStudentReportTransformedSink = "adt-student-report-transformed-sink"

  val AdtStudentReportColMapping = Map(
    "id" -> "fasr_id",
    "tenantId" -> "fasr_tenant_id",
    "learningSessionId" -> "fasr_fle_ls_id",
    "studentId" -> "fasr_student_id",
    "questionPoolId" -> "fasr_question_pool_id",
    "finalProficiency" -> "fasr_final_proficiency",
    "finalScore" -> "fasr_final_score",
    "finalResult" -> "fasr_final_result",
    "totalTimespent" -> "fasr_total_time_spent",
    "eventDateDw" -> "fasr_date_dw_id",
    "academicYear" -> "fasr_academic_year",
    "academicTerm" -> "fasr_academic_term",
    "testId" -> "fasr_test_id",
    "curriculumSubjectId" -> "fasr_curriculum_subject_id",
    "curriculumSubjectName" -> "fasr_curriculum_subject_name",
    "status" -> "fasr_status",
    "finalUncertainty" -> "fasr_final_uncertainty",
    "framework" -> "fasr_framework",
    "breakdown" -> "fasr_breakdown",
    "occurredOn" -> "occurredOn",
    "finalStandardError" -> "fasr_final_standard_error",
    "language" -> "fasr_language",
    "schoolId" -> "fasr_school_id",
    "attempt" -> "fasr_attempt",
    "finalGrade" -> "fasr_final_grade",
    "forecastScore" -> "fasr_forecast_score",
    "finalCategory" -> "fasr_final_category",
    "grade" -> "fasr_grade",
    "gradeId" -> "fasr_grade_id",
    "academicYearId" -> "fasr_academic_year_id",
    "secondaryResult" -> "fasr_secondary_result",
    "classSubjectName" -> "fasr_class_subject_name",
    "skill" -> "fasr_skill"
  )

  val session = SparkSessionUtils.getSession(AdtStudentReportTransformService)
  val service = new SparkBatchService(AdtStudentReportTransformService, session)

  def main(args: Array[String]): Unit = {
    val transform = new AdtStudentReportTransform(session, service)
    service.run(transform.transform())
  }

}
