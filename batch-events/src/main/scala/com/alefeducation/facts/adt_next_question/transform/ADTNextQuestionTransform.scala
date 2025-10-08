package com.alefeducation.facts.adt_next_question.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.adt_next_question.transform.ADTNextQuestionTransform.{ADTNextQuestionCols, ADTNextQuestionSource, ADTNextQuestionTransformedSink, FactADTNextQuestionEntity}
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.TimeZone

class ADTNextQuestionTransform(val session: SparkSession, val service: SparkBatchService){

  import com.alefeducation.util.BatchTransformerUtility._

  val BREAKDOWN = "breakdown"

  def transform(): Option[DataSink] = {
    //Read Raw Data
    val qstnSource = service.readOptional(ADTNextQuestionSource, session, extraProps = List(("mergeSchema", "true")))
    val qstnTransformed = qstnSource
      .map(_
        .withColumn("response", col("response").cast(BooleanType))
        .withColumn("status", lit(1).cast(IntegerType))
        .transform(handleBreakdown)
        .addColIfNotExists("language", StringType)
        .addColIfNotExists("standardError", FloatType)
        .addColIfNotExists("attempt", IntegerType)
        .addColIfNotExists("classSubjectName", StringType)
        .addColIfNotExists("skill", StringType)
        .transformForInsertFact(ADTNextQuestionCols, FactADTNextQuestionEntity))

    qstnTransformed.map(DataSink(ADTNextQuestionTransformedSink, _))

}

  def handleBreakdown(df: DataFrame): DataFrame =
    if (df.columns.contains(BREAKDOWN)) {
      if (df.schema(BREAKDOWN).dataType.typeName == "struct") {
        df.withColumn(BREAKDOWN, to_json(col(BREAKDOWN)).cast(StringType))
      } else df
    } else {
      df.withColumn(BREAKDOWN, lit("{}"))
    }
}


object ADTNextQuestionTransform {
  val ADTNextQuestionTransformService = "transform-adt-next-question"
  val ADTNextQuestionSource = "parquet-next-question-source"
  val ADTNextQuestionTransformedSink = "transformed-next-question-sink"
  val FactADTNextQuestionEntity = "fanq"


  val ADTNextQuestionCols: Map[String, String] = Map(
    "id" -> "fanq_id",
    "learningSessionId" -> "fle_ls_uuid",
    "studentId" -> "student_uuid",
    "questionPoolId" -> "fanq_question_pool_id",
    "response" -> "fanq_response",
    "proficiency" -> "fanq_proficiency",
    "nextQuestion" -> "fanq_next_question_id",
    "timeSpent" -> "fanq_time_spent",
    "tenantId" -> "tenant_uuid",
    "currentQuestion" -> "fanq_current_question_id",
    "intestProgress" -> "fanq_intest_progress",
    "status" -> "fanq_status",
    "curriculumSubjectId" -> "fanq_curriculum_subject_id",
    "curriculumSubjectName" -> "fanq_curriculum_subject_name",
    "breakdown" -> "fanq_breakdown",
    "occurredOn" -> "occurredOn",
    "language" -> "fanq_language",
    "standardError" -> "fanq_standard_error",
    "attempt" -> "fanq_attempt",
    "grade" -> "fanq_grade",
    "gradeId" -> "fanq_grade_id",
    "academicYear" -> "fanq_academic_year",
    "academicYearId" -> "fanq_academic_year_id",
    "academicTerm" -> "fanq_academic_term",
    "classSubjectName" -> "fanq_class_subject_name",
    "skill" -> "fanq_skill"
  )

  val session = SparkSessionUtils.getSession(ADTNextQuestionTransformService)
  val service = new SparkBatchService(ADTNextQuestionTransformService, session)

  def main(args: Array[String]): Unit = {
    val transform = new ADTNextQuestionTransform(session, service)
    service.run(transform.transform())
  }
}
