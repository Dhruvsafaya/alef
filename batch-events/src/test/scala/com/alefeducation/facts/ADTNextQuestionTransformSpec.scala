package com.alefeducation.facts

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.adt_next_question.transform.ADTNextQuestionTransform
import com.alefeducation.facts.adt_next_question.transform.ADTNextQuestionTransform.ADTNextQuestionSource
import com.alefeducation.util.DataFrameEqualityUtils.createDfFromJson
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class ADTNextQuestionTransformSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedCols = Set(
    "fanq_created_time",
    "fanq_dw_created_time",
    "fanq_date_dw_id",
    "fanq_question_pool_id",
    "fanq_id",
    "fanq_response",
    "fanq_next_question_id",
    "fanq_proficiency",
    "fanq_time_spent",
    "fanq_current_question_id",
    "fanq_intest_progress",
    "fanq_status",
    "fanq_curriculum_subject_id",
    "fanq_curriculum_subject_name",
    "eventdate",
    "fanq_language",
    "fanq_standard_error",
    "fanq_attempt",
    "fanq_grade",
    "fanq_grade_id",
    "fanq_academic_year",
    "fanq_academic_year_id",
    "fanq_academic_term",
    "fanq_class_subject_name",
    "fanq_skill"
  )

  val expectedTransformedCols = expectedCols ++ Set(
    "fanq_breakdown",
    "tenant_uuid",
    "student_uuid",
    "fle_ls_uuid"
  )

  val fanqId = "33617304-9917-4810-9bb2-e01f020f8359"


  test("transform in adt next question fact successfully") {
    val msg = makeMsg(fanqId,
      s"""
         |,
         |"response": true,
         |"breakdown": "{ \\"mm_id\\": \\"Q1234\\", \\"mm_strand\\": \\"Geometry\\" }",
         |"occurredOn": "2019-01-15 09:02:56.768",
         |"language": "EN_GB",
         |"standardError": 0.5
      """.stripMargin)

    val sprk = spark
    import sprk.implicits._

    val ADTQuestionTransformer = new ADTNextQuestionTransform(sprk, service)
    val ADTNextQuestionDF = spark.read.json(Seq(msg).toDS())
    when(service.readOptional(ADTNextQuestionSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ADTNextQuestionDF))
    val transformedADTNextQuestionDF = ADTQuestionTransformer.transform().get.output
    assert(transformedADTNextQuestionDF.columns.toSet === expectedTransformedCols)

    assert[Boolean](transformedADTNextQuestionDF, "fanq_response", true)
    assert[Double](transformedADTNextQuestionDF, "fanq_standard_error", 0.5)
    assert[String](transformedADTNextQuestionDF, "fanq_language", "EN_GB")
    assert[Boolean](transformedADTNextQuestionDF, "fanq_response", true)
    assert[String](transformedADTNextQuestionDF, "fanq_breakdown", """{ "mm_id": "Q1234", "mm_strand": "Geometry" }""")
    assert[Double](transformedADTNextQuestionDF, "fanq_standard_error", 0.5)
    assert[String](transformedADTNextQuestionDF, "fanq_language", "EN_GB")
    assert[String](transformedADTNextQuestionDF, "fanq_class_subject_name", null)

  }

  test("when class subject name is present transform adt next question fact successfully") {
    val msg = makeMsg(fanqId,
      s"""
         |,
         |"response": true,
         |"breakdown": "{ \\"mm_id\\": \\"Q1234\\", \\"mm_strand\\": \\"Geometry\\" }",
         |"occurredOn": "2019-01-15 09:02:56.768",
         |"language": "EN_GB",
         |"standardError": 0.5,
         |"classSubjectName": "test-subject"
      """.stripMargin)

    val sprk = spark
    import sprk.implicits._

    val ADTQuestionTransformer = new ADTNextQuestionTransform(sprk, service)
    val ADTNextQuestionDF = spark.read.json(Seq(msg).toDS())
    when(service.readOptional(ADTNextQuestionSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ADTNextQuestionDF))
    val transformedADTNextQuestionDF = ADTQuestionTransformer.transform().get.output
    assert(transformedADTNextQuestionDF.columns.toSet === expectedTransformedCols)

    assert[Boolean](transformedADTNextQuestionDF, "fanq_response", true)
    assert[Double](transformedADTNextQuestionDF, "fanq_standard_error", 0.5)
    assert[String](transformedADTNextQuestionDF, "fanq_language", "EN_GB")
    assert[Boolean](transformedADTNextQuestionDF, "fanq_response", true)
    assert[String](transformedADTNextQuestionDF, "fanq_breakdown", """{ "mm_id": "Q1234", "mm_strand": "Geometry" }""")
    assert[Double](transformedADTNextQuestionDF, "fanq_standard_error", 0.5)
    assert[String](transformedADTNextQuestionDF, "fanq_language", "EN_GB")
    assert[String](transformedADTNextQuestionDF, "fanq_class_subject_name", "test-subject")
    assert[String](transformedADTNextQuestionDF, "fanq_skill", "listening")

  }

  test("transform in adt next question fact successfully to response as nullable boolean " +
    " and without language and standardError and empty breakdown") {

    val msg = makeMsg(fanqId,
      s"""
         |,
         |"response": null,
         |"breakdown": {},
         |"occurredOn": "2019-01-15 09:02:56.768"
      """.stripMargin)

    val sprk = spark
    import sprk.implicits._

    val ADTQuestionTransformer = new ADTNextQuestionTransform(sprk, service)
    val ADTNextQuestionDF = spark.read.json(Seq(msg).toDS())
    when(service.readOptional(ADTNextQuestionSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ADTNextQuestionDF))
    val transformedADTNextQuestionDF = ADTQuestionTransformer.transform().get.output

    assert[String](transformedADTNextQuestionDF, "fanq_response", null)
    assert[String](transformedADTNextQuestionDF, "fanq_breakdown", """{}""")

  }

  test("transform in adt next question with breakdown as a struct") {
    val msg = makeMsg(fanqId,
      s"""
         |,
         |"response": null,
         |"breakdown": {
         |      "mm_id": "Q1234",
         |      "mm_strand": "Geometry"
         |},
         |"occurredOn": "2019-01-15 09:02:56.768",
         |"language": "EN_GB",
         |"standardError": 0.5
      """.stripMargin)

    val sprk = spark
    import sprk.implicits._

    val ADTQuestionTransformer = new ADTNextQuestionTransform(sprk, service)
    val ADTNextQuestionDF = spark.read.json(Seq(msg).toDS())
    when(service.readOptional(ADTNextQuestionSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ADTNextQuestionDF))
    val transformedADTNextQuestionDF = ADTQuestionTransformer.transform().get.output

    assert[Double](transformedADTNextQuestionDF, "fanq_standard_error", 0.5)
    assert[String](transformedADTNextQuestionDF, "fanq_language", "EN_GB")
    assert[Double](transformedADTNextQuestionDF, "fanq_standard_error", 0.5)
    assert[String](transformedADTNextQuestionDF, "fanq_language", "EN_GB")
    assert[Int](transformedADTNextQuestionDF, "fanq_grade", 6)
    assert[String](transformedADTNextQuestionDF, "fanq_grade_id", "grade-id-1")
    assert[Int](transformedADTNextQuestionDF, "fanq_academic_year", 2021)
    assert[String](transformedADTNextQuestionDF, "fanq_academic_year_id", "ay-1")
    assert[Int](transformedADTNextQuestionDF, "fanq_academic_term", 1)
    assert[String](transformedADTNextQuestionDF, "fanq_breakdown", """{"mm_id":"Q1234","mm_strand":"Geometry"}""")
  }

  private def makeMsg(id: String, append: String): String =
    s"""
      |{
      |"eventType": "NextQuestion",
      |"tenantId":"tenant-id",
      |"id": "$id",
      |"learningSessionId":"99336173-0417-4810-9bb2-e01f020f8359",
      |"studentId":"6e8ad42f-45e8-4a6d-97d9-20af34935494",
      |"questionPoolId":"5e37b39d-0494-6600-015a-abe6abe6abe6",
      |"proficiency": 1.0,
      |"nextQuestion": "uuid2",
      |"timeSpent":20.0,
      |"currentQuestion": "64608e51-9d7c-4f83-8f89-df63991a60f1",
      |"intestProgress": 0.364,
      |"curriculumSubjectId": 423412,
      |"curriculumSubjectName": "English_MOE",
      |"grade": 6,
      |"gradeId": "grade-id-1",
      |"academicYear": 2021,
      |"academicYearId": "ay-1",
      |"academicTerm": 1,
      |"eventDateDw": 20190115,
      |"skill": "listening"
      |$append
      |}""".stripMargin

  def createDfFromJsonWithTimeCols(spark: SparkSession, json: String): DataFrame = {
    createDfFromJson(spark, json)
      .withColumn("fanq_created_time", col("fanq_created_time").cast(TimestampType))
  }
}
