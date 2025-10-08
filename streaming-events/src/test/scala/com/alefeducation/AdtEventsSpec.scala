package com.alefeducation

import spray.json._
import DefaultJsonProtocol._
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers


class AdtEventsSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: PracticeEventsTransformer = new PracticeEventsTransformer("practice-events", spark)
  }

  val sourceKey = "practice-source"
  val studentReportSink = "adt-student-report-sink"
  val studentReport = "StudentReport"
  val nextQuestionSink = "adt-next-question-sink"
  val nextQuestion = "NextQuestion"
  val attemptThresholdMutatedSink = "adt-attempt-threshold-mutated-sink"
  val attemptThresholdCreated = "AttemptThresholdCreated"

  val expCommonCols: Set[String] = Set(
    "eventType",
    "eventDateDw",
    "loadtime",
    "occurredOn",
    "tenantId",
    "id",
    "learningSessionId",
    "studentId",
    "questionPoolId",
    "breakdown",
    "curriculumSubjectId",
    "curriculumSubjectName",
    "language",
    "grade",
    "gradeId",
    "academicYearId",
    "academicYear",
    "academicTerm",
    "attempt",
    "classSubjectName",
    "skill"
  )

  val nextQuestionBreakdown: JsValue = Map(
   "external_id" -> "Q5367",
    "external_strand" -> "Algebra and Algebraic Thinking",
    "internal_domain" -> "Algebra and Algebraic Thinking"
  ).toJson

  val expNextQuestionCols: Set[String] = expCommonCols ++ Set(
    "response",
    "proficiency",
    "nextQuestion",
    "timeSpent",
    "intestProgress",
    "currentQuestion",
    "standardError",
  )

  val nextQuestionCommonCols: String =
    s"""
      |"response": null,
      |"proficiency": 0.3,
      |"standardError": 1.0,
      |"nextQuestion": "uuid2",
      |"currentQuestion": "current question",
      |"intestProgress": 0.012272727272950415,
      |"timeSpent": 0,
      |"attempt": 1,
      |"classSubjectName": "math",
      |"breakdown": $nextQuestionBreakdown
      |""".stripMargin

  val expStudentReportCols: Set[String] = expCommonCols ++ Set(
    "finalCategory",
    "finalProficiency",
    "finalStandardError",
    "finalScore",
    "finalResult",
    "totalTimespent",
    "testId",
    "finalUncertainty",
    "framework",
    "finalStandardError",
    "schoolId",
    "finalGrade",
    "secondaryResult",
    "forecastScore"
  )

  val studentReportCommonCols: String =
    """
      |"finalProficiency": 1.0,
      |"finalScore": 20.0,
      |"finalResult": "40",
      |"finalUncertainty": 30.0,
      |"finalLevel": "BELOW",
      |"framework":"Alef",
      |"totalTimespent":20,
      |"academicYear": 2020,
      |"academicTerm": 1,
      |"testId": "testId",
      |"attempt":1,
      |"classSubjectName": "math",
      |"finalGrade":2
      |""".stripMargin

  val studentReportTemplate: String = makeMsg(
    studentReport,
    s"""
       |"finalProficiency": 1.0,
       |"finalUncertainty": 37,
       |"finalCategory": "APPROACHING",
       |"finalGrade": 4,
       |"finalStandardError": 1.0,
       |"finalScore": 484,
       |"finalResult": "484",
       |"totalTimespent": 300,
       |"academicYear": 2023,
       |"academicTerm": 1,
       |"testId": "testId",
       |"language": "EN_GB",
       |"framework": "alef",
       |"schoolId": "schoolId",
       |"attempt": 1,
       |"grade": 5,
       |"gradeId": "grade_id",
       |"classSubjectName": "math",
       |"academicYearId": "academic_year_id",
       |"breakdown": {
       |  "score": {
       |    "final_score": 568,
       |    "final_result": "570Q",
       |    "final_uncertainty": 75,
       |    "framework": "quantile"
       |  },
       |  "domains": [
       |    {
       |      "final_score": 570,
       |      "final_result": "570",
       |      "final_uncertainty": 79,
       |      "name": "Geometry",
       |      "final_grade": 5,
       |      "final_category": "MEETS",
       |      "is_overall_result": false
       |    },
       |    {
       |      "final_score": 593,
       |      "final_result": "593",
       |      "final_uncertainty": 110,
       |      "name": "Algebra and Algebraic Thinking",
       |      "final_grade": 6,
       |      "final_category": "EXCEEDS",
       |      "is_overall_result": false
       |    },
       |    {
       |      "final_score": 583,
       |      "final_result": "583",
       |      "final_uncertainty": 72,
       |      "name": "Measurement, Data and Statistics",
       |      "final_grade": 6,
       |      "final_category": "EXCEEDS",
       |      "is_overall_result": false
       |    },
       |    {
       |      "final_score": 373,
       |      "final_result": "373",
       |      "final_uncertainty": 57,
       |      "name": "Numbers and Operations",
       |      "final_grade": 2,
       |      "final_category": "BELOW",
       |      "is_overall_result": false
       |    }
       |  ]
       |},
       |"secondaryResult": null,
       |"forecastScore": ""
       |""".stripMargin
  )

  test("handle ADT next question events") {

    val msg = makeMsg(
      nextQuestion,
      s"""
         |$nextQuestionCommonCols,
         |"language": "EN_GB",
         |"curriculumSubjectId": 42,
         |"curriculumSubjectName": "curr subject name"
         |""".stripMargin
    )

    val fixtures = List(
      SparkFixture(
        key = sourceKey,
        value = msg
      )
    )

    executeTest(
      fixtures,
      nextQuestionSink,
      nextQuestion,
      expNextQuestionCols, { fst =>

        fst.getAs[String]("nextQuestion") shouldBe "uuid2"
        fst.getAs[String]("response") shouldBe null
        fst.getAs[Double]("timeSpent") shouldBe 0.0
        fst.getAs[Double]("proficiency") shouldBe 0.3
        fst.getAs[String]("currentQuestion") shouldBe "current question"
        fst.getAs[Double]("standardError") shouldBe 1.0
        fst.getAs[Double]("intestProgress") shouldBe 0.012272727272950415
        fst.getAs[String]("language") shouldBe "EN_GB"
        fst.getAs[Int]("curriculumSubjectId") shouldBe 42
        fst.getAs[String]("curriculumSubjectName") shouldBe "curr subject name"
        fst.getAs[String]("classSubjectName") shouldBe "math"
        val breakdown = fst.getAs[String]("breakdown").parseJson
        breakdown shouldBe nextQuestionBreakdown
      }
    )
  }

  test("handle ADT student report events") {
    val fixtures = List(
      SparkFixture(
        key = sourceKey,
        value = studentReportTemplate
      )
    )

    executeTest(
      fixtures,
      studentReportSink,
      studentReport,
      expStudentReportCols, { fst =>
        fst.getAs[Double]("finalProficiency") shouldBe 1.0
        fst.getAs[Double]("finalStandardError") shouldBe 1.0
        fst.getAs[Double]("finalScore") shouldBe 484
        fst.getAs[String]("finalResult") shouldBe "484"
        fst.getAs[Int]("totalTimespent") shouldBe 300
        fst.getAs[Int]("academicYear") shouldBe 2023
        fst.getAs[Int]("academicTerm") shouldBe 1
        fst.getAs[String]("testId") shouldBe "testId"
        fst.getAs[Double]("finalUncertainty") shouldBe 37.0
        fst.getAs[String]("framework") shouldBe "alef"
        fst.getAs[String]("language") shouldBe "EN_GB"
        fst.getAs[String]("schoolId") shouldBe "schoolId"
        fst.getAs[String]("classSubjectName") shouldBe "math"
      }
    )
  }

  test("handle ADT student report without language, schoolId and finalStandardError") {

    val msg = makeMsg(
      studentReport,
    s"""
      |$studentReportCommonCols
      """.stripMargin
    )

    val fixtures = List(
      SparkFixture(
        key = sourceKey,
        value = msg
      )
    )

    executeTest(
      fixtures,
      studentReportSink,
      studentReport,
      expStudentReportCols, { fst =>
        fst.getAs[String]("finalStandardError") shouldBe null
        fst.getAs[String]("language") shouldBe null
        fst.getAs[String]("schoolId") shouldBe null
        fst.getAs[Double]("academicTerm") shouldBe 1
      }
    )
  }

  test("handle ADT next question without language and standardError") {
    val msg = makeMsg(
      "NextQuestion",
      s"""
        |$nextQuestionCommonCols,
        |"standardError": 0.0
        |""".stripMargin
    )

    val fixtures = List(
      SparkFixture(
        key = sourceKey,
        value = msg
      )
    )

    executeTest(
      fixtures,
      nextQuestionSink,
      nextQuestion,
      expNextQuestionCols, { fst =>
        fst.getAs[Double]("standardError") shouldBe 0.0
        fst.getAs[String]("language") shouldBe null
        fst.getAs[Double]("proficiency") shouldBe 0.3
        fst.getAs[String]("classSubjectName") shouldBe "math"
      }
    )
  }

  test("handle AttemptThresholdCreatedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "id",
        "academicYearId",
        "schoolId",
        "schoolName",
        "status",
        "numberOfAttempts",
        "attempts",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = sourceKey,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"AttemptThresholdCreated",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |    "id": "65dd8751d130a58ae9582819",
                    |    "academicYearId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
                    |    "schoolId": "c7378da9-76fb-432c-8181-1eb970b3982f",
                    |    "schoolName": "Targaryen of King's Landing",
                    |    "attempts": [
                    |      {
                    |        "attemptNumber": 1,
                    |        "startTime" : 1724965200,
                    |        "endTime" : 1732913999,
                    |        "attemptTitle": "Test 1"
                    |      },
                    |      {
                    |        "attemptNumber": 2,
                    |        "startTime" : 1724965200,
                    |        "endTime" : 1732913999,
                    |        "attemptTitle": "Test 2"
                    |      }
                    |    ],
                    |    "status": "ACTIVE",
                    |    "numberOfAttempts": 2,
                    |    "occurredOn": 100500092323
                    |   }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == attemptThresholdMutatedSink).map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "AttemptThresholdCreated"
            fst.getAs[String]("id") shouldBe "65dd8751d130a58ae9582819"
            fst.getAs[String]("academicYearId") shouldBe "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f"
            fst.getAs[String]("schoolName") shouldBe "Targaryen of King's Landing"
            fst.getAs[String]("schoolId") shouldBe "c7378da9-76fb-432c-8181-1eb970b3982f"
          }
        }
      )
    }
  }

  test("handle AttemptThresholdUpdatedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "id",
        "academicYearId",
        "schoolId",
        "schoolName",
        "status",
        "numberOfAttempts",
        "attempts",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = sourceKey,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"AttemptThresholdUpdated",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |    "id": "65dd8751d130a58ae9582819",
                    |    "academicYearId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
                    |    "schoolId": "c7378da9-76fb-432c-8181-1eb970b3982f",
                    |    "schoolName": "Targaryen of King's Landing",
                    |    "attempts": [
                    |      {
                    |        "attemptNumber": 1,
                    |        "startTime" : 1724965200,
                    |        "endTime" : 1732913999,
                    |        "attemptTitle": "Test 1"
                    |      },
                    |      {
                    |        "attemptNumber": 2,
                    |        "startTime" : 1724965200,
                    |        "endTime" : 1732913999,
                    |        "attemptTitle": "Test 2"
                    |      }
                    |    ],
                    |    "status": "ACTIVE",
                    |    "numberOfAttempts": 2,
                    |    "occurredOn": 100500092323
                    |   }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == attemptThresholdMutatedSink).map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "AttemptThresholdUpdated"
            fst.getAs[String]("id") shouldBe "65dd8751d130a58ae9582819"
            fst.getAs[String]("academicYearId") shouldBe "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f"
            fst.getAs[String]("schoolName") shouldBe "Targaryen of King's Landing"
            fst.getAs[String]("schoolId") shouldBe "c7378da9-76fb-432c-8181-1eb970b3982f"
          }
        }
      )
    }
  }

  def makeMsg(eventType: String, append: String): String = {
    s"""
       |{
       | "key": "key1",
       | "value":{
       |    "headers":{
       |       "eventType": "$eventType",
       |       "tenantId":"tenantId"
       |    },
       |    "body":{
       |       "occurredOn": 1709016913963,
       |       "id": "some-generated-event-uuid",
       |       "learningSessionId":"99336173-0417-4810-9bb2-e01f020f8359",
       |       "studentId":"6e8ad42f-45e8-4a6d-97d9-20af34935494",
       |       "questionPoolId":"5e37b39d04946600015aabe6",
       |       "curriculumSubjectId": 42,
       |       "curriculumSubjectName": "curr subject name",
       |       $append
       |     }
       | },
       | "timestamp": "2020-02-20 16:23:46.609"
       |}""".stripMargin
  }

  def executeTest(fixtures: List[SparkFixture], name: String, evenType: String, expCols: Set[String], f: Row => Unit): Unit = {
    new Setup {
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == name).map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expCols)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe evenType
            fst.getAs[String]("eventDateDw") shouldBe "20240227"
            fst.getAs[String]("occurredOn") shouldBe "2024-02-27 06:55:13.963"
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("id") shouldBe "some-generated-event-uuid"
            fst.getAs[String]("learningSessionId") shouldBe "99336173-0417-4810-9bb2-e01f020f8359"
            fst.getAs[String]("studentId") shouldBe "6e8ad42f-45e8-4a6d-97d9-20af34935494"
            fst.getAs[String]("questionPoolId") shouldBe "5e37b39d04946600015aabe6"
            fst.getAs[Long]("curriculumSubjectId") shouldBe 42
            fst.getAs[String]("curriculumSubjectName") shouldBe "curr subject name"

            f(fst)
          }
        }
      )
    }
  }
}
