package com.alefeducation.facts

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.util.ktgame.KTGameHelper._

class KTGameSessionSpec extends SparkSuite with BaseDimensionSpec {

  trait Setup {
    implicit val transformer = new KTGameSessionTransformer(ktGameSessionService, spark)
  }

  def testData(sinks: List[Sink]) = {
    val startedDf = sinks.head.output
    assert(startedDf.columns.toSet === expectedColumns)
    assert[String](startedDf, "ktg_session_id", "game-session-id")
    assert[String](startedDf, "ktg_session_created_time", "2019-06-25 12:45:46.879")
    assert[String](startedDf, "ktg_session_question_id", null)
    assert[String](startedDf, "ktg_session_kt_id", null)
    assert[String](startedDf, "tenant_uuid", "tenantId")
    assert[String](startedDf, "student_uuid", "student-1")
    assert[String](startedDf, "lo_uuid", null)
    assert[Boolean](startedDf, "ktg_session_outside_of_school", true)
    assert[String](startedDf, "ktg_session_trimester_id", "trimester-1")
    assert[Int](startedDf, "ktg_session_trimester_order", 3)
    assert[String](startedDf, "ktg_session_type", "ROCKET")
    assert[BigDecimal](startedDf, "ktg_session_question_time_allotted", -1.0)
    assert[String](startedDf, "ktg_session_answer", null)
    assert[Int](startedDf, "ktg_session_num_attempts", -1)
    assert[Double](startedDf, "ktg_session_score", -1)
    assert[String](startedDf, "ktg_session_max_score", "-1")
    assert[Int](startedDf, "ktg_session_stars", -1)
    assert[Boolean](startedDf, "ktg_session_is_attended", false)
    assert[Int](startedDf, "ktg_session_event_type", 1)
    assert[Boolean](startedDf, "ktg_session_is_start", true)
    assert[Boolean](startedDf, "ktg_session_is_start_event_processed", false)
    assert[String](startedDf, "academic_year_uuid", "763578c8-a546-4c74-b660-5df866a337c9")
    assert[String](startedDf, "ktg_session_instructional_plan_id", "ip1")
    assert[String](startedDf, "ktg_session_learning_path_id", "lp1")
    assert[String](startedDf, "subject_uuid", "subject-1")
    assert[String](startedDf, "section_uuid", "section-1")
    assert[String](startedDf, "class_uuid", "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312")
    assert[String](startedDf, "ktg_session_material_id", "ktd1b4e4-9eb5-4eff-92a4-94139f892300")
    assert[String](startedDf, "ktg_session_material_type", "CORE")

    val questionStartedDf = sinks(1).output
    assert[String](questionStartedDf, "ktg_session_id", "game-session-id")
    assert[String](questionStartedDf, "ktg_session_created_time", "2019-06-25 12:45:46.879")
    assert[String](questionStartedDf, "ktg_session_question_id", "question-1")
    assert[String](questionStartedDf, "ktg_session_kt_id", "kt-1")
    assert[String](questionStartedDf, "tenant_uuid", "tenantId")
    assert[String](questionStartedDf, "student_uuid", "student-1")
    assert[String](questionStartedDf, "lo_uuid", "lo-1")
    assert[Boolean](questionStartedDf, "ktg_session_outside_of_school", true)
    assert[BigDecimal](questionStartedDf, "ktg_session_question_time_allotted", 8.0)
    assert[String](questionStartedDf, "ktg_session_answer", null)
    assert[Int](questionStartedDf, "ktg_session_num_attempts", -1)
    assert[Double](questionStartedDf, "ktg_session_score", -1)
    assert[String](questionStartedDf, "ktg_session_max_score", "-1")
    assert[Int](questionStartedDf, "ktg_session_stars", -1)
    assert[Int](questionStartedDf, "ktg_session_event_type", 2)
    assert[Boolean](questionStartedDf, "ktg_session_is_start", true)
    assert[String](questionStartedDf, "academic_year_uuid", "763578c8-a546-4c74-b660-5df866a337c9")
    assert[String](questionStartedDf, "ktg_session_instructional_plan_id", "ip1")
    assert[String](questionStartedDf, "ktg_session_learning_path_id", "lp1")
    assert[String](questionStartedDf, "subject_uuid", "subject-1")
    assert[String](questionStartedDf, "section_uuid", "section-1")
    assert[String](questionStartedDf, "class_uuid", "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312")
    assert[String](questionStartedDf, "ktg_session_material_id", "ktd1b4e4-9eb5-4eff-92a4-94139f892300")
    assert[String](questionStartedDf, "ktg_session_material_type", "CORE")

    val questionFinishedDf = sinks(2).output
    assert[String](questionFinishedDf, "ktg_session_id", "game-session-id")
    assert[String](questionFinishedDf, "ktg_session_created_time", "2019-06-25 12:45:55.037")
    assert[String](questionFinishedDf, "ktg_session_question_id", "question-1")
    assert[String](questionFinishedDf, "ktg_session_kt_id", "kt-1")
    assert[String](questionFinishedDf, "lo_uuid", "lo-1")
    assert[BigDecimal](questionFinishedDf, "ktg_session_question_time_allotted", 8.0)
    assert[String](questionFinishedDf, "ktg_session_answer", "Value doesn't change, Value changes")
    assert[Int](questionFinishedDf, "ktg_session_num_attempts", 2)
    assert[Double](questionFinishedDf, "ktg_session_score", 1)
    assert[String](questionFinishedDf, "ktg_session_max_score", "1")
    assert[Int](questionFinishedDf, "ktg_session_stars", -1)
    assert[Boolean](questionFinishedDf, "ktg_session_is_attended", true)
    assert[Int](questionFinishedDf, "ktg_session_event_type", 2)
    assert[Boolean](questionFinishedDf, "ktg_session_is_start", false)
    assert[Boolean](questionFinishedDf, "ktg_session_is_start_event_processed", false)
    assert[String](questionFinishedDf, "academic_year_uuid", "763578c8-a546-4c74-b660-5df866a337c9")
    assert[String](questionFinishedDf, "ktg_session_instructional_plan_id", "ip1")
    assert[String](questionFinishedDf, "ktg_session_learning_path_id", "lp1")
    assert[String](questionFinishedDf, "subject_uuid", "subject-1")
    assert[String](questionFinishedDf, "section_uuid", "section-1")
    assert[String](questionFinishedDf, "class_uuid", "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312")
    assert[String](questionFinishedDf, "ktg_session_material_id", "ktd1b4e4-9eb5-4eff-92a4-94139f892300")
    assert[String](questionFinishedDf, "ktg_session_material_type", "CORE")

    val gameFinishedDf = sinks(3).output
    assert(gameFinishedDf.columns.toSet === expectedColumns)
    assert[String](gameFinishedDf, "ktg_session_id", "game-session-id")
    assert[String](gameFinishedDf, "ktg_session_created_time", "2019-06-25 12:46:18.608")
    assert[String](gameFinishedDf, "ktg_session_question_id", null)
    assert[String](gameFinishedDf, "ktg_session_kt_id", null)
    assert[String](gameFinishedDf, "tenant_uuid", "tenantId")
    assert[String](gameFinishedDf, "student_uuid", "student-1")
    assert[String](gameFinishedDf, "lo_uuid", null)
    assert[Boolean](gameFinishedDf, "ktg_session_outside_of_school", true)
    assert[String](gameFinishedDf, "ktg_session_trimester_id", "trimester-1")
    assert[Int](gameFinishedDf, "ktg_session_trimester_order", 3)
    assert[String](gameFinishedDf, "ktg_session_type", "ROCKET")
    assert[BigDecimal](gameFinishedDf, "ktg_session_question_time_allotted", -1.0)
    assert[String](gameFinishedDf, "ktg_session_answer", null)
    assert[Int](gameFinishedDf, "ktg_session_num_attempts", -1)
    assert[Double](gameFinishedDf, "ktg_session_score", 70)
    assert[String](gameFinishedDf, "ktg_session_max_score", "-1")
    assert[Int](gameFinishedDf, "ktg_session_stars", 5)
    assert[Boolean](gameFinishedDf, "ktg_session_is_attended", false)
    assert[Int](gameFinishedDf, "ktg_session_event_type", 1)
    assert[Boolean](gameFinishedDf, "ktg_session_is_start", false)
    assert[Boolean](gameFinishedDf, "ktg_session_is_start_event_processed", false)
    assert[String](gameFinishedDf, "academic_year_uuid", "763578c8-a546-4c74-b660-5df866a337c9")
    assert[String](gameFinishedDf, "ktg_session_instructional_plan_id", "ip1")
    assert[String](gameFinishedDf, "ktg_session_learning_path_id", "lp1")
    assert[String](gameFinishedDf, "subject_uuid", "subject-1")
    assert[String](gameFinishedDf, "section_uuid", "section-1")
    assert[String](gameFinishedDf, "class_uuid", "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312")
    assert[String](gameFinishedDf, "ktg_session_material_id", "ktd1b4e4-9eb5-4eff-92a4-94139f892300")
    assert[String](gameFinishedDf, "ktg_session_material_type", "CORE")

  }

  val expectedColumns = Set(
    "ktg_session_id",
    "ktg_session_created_time",
    "ktg_session_dw_created_time",
    "ktg_session_date_dw_id",
    "ktg_session_question_id",
    "ktg_session_kt_id",
    "tenant_uuid",
    "student_uuid",
    "subject_uuid",
    "school_uuid",
    "grade_uuid",
    "section_uuid",
    "lo_uuid",
    "ktg_session_outside_of_school",
    "ktg_session_trimester_id",
    "ktg_session_trimester_order",
    "ktg_session_type",
    "ktg_session_question_time_allotted",
    "ktg_session_answer",
    "ktg_session_num_attempts",
    "ktg_session_score",
    "ktg_session_max_score",
    "ktg_session_stars",
    "ktg_session_is_attended",
    "ktg_session_event_type",
    "ktg_session_is_start",
    "ktg_session_is_start_event_processed",
    "academic_year_uuid",
    "ktg_session_instructional_plan_id",
    "ktg_session_learning_path_id",
    "class_uuid",
    "ktg_session_material_id",
    "ktg_session_material_type",
  )

  test("transform practice created events successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetKTGameStartedSource,
          value = """
              |{
              |  "tenantId": "tenantId",
              |  "eventType": "GameSessionStartedEvent",
              |  "occurredOn": "2019-06-25 12:45:46.879",
              |  "created": "2019-06-25T12:45:20.66",
              |  "updated": "2019-06-25T12:45:46.879",
              |  "gameSessionId": "game-session-id",
              |  "learnerId": "student-1",
              |  "subjectId": "subject-1",
              |  "subjectName": "Math",
              |  "subjectCode": "MATH",
              |  "trimesterId": "trimester-1",
              |  "trimesterOrder": 3,
              |  "schoolId": "school-1",
              |  "gradeId": "grade-1",
              |  "grade": 6,
              |  "sectionId": "section-1",
              |  "status": "IN_PROGRESS",
              |  "gameType": "ROCKET",
              |  "outsideOfSchool": true,
              |  "loadtime": "2019-06-25T12:45:46.959Z",
              |  "eventDateDw": "20190625",
              |  "academicYearId": "763578c8-a546-4c74-b660-5df866a337c9",
              |  "instructionalPlanId":"ip1",
              |  "learningPathId":"lp1",
              |  "classId": "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312",
              |  "materialId": "ktd1b4e4-9eb5-4eff-92a4-94139f892300",
              |  "materialType": "CORE"
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = ParquetKTGameFinishedSource,
          value = """
              |{
              |  "tenantId": "tenantId",
              |  "eventType": "GameSessionFinishedEvent",
              |  "occurredOn": "2019-06-25 12:46:18.608",
              |  "created": "2019-06-25T12:45:20.66",
              |  "updated": "2019-06-25T12:46:17.516",
              |  "gameSessionId": "game-session-id",
              |  "learnerId": "student-1",
              |  "subjectId": "subject-1",
              |  "subjectName": "Math",
              |  "subjectCode": "MATH",
              |  "trimesterId": "trimester-1",
              |  "trimesterOrder": 3,
              |  "schoolId": "school-1",
              |  "gradeId": "grade-1",
              |  "grade": 6,
              |  "sectionId": "section-1",
              |  "status": "IN_PROGRESS",
              |  "gameType": "ROCKET",
              |  "outsideOfSchool": true,
              |  "score": 70,
              |  "stars": 5,
              |  "loadtime": "2019-06-25T12:46:18.681Z",
              |  "eventDateDw": "20190625",
              |  "academicYearId": "763578c8-a546-4c74-b660-5df866a337c9",
              |  "instructionalPlanId":"ip1",
              |  "learningPathId":"lp1",
              |  "classId": "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312",
              |  "materialId": "ktd1b4e4-9eb5-4eff-92a4-94139f892300",
              |  "materialType": "CORE"
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = ParquetKTGameQuestionStartedSource,
          value = """
              |{
              |  "tenantId": "tenantId",
              |  "eventType": "GameQuestionSessionStartedEvent",
              |  "occurredOn": "2019-06-25 12:45:46.879",
              |  "questionId": "question-1",
              |  "language": "ENGLISH",
              |  "keyTermId": "kt-1",
              |  "questionType": "MULTIPLE_CHOICE",
              |  "time": 8,
              |  "learningObjectiveId": "lo-1",
              |  "status": "OPEN",
              |  "gameSessionId": "game-session-id",
              |  "learnerId": "student-1",
              |  "subjectId": "subject-1",
              |  "subjectName": "Math",
              |  "subjectCode": "MATH",
              |  "trimesterId": "trimester-1",
              |  "trimesterOrder": 3,
              |  "schoolId": "school-1",
              |  "gradeId": "grade-1",
              |  "grade": 6,
              |  "sectionId": "section-1",
              |  "gameType": "ROCKET",
              |  "outsideOfSchool": true,
              |  "loadtime": "2019-06-25T12:45:46.957Z",
              |  "eventDateDw": "20190625",
              |  "academicYearId": "763578c8-a546-4c74-b660-5df866a337c9",
              |  "instructionalPlanId":"ip1",
              |  "learningPathId":"lp1",
              |  "classId": "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312",
              |  "materialId": "ktd1b4e4-9eb5-4eff-92a4-94139f892300",
              |  "materialType": "CORE"
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = ParquetKTGameQuestionFinishedSource,
          value = """
              |{
              |  "tenantId": "tenantId",
              |  "eventType": "GameQuestionSessionFinishedEvent",
              |  "occurredOn": "2019-06-25 12:45:55.037",
              |  "questionId": "question-1",
              |  "language": "ENGLISH",
              |  "keyTermId": "kt-1",
              |  "questionType": "MULTIPLE_CHOICE",
              |  "time": 8,
              |  "learningObjectiveId": "lo-1",
              |  "status": "COMPLETED",
              |  "gameSessionId": "game-session-id",
              |  "learnerId": "student-1",
              |  "subjectId": "subject-1",
              |  "subjectName": "Math",
              |  "subjectCode": "MATH",
              |  "trimesterId": "trimester-1",
              |  "trimesterOrder": 3,
              |  "schoolId": "school-1",
              |  "gradeId": "grade-1",
              |  "grade": 6,
              |  "sectionId": "section-1",
              |  "gameType": "ROCKET",
              |  "outsideOfSchool": true,
              |  "scoreBreakdown": {
              |    "isAttended": true,
              |    "maxScore": "1",
              |    "score": 1,
              |    "answer": null,
              |    "answers": ["Value doesn't change","Value changes"],
              |    "timeSpent": 7
              |  },
              |  "loadtime": "2019-06-25T12:45:55.116Z",
              |  "eventDateDw": "20190625",
              |  "academicYearId": "763578c8-a546-4c74-b660-5df866a337c9",
              |  "instructionalPlanId":"ip1",
              |  "learningPathId":"lp1",
              |  "classId": "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312",
              |  "materialId": "ktd1b4e4-9eb5-4eff-92a4-94139f892300",
              |  "materialType": "CORE"
              |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshiftSinks = sinks.filter(_.name == RedshiftKTGameSessionSink)
          assert(redshiftSinks.size == 4)
          testData(redshiftSinks)

        }
      )

    }
  }

  test("Test delta sinks") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTtGameDeltaStagingSource,
          value =
            """
              |[
              |{"ktg_session_id":"game-session-id-0","ktg_session_date_dw_id":"20190625","ktg_session_question_id":"null","ktg_session_kt_id":"null","ktg_session_tenant_id":"tenantId","ktg_session_student_id":"student-1","ktg_session_subject_id":"subject-1","ktg_session_school_id":"school-1","ktg_session_grade_id":"grade-1","ktg_session_section_id":"section-1","ktg_session_lo_id":"null","ktg_session_outside_of_school":true,"ktg_session_trimester_id":"trimester-1","ktg_session_trimester_order":3,"ktg_session_type":"ROCKET","ktg_session_question_time_allotted":-1.0,"ktg_session_answer":"null","ktg_session_num_attempts":-1,"ktg_session_score":-1,"ktg_session_max_score":"-1","ktg_session_stars":-1,"ktg_session_is_attended":false,"ktg_session_event_type":1,"ktg_session_is_start":true,"ktg_session_academic_year_id":"763578c8-a546-4c74-b660-5df866a337c9","ktg_session_instructional_plan_id":"ip1","ktg_session_learning_path_id":"lp1","ktg_session_class_id":"b4d1b4e4-9eb5-4eff-92a4-94139f4f2312","ktg_session_dw_created_time":"2020-09-16T10:15:47.620Z","ktg_session_created_time":"2019-06-25T12:45:46.879Z","ktg_session_is_start_event_processed":false, "ktg_session_material_id": "ktd1b4e4-9eb5-4eff-92a4-94139f892300", "ktg_session_material_type": "CORE"},
              |{"ktg_session_id":"game-session-id-1","ktg_session_date_dw_id":"20190625","ktg_session_question_id":"null","ktg_session_kt_id":"null","ktg_session_tenant_id":"tenantId","ktg_session_student_id":"student-1","ktg_session_subject_id":"subject-1","ktg_session_school_id":"school-1","ktg_session_grade_id":"grade-1","ktg_session_section_id":"section-1","ktg_session_lo_id":"null","ktg_session_outside_of_school":true,"ktg_session_trimester_id":"trimester-1","ktg_session_trimester_order":3,"ktg_session_type":"ROCKET","ktg_session_question_time_allotted":-1.0,"ktg_session_answer":"null","ktg_session_num_attempts":-1,"ktg_session_score":-1,"ktg_session_max_score":"-1","ktg_session_stars":-1,"ktg_session_is_attended":false,"ktg_session_event_type":1,"ktg_session_is_start":true,"ktg_session_academic_year_id":"763578c8-a546-4c74-b660-5df866a337c9","ktg_session_instructional_plan_id":"ip1","ktg_session_learning_path_id":"lp1","ktg_session_class_id":"b4d1b4e4-9eb5-4eff-92a4-94139f4f2312","ktg_session_dw_created_time":"2020-09-16T10:15:47.620Z","ktg_session_created_time":"2019-06-25T12:45:46.979Z","ktg_session_is_start_event_processed":true, "ktg_session_material_id": "ktd1b4e4-9eb5-4eff-92a4-94139f892300", "ktg_session_material_type": "CORE"}
              |]
              |""".stripMargin
        ),
        SparkFixture(
          key = ParquetTtGameQuestionDeltaStagingSource,
          value = """
              |[
              |{"ktg_session_id":"game-session-id","ktg_session_question_id":"question-1","ktg_session_date_dw_id":"20190625","ktg_session_kt_id":"null","ktg_session_tenant_id":"tenantId","ktg_session_student_id":"student-1","ktg_session_subject_id":"subject-1","ktg_session_school_id":"school-1","ktg_session_grade_id":"grade-1","ktg_session_section_id":"section-1","ktg_session_lo_id":"null","ktg_session_outside_of_school":true,"ktg_session_trimester_id":"trimester-1","ktg_session_trimester_order":3,"ktg_session_type":"ROCKET","ktg_session_question_time_allotted":-1.0,"ktg_session_answer":"null","ktg_session_num_attempts":-1,"ktg_session_score":-1,"ktg_session_max_score":"-1","ktg_session_stars":-1,"ktg_session_is_attended":false,"ktg_session_event_type":2,"ktg_session_is_start":true,"ktg_session_academic_year_id":"763578c8-a546-4c74-b660-5df866a337c9","ktg_session_instructional_plan_id":"ip1","ktg_session_learning_path_id":"lp1","ktg_session_class_id":"b4d1b4e4-9eb5-4eff-92a4-94139f4f2312","ktg_session_dw_created_time":"2020-09-16T10:15:47.620Z","ktg_session_created_time":"2019-06-25T12:45:46.979Z","ktg_session_is_start_event_processed":false, "ktg_session_material_id": "ktd1b4e4-9eb5-4eff-92a4-94139f892300", "ktg_session_material_type": "CORE"},
              |{"ktg_session_id":"game-session-id-1","ktg_session_question_id":"question-11","ktg_session_date_dw_id":"20190625","ktg_session_kt_id":"null","ktg_session_tenant_id":"tenantId","ktg_session_student_id":"student-1","ktg_session_subject_id":"subject-1","ktg_session_school_id":"school-1","ktg_session_grade_id":"grade-1","ktg_session_section_id":"section-1","ktg_session_lo_id":"null","ktg_session_outside_of_school":true,"ktg_session_trimester_id":"trimester-1","ktg_session_trimester_order":3,"ktg_session_type":"ROCKET","ktg_session_question_time_allotted":-1.0,"ktg_session_answer":"null","ktg_session_num_attempts":-1,"ktg_session_score":-1,"ktg_session_max_score":"-1","ktg_session_stars":-1,"ktg_session_is_attended":false,"ktg_session_event_type":2,"ktg_session_is_start":true,"ktg_session_academic_year_id":"763578c8-a546-4c74-b660-5df866a337c9","ktg_session_instructional_plan_id":"ip1","ktg_session_learning_path_id":"lp1","ktg_session_class_id":"b4d1b4e4-9eb5-4eff-92a4-94139f4f2312","ktg_session_dw_created_time":"2020-09-16T10:15:47.620Z","ktg_session_created_time":"2019-06-25T12:45:46.979Z","ktg_session_is_start_event_processed":true, "ktg_session_material_id": "ktd1b4e4-9eb5-4eff-92a4-94139f892300", "ktg_session_material_type": "CORE"}
              |]
              |""".stripMargin
        ),
        SparkFixture(
          key = ParquetKTGameStartedSource,
          value =
            """
              |[
              |{"tenantId":"tenantId","eventType":"GameSessionStartedEvent","occurredOn":"2019-06-25 12:45:46.879","created":"2019-06-25T12:45:20.66","updated":"2019-06-25T12:45:46.879","gameSessionId":"game-session-id","learnerId":"student-1","subjectId":"subject-1","subjectName":"Math","subjectCode":"MATH","trimesterId":"trimester-1","trimesterOrder":3,"schoolId":"school-1","gradeId":"grade-1","grade":6,"sectionId":"section-1","status":"IN_PROGRESS","gameType":"ROCKET","outsideOfSchool":true,"loadtime":"2019-06-25T12:45:46.959Z","eventDateDw":"20190625","academicYearId":"763578c8-a546-4c74-b660-5df866a337c9","instructionalPlanId":"ip1","learningPathId":"lp1","classId":"b4d1b4e4-9eb5-4eff-92a4-94139f4f2312", "materialId": "ktd1b4e4-9eb5-4eff-92a4-94139f892300", "materialType": "CORE"},
              |{"tenantId":"tenantId","eventType":"GameSessionStartedEvent","occurredOn":"2019-06-25 12:45:46.879","created":"2019-06-25T12:45:20.66","updated":"2019-06-25T12:45:46.879","gameSessionId":"game-session-id2","learnerId":"student-1","subjectId":"subject-1","subjectName":"Math","subjectCode":"MATH","trimesterId":"trimester-1","trimesterOrder":3,"schoolId":"school-1","gradeId":"grade-1","grade":6,"sectionId":"section-1","status":"IN_PROGRESS","gameType":"ROCKET","outsideOfSchool":true,"loadtime":"2019-06-25T12:45:46.959Z","eventDateDw":"20190625","academicYearId":"763578c8-a546-4c74-b660-5df866a337c9","instructionalPlanId":"ip1","learningPathId":"lp1","classId":"b4d1b4e4-9eb5-4eff-92a4-94139f4f2312", "materialId": "ktd1b4e4-9eb5-4eff-92a4-94139f892300", "materialType": "CORE"}
              |]
              |""".stripMargin
        ),
        SparkFixture(
          key = ParquetKTGameFinishedSource,
          value =
            """
              |[
              |{"tenantId":"tenantId","eventType":"GameSessionFinishedEvent","occurredOn":"2019-06-25 12:46:18.608","created":"2019-06-25T12:45:20.66","updated":"2019-06-25T12:46:17.516","gameSessionId":"game-session-id","learnerId":"student-1","subjectId":"subject-1","subjectName":"Math","subjectCode":"MATH","trimesterId":"trimester-1","trimesterOrder":3,"schoolId":"school-1","gradeId":"grade-1","grade":6,"sectionId":"section-1","status":"IN_PROGRESS","gameType":"ROCKET","outsideOfSchool":true,"score":70,"stars":5,"loadtime":"2019-06-25T12:46:18.681Z","eventDateDw":"20190625","academicYearId":"763578c8-a546-4c74-b660-5df866a337c9","instructionalPlanId":"ip1","learningPathId":"lp1","classId":"b4d1b4e4-9eb5-4eff-92a4-94139f4f2312", "materialId": "ktd1b4e4-9eb5-4eff-92a4-94139f892300", "materialType": "CORE"},
              |{"tenantId":"tenantId","eventType":"GameSessionFinishedEvent","occurredOn":"2019-06-25 12:46:18.608","created":"2019-06-25T12:45:20.66","updated":"2019-06-25T12:46:17.516","gameSessionId":"game-session-id-0","learnerId":"student-1","subjectId":"subject-1","subjectName":"Math","subjectCode":"MATH","trimesterId":"trimester-1","trimesterOrder":3,"schoolId":"school-1","gradeId":"grade-1","grade":6,"sectionId":"section-1","status":"IN_PROGRESS","gameType":"ROCKET","outsideOfSchool":true,"score":70,"stars":5,"loadtime":"2019-06-25T12:46:18.681Z","eventDateDw":"20190625","academicYearId":"763578c8-a546-4c74-b660-5df866a337c9","instructionalPlanId":"ip1","learningPathId":"lp1","classId":"b4d1b4e4-9eb5-4eff-92a4-94139f4f2312", "materialId": "ktd1b4e4-9eb5-4eff-92a4-94139f892300", "materialType": "CORE"}
              |]
              |""".stripMargin
        ),
        SparkFixture(
          key = ParquetKTGameQuestionStartedSource,
          value =
            """
              |[
              |{"tenantId":"tenantId","eventType":"GameQuestionSessionStartedEvent","occurredOn":"2019-06-25 12:45:46.879","questionId":"question-1","language":"ENGLISH","keyTermId":"kt-1","questionType":"MULTIPLE_CHOICE","time":8,"learningObjectiveId":"lo-1","status":"OPEN","gameSessionId":"game-session-id","learnerId":"student-1","subjectId":"subject-1","subjectName":"Math","subjectCode":"MATH","trimesterId":"trimester-1","trimesterOrder":3,"schoolId":"school-1","gradeId":"grade-1","grade":6,"sectionId":"section-1","gameType":"ROCKET","outsideOfSchool":true,"loadtime":"2019-06-25T12:45:46.957Z","eventDateDw":"20190625","academicYearId":"763578c8-a546-4c74-b660-5df866a337c9","instructionalPlanId":"ip1","learningPathId":"lp1","classId":"b4d1b4e4-9eb5-4eff-92a4-94139f4f2312", "materialId": "ktd1b4e4-9eb5-4eff-92a4-94139f892300", "materialType": "CORE"}
              |]
              |""".stripMargin
        ),
        SparkFixture(
          key = ParquetKTGameQuestionFinishedSource,
          value =
            """
              |[
              |{"tenantId":"tenantId","eventType":"GameQuestionSessionFinishedEvent","occurredOn":"2019-06-25 12:45:55.037","questionId":"question-1","language":"ENGLISH","keyTermId":"kt-1","questionType":"MULTIPLE_CHOICE","time":8,"learningObjectiveId":"lo-1","status":"COMPLETED","gameSessionId":"game-session-id","learnerId":"student-1","subjectId":"subject-1","subjectName":"Math","subjectCode":"MATH","trimesterId":"trimester-1","trimesterOrder":3,"schoolId":"school-1","gradeId":"grade-1","grade":6,"sectionId":"section-1","gameType":"ROCKET","outsideOfSchool":true,"scoreBreakdown":{"isAttended":true,"maxScore":"1","score":1,"answer":null,"answers":["Value doesn't change","Value changes"],"timeSpent":7},"loadtime":"2019-06-25T12:45:55.116Z","eventDateDw":"20190625","academicYearId":"763578c8-a546-4c74-b660-5df866a337c9","instructionalPlanId":"ip1","learningPathId":"lp1","classId":"b4d1b4e4-9eb5-4eff-92a4-94139f4f2312", "materialId": "ktd1b4e4-9eb5-4eff-92a4-94139f892300", "materialType": "CORE"}
              |]
              |""".stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val deltaFactExpectCols = List(
            "ktg_session_id",
            "ktg_session_start_time",
            "ktg_session_end_time",
            "ktg_session_question_id",
            "ktg_session_tenant_id",
            "ktg_session_student_id",
            "ktg_session_subject_id",
            "ktg_session_school_id",
            "ktg_session_grade_id",
            "ktg_session_section_id",
            "ktg_session_lo_id",
            "ktg_session_academic_year_id",
            "ktg_session_class_id",
            "ktg_session_dw_created_time",
            "ktg_session_answer",
            "ktg_session_date_dw_id",
            "ktg_session_event_type",
            "ktg_session_instructional_plan_id",
            "ktg_session_is_attended",
            "ktg_session_is_start",
            "ktg_session_kt_id",
            "ktg_session_learning_path_id",
            "ktg_session_max_score",
            "ktg_session_num_attempts",
            "ktg_session_outside_of_school",
            "ktg_session_question_time_allotted",
            "ktg_session_score",
            "ktg_session_stars",
            "ktg_session_trimester_id",
            "ktg_session_trimester_order",
            "ktg_session_type",
            "ktg_session_time_spent",
            "eventdate",
            "ktg_session_is_start_event_processed",
            "ktg_session_material_id",
            "ktg_session_material_type"
          )

          val pqtStagingExpectCols =
            deltaFactExpectCols
              .diff(Seq("eventdate", "ktg_session_start_time", "ktg_session_end_time", "ktg_session_time_spent")) :+ "ktg_session_created_time"

          testSinkByEventType(sinks, DeltaKTGameSessionSink, "ktg_session", deltaFactExpectCols, 3)
          testSinkByEventType(sinks, DeltaKTGameSessionSink, "ktg_question_session", deltaFactExpectCols, 1)

          testSinkByEventType(sinks, ParquetTtGameDeltaStagingSource, "ktg_session", pqtStagingExpectCols, 2)
          testSinkByEventType(sinks, ParquetTtGameQuestionDeltaStagingSource, "ktg_question_session", pqtStagingExpectCols, 1)

        }
      )
    }
  }

}
