package com.alefeducation.facts

import com.alefeducation.bigdata.batch.delta.DeltaUpdateSink
import com.alefeducation.bigdata.commons.testutils.{ExpectedFields, SparkSuite}
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.types.StringType

class IncGameSpec extends SparkSuite with BaseDimensionSpec {

  import ExpectedFields._

  trait Setup {
    implicit val transformer = new IncGame(incGameName, spark)
  }

  val expectedRedshiftIncGameColumns: Set[String] = Set[String](
    "inc_game_num_questions",
    "inc_game_dw_created_time",
    "inc_game_date_dw_id",
    "tenant_uuid",
    "school_uuid",
    "lo_uuid",
    "inc_game_title",
    "teacher_uuid",
    "grade_uuid",
    "inc_game_id",
    "subject_uuid",
    "section_uuid",
    "inc_game_event_type",
    "inc_game_created_time",
    "inc_game_instructional_plan_id",
    "inc_game_is_assessment",
    "inc_game_lesson_component_id",
    "class_uuid"
  )

  val expectedDeltaIncGameColumns: Set[String] = Set[String](
    "inc_game_tenant_id",
    "inc_game_section_id",
    "inc_game_num_questions",
    "inc_game_instructional_plan_id",
    "inc_game_class_id",
    "inc_game_dw_created_time",
    "inc_game_title",
    "inc_game_lo_id",
    "inc_game_updated_time",
    "inc_game_teacher_id",
    "inc_game_grade_id",
    "inc_game_id",
    "inc_game_school_id",
    "inc_game_date_dw_id",
    "inc_game_subject_id",
    "inc_game_created_time",
    "inc_game_dw_updated_time",
    "inc_game_is_assessment",
    "inc_game_lesson_component_id",
    "eventdate"
  )

  val expectedRedshiftIncGameSessionColumns: Set[String] = Set[String](
    "inc_game_session_num_players",
    "inc_game_session_started_by",
    "tenant_uuid",
    "game_uuid",
    "inc_game_session_status",
    "inc_game_session_created_time",
    "inc_game_session_title",
    "inc_game_session_id",
    "inc_game_session_dw_created_time",
    "inc_game_session_num_joined_players",
    "inc_game_session_date_dw_id",
    "inc_game_session_is_start",
    "inc_game_session_is_start_event_processed",
    "inc_game_session_is_assessment"
  )

  val expectedDeltaIncGameSessionColumns: Set[String] = Set[String](
    "inc_game_session_is_start",
    "eventdate",
    "inc_game_session_num_players",
    "inc_game_session_started_by",
    "inc_game_session_tenant_id",
    "inc_game_session_id",
    "inc_game_session_status",
    "inc_game_session_is_start_event_processed",
    "inc_game_session_created_time",
    "inc_game_session_date_dw_id",
    "inc_game_session_title",
    "inc_game_session_dw_created_time",
    "inc_game_session_num_joined_players",
    "inc_game_session_game_id",
    "inc_game_session_is_assessment",
  )

  val expectedRedshiftIncGameOutcomeColumns: Set[String] = Set[String](
    "inc_game_outcome_status",
    "inc_game_outcome_created_time",
    "tenant_uuid",
    "lo_uuid",
    "inc_game_outcome_dw_created_time",
    "inc_game_outcome_id",
    "game_uuid",
    "session_uuid",
    "inc_game_outcome_score",
    "player_uuid",
    "inc_game_outcome_date_dw_id",
    "inc_game_outcome_is_assessment",
  )

  val expectedDeltaIncGameOutcomeColumns: Set[String] = Set[String](
    "inc_game_outcome_status",
    "inc_game_outcome_created_time",
    "inc_game_outcome_lo_id",
    "inc_game_outcome_date_dw_id",
    "inc_game_outcome_dw_created_time",
    "inc_game_outcome_id",
    "inc_game_outcome_game_id",
    "inc_game_outcome_session_id",
    "inc_game_outcome_player_id",
    "inc_game_outcome_tenant_id",
    "inc_game_outcome_score",
    "eventdate",
    "inc_game_outcome_score_breakdown",
    "inc_game_outcome_is_assessment"
  )

  test("Check addNumQuestions function in IncGameTransformer") {
    val session = spark
    import session.implicits._

    val questions = List("questionCode1", "questionCode2", "questionCode3", "questionCode4")
    val df = spark.sparkContext.parallelize(List(questions)).toDF("questions")

    val resDf = new IncGame(incGameName, spark).addNumQuestions(df)

    assert[Int](resDf, "numQuestions", 4)
  }

  test("Check addNumQuestions function in IncGameTransformer should return default value") {
    val session = spark
    import session.implicits._

    val questions = null
    val df = spark.sparkContext.parallelize(List((questions, List()))).toDF("questions", "someColumn")

    val resDf = new IncGame(incGameName, spark).addNumQuestions(df)

    assert[Int](resDf, "numQuestions", ZeroArraySize)
  }

  test("transform in class game created event successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = IncGameSource,
          value = """
              |{
              |   "id":"idVal",
              |   "gameId": "3f37b046-d00b-4fae-951a-cbe2ccb14aee",
              |   "tenantId": "tenantIdVal",
              |   "eventType": "InClassGameCreatedEvent",
              |   "occurredOn":"2019-08-25T09:56:31.183",
              |   "schoolId":"schoolIdVal",
              |   "classId":"classIdVal",
              |   "sectionId":"sectionIdVal",
              |   "mloId":"mloIdVal",
              |   "title":"titleVal",
              |   "teacherId":"teacherIdVal",
              |   "subjectId":"subjectIdVal",
              |   "schoolGradeId":"gradeIdVal",
              |   "genSubject":"genSubjectVal",
              |   "k12Grade":6,
              |   "groupId":"groupIdVal",
              |   "learningPathId":"learningPathIdVal",
              |   "questions":[
              |         {
              |           "code": "questionCode1",
              |           "time": 60
              |         },
              |         {
              |           "code": "questionCode2",
              |           "time": 59
              |         },
              |         {
              |           "code": "questionCode3",
              |           "time": 58
              |         }
              |   ],
              |   "eventDateDw":20190825,
              |   "instructionalPlanId": "",
              |   "newCBMClassId": "newClassId",
              |   "isFormativeAssessment": false,
              |   "lessonComponentId": "component-id-1"
              |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshiftSink = sinks.filter(_.name == RedshiftIncGameSink).head.output

          assert(redshiftSink.count === 1)
          assert(redshiftSink.columns.toSet === expectedRedshiftIncGameColumns)

          assert[String](redshiftSink, "inc_game_id", "3f37b046-d00b-4fae-951a-cbe2ccb14aee")
          assert[Int](redshiftSink, "inc_game_event_type", 1)
          assert[String](redshiftSink, "inc_game_created_time", "2019-08-25 09:56:31.183")
          assert[String](redshiftSink, "school_uuid", "schoolIdVal")
          assert[String](redshiftSink, "section_uuid", "sectionIdVal")
          assert[String](redshiftSink, "lo_uuid", "mloIdVal")
          assert[String](redshiftSink, "inc_game_title", "titleVal")
          assert[String](redshiftSink, "teacher_uuid", "teacherIdVal")
          assert[String](redshiftSink, "subject_uuid", "subjectIdVal")
          assert[String](redshiftSink, "grade_uuid", "gradeIdVal")
          assert[String](redshiftSink, "class_uuid", "classIdVal")
          assert[Int](redshiftSink, "inc_game_num_questions", 3)
          assert[Long](redshiftSink, "inc_game_date_dw_id", 20190825L)
          val expectedFields = List(ExpectedField(name = "class_uuid", dataType = StringType))
          assertExpectedFields(redshiftSink.schema.fields.filter(_.name == "class_uuid").toList, expectedFields)

          val deltaSink = sinks
            .find(_.name == DeltaIncGameSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$DeltaIncGameSink is not found"))

          assert(deltaSink.columns.toSet === expectedDeltaIncGameColumns)

          assert[String](deltaSink, "inc_game_id", "3f37b046-d00b-4fae-951a-cbe2ccb14aee")
          assert[String](deltaSink, "inc_game_created_time", "2019-08-25 09:56:31.183")
          assert[String](deltaSink, "inc_game_school_id", "schoolIdVal")
          assert[String](deltaSink, "inc_game_section_id", "sectionIdVal")
          assert[String](deltaSink, "inc_game_lo_id", "mloIdVal")
          assert[String](deltaSink, "inc_game_title", "titleVal")
          assert[String](deltaSink, "inc_game_teacher_id", "teacherIdVal")
          assert[String](deltaSink, "inc_game_subject_id", "subjectIdVal")
          assert[String](deltaSink, "inc_game_grade_id", "gradeIdVal")
          assert[Int](deltaSink, "inc_game_num_questions", 3)
          assert[String](deltaSink, "inc_game_date_dw_id", "20190825")
          assert[String](deltaSink, "inc_game_tenant_id", "tenantIdVal")
          assert[String](deltaSink, "inc_game_instructional_plan_id", "")
          assert[String](deltaSink, "inc_game_class_id", "classIdVal")
          assert[String](deltaSink, "eventdate", "2019-08-25")
        }
      )
    }
  }

  test("transform in class game updated event successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = IncGameSource,
          value = """
              |[
              |{
              |   "id":"idVal",
              |   "gameId": "3f37b046-d00b-4fae-951a-cbe2ccb14aee",
              |   "tenantId": "tenantIdVal",
              |   "eventType": "InClassGameUpdatedEvent",
              |   "occurredOn":"2019-08-25T09:56:31.183",
              |   "schoolId":"schoolIdVal",
              |   "classId":"classIdVal",
              |   "sectionId":"sectionIdVal",
              |   "mloId":"mloIdVal",
              |   "title":"updTitleVal",
              |   "teacherId":"teacherIdVal",
              |   "subjectId":"subjectIdVal",
              |   "schoolGradeId":"gradeIdVal",
              |   "genSubject":"genSubjectVal",
              |   "k12Grade":6,
              |   "groupId":"groupIdVal",
              |   "learningPathId":"learningPathIdVal",
              |   "questions":[
              |         {
              |           "code": "questionCode1",
              |           "time": 60
              |         },
              |         {
              |           "code": "questionCode2",
              |           "time": 59
              |         },
              |         {
              |           "code": "questionCode3",
              |           "time": 58
              |         }
              |   ],
              |   "eventDateDw":20190825,
              |   "instructionalPlanId": " ",
              |   "newCBMClassId": "newClassId",
              |   "isFormativeAssessment": false,
              |   "lessonComponentId": "component-id-1"
              |},
              |{
              |   "id":"idVal",
              |   "gameId": "3f37b046-d00b-4fae-951a-cbe2ccb14aee",
              |   "tenantId": "tenantIdVal",
              |   "eventType": "InClassGameUpdatedEvent",
              |   "occurredOn":"2019-08-25T09:57:31.183",
              |   "schoolId":"schoolIdVal",
              |   "classId":"classIdVal",
              |   "sectionId":"sectionIdVal",
              |   "mloId":"mloIdVal",
              |   "title":"updTitleVal",
              |   "teacherId":"teacherIdVal",
              |   "subjectId":"subjectIdVal",
              |   "schoolGradeId":"gradeIdVal",
              |   "genSubject":"genSubjectVal",
              |   "k12Grade":6,
              |   "groupId":"groupIdVal",
              |   "learningPathId":"learningPathIdVal",
              |   "questions":[
              |         {
              |           "code": "questionCode1",
              |           "time": 60
              |         },
              |         {
              |           "code": "questionCode2",
              |           "time": 59
              |         },
              |         {
              |           "code": "questionCode3",
              |           "time": 58
              |         }
              |   ],
              |   "eventDateDw":20190825,
              |   "instructionalPlanId": " ",
              |   "newCBMClassId": "newClassId",
              |   "isFormativeAssessment": false,
              |   "lessonComponentId": "component-id-1"
              |}
              |]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks.filter(_.name == RedshiftIncGameSink).head.output

          assert(df.count === 2)
          assert(df.columns.toSet === expectedRedshiftIncGameColumns)

          assert[String](df, "inc_game_id", "3f37b046-d00b-4fae-951a-cbe2ccb14aee")
          assert[String](df, "tenant_uuid", "tenantIdVal")
          assert[Int](df, "inc_game_event_type", 2)
          assert[String](df, "inc_game_created_time", "2019-08-25 09:56:31.183")
          assert[String](df, "school_uuid", "schoolIdVal")
          assert[String](df, "section_uuid", "sectionIdVal")
          assert[String](df, "lo_uuid", "mloIdVal")
          assert[String](df, "inc_game_title", "updTitleVal")
          assert[String](df, "teacher_uuid", "teacherIdVal")
          assert[String](df, "subject_uuid", "subjectIdVal")
          assert[String](df, "grade_uuid", "gradeIdVal")
          assert[Int](df, "inc_game_num_questions", 3)
          assert[Long](df, "inc_game_date_dw_id", 20190825)
          val expectedFields = List(ExpectedField(name = "class_uuid", dataType = StringType))
          assertExpectedFields(df.schema.fields.filter(_.name == "class_uuid").toList, expectedFields)

          val deltaDf = sinks
            .find(x => x.name == DeltaIncGameSink && x.isInstanceOf[DeltaUpdateSink])
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$DeltaIncGameSink is not found"))

          assert(deltaDf.columns.toSet === expectedDeltaIncGameColumns)
          assert(deltaDf.count == 1)

          assert[String](deltaDf, "inc_game_id", "3f37b046-d00b-4fae-951a-cbe2ccb14aee")
          assert[String](deltaDf, "inc_game_created_time", "2019-08-25 09:57:31.183")
          assert[String](deltaDf, "inc_game_school_id", "schoolIdVal")
          assert[String](deltaDf, "inc_game_section_id", "sectionIdVal")
          assert[String](deltaDf, "inc_game_lo_id", "mloIdVal")
          assert[String](deltaDf, "inc_game_title", "updTitleVal")
          assert[String](deltaDf, "inc_game_teacher_id", "teacherIdVal")
          assert[String](deltaDf, "inc_game_subject_id", "subjectIdVal")
          assert[String](deltaDf, "inc_game_grade_id", "gradeIdVal")
          assert[Int](deltaDf, "inc_game_num_questions", 3)
          assert[Long](deltaDf, "inc_game_date_dw_id", 20190825L)
          assert[String](deltaDf, "inc_game_tenant_id", "tenantIdVal")
          assert[String](deltaDf, "inc_game_instructional_plan_id", " ")
          assert[String](deltaDf, "inc_game_class_id", "classIdVal")
          assert[String](deltaDf, "eventdate", "2019-08-25")
        }
      )
    }
  }

  test("transform in class game session started event successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = IncGameSessionSource,
          value = """
              |{
              |   "tenantId": "tenantIdVal",
              |   "eventType": "InClassGameSessionStartedEvent",
              |   "id":"idVal",
              |   "sessionId": "a615b793-1084-4880-a3d1-98c02af5473f",
              |   "occurredOn":"2019-08-25T11:14:19.789",
              |   "gameId":"gameIdVal",
              |   "gameTitle":"gameTitleVal",
              |   "players":[
              |      "playerId1",
              |      "playerId2"
              |   ],
              |   "joinedPlayersDetails":  [
              |    { "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },
              |    { "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }
              | ],
              |   "questions":[
              |         {
              |           "code": "questionCode1",
              |           "time": 60
              |         },
              |         {
              |           "code": "questionCode2",
              |           "time": 59
              |         },
              |         {
              |           "code": "questionCode3",
              |           "time": 58
              |         }
              |   ],
              |   "startedOn":"2019-08-25T11:14:19.793",
              |   "startedBy":"teacherIdVal",
              |   "status":"IN_PROGRESS",
              |   "isFormativeAssessment": false,
              |   "lessonComponentId": "component-id-1",
              |   "eventDateDw":20190825
              |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshiftSink = sinks.filter(_.name == RedshiftIncGameSessionSink).head.output

          assert(redshiftSink.count === 1)
          assert(redshiftSink.columns.toSet === expectedRedshiftIncGameSessionColumns)

          assert[String](redshiftSink, "inc_game_session_id", "a615b793-1084-4880-a3d1-98c02af5473f")
          assert[String](redshiftSink, "tenant_uuid", "tenantIdVal")
          assert[String](redshiftSink, "game_uuid", "gameIdVal")
          assert[String](redshiftSink, "inc_game_session_title", "gameTitleVal")
          assert[Int](redshiftSink, "inc_game_session_status", 1)
          assert[String](redshiftSink, "inc_game_session_started_by", "teacherIdVal")
          assert[Int](redshiftSink, "inc_game_session_num_players", 2)
          assert[Int](redshiftSink, "inc_game_session_num_joined_players", 2)
          assert[Long](redshiftSink, "inc_game_session_date_dw_id", 20190825)
          assert[Boolean](redshiftSink, "inc_game_session_is_start", true)
          assert[Boolean](redshiftSink, "inc_game_session_is_start_event_processed", false)
        }
      )
    }
  }

  test("transform in class game session finished event successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = IncGameSessionSource,
          value = """
              |{
              |   "tenantId": "tenantIdVal",
              |   "eventType": "InClassGameSessionFinishedEvent",
              |   "id":"idVal",
              |   "sessionId": "a615b793-1084-4880-a3d1-98c02af5473f",
              |   "occurredOn":"2019-08-25T11:14:19.789",
              |   "gameId":"gameIdVal",
              |   "gameTitle":"gameTitleVal",
              |   "players":[
              |      "playerId1",
              |      "playerId2",
              |      "playerId3",
              |      "playerId4"
              |   ],
              |   "joinedPlayersDetails":  [
              |    { "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },
              |    { "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }
              | ],
              |   "questions":[
              |         {
              |           "code": "questionCode1",
              |           "time": 60
              |         },
              |         {
              |           "code": "questionCode2",
              |           "time": 59
              |         },
              |         {
              |           "code": "questionCode3",
              |           "time": 58
              |         }
              |   ],
              |   "startedOn":"2019-08-25T11:14:19.793",
              |   "startedBy":"teacherIdVal",
              |   "status":"COMPLETED",
              |   "eventDateDw":20190825,
              |   "isFormativeAssessment": false,
              |   "lessonComponentId": "component-id-1"
              |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks.filter(_.name == RedshiftIncGameSessionSink).head.output

          assert(df.count === 1)
          assert(df.columns.toSet === expectedRedshiftIncGameSessionColumns)

          assert[String](df, "inc_game_session_id", "a615b793-1084-4880-a3d1-98c02af5473f")
          assert[String](df, "tenant_uuid", "tenantIdVal")
          assert[String](df, "game_uuid", "gameIdVal")
          assert[String](df, "inc_game_session_title", "gameTitleVal")
          assert[Int](df, "inc_game_session_status", 2)
          assert[String](df, "inc_game_session_started_by", "teacherIdVal")
          assert[Int](df, "inc_game_session_num_players", 4)
          assert[Int](df, "inc_game_session_num_joined_players", 2)
          assert[Int](df, "inc_game_session_date_dw_id", 20190825)
          assert[Boolean](df, "inc_game_session_is_start", false)
          assert[Boolean](df, "inc_game_session_is_start_event_processed", false)

        }
      )
    }
  }

  test("transform in class game session canceled event successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = IncGameSessionSource,
          value = """
              |{
              |   "tenantId": "tenantIdVal",
              |   "eventType": "InClassGameSessionCancelledEvent",
              |   "id":"idVal",
              |   "sessionId": "a615b793-1084-4880-a3d1-98c02af5473f",
              |   "occurredOn":"2019-08-25T11:14:19.789",
              |   "gameId":"gameIdVal",
              |   "gameTitle":"gameTitleVal",
              |   "players":[
              |      "playerId1",
              |      "playerId2",
              |      "playerId3",
              |      "playerId4"
              |   ],
              |   "joinedPlayersDetails":  [
              |    { "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },
              |    { "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }
              | ],
              |   "questions":[
              |         {
              |           "code": "questionCode1",
              |           "time": 60
              |         },
              |         {
              |           "code": "questionCode2",
              |           "time": 59
              |         },
              |         {
              |           "code": "questionCode3",
              |           "time": 58
              |         }
              |   ],
              |   "startedOn":"2019-08-25T11:14:19.793",
              |   "startedBy":"teacherIdVal",
              |   "status":"CANCELLED",
              |   "eventDateDw":20190825,
              |   "isFormativeAssessment": false,
              |   "lessonComponentId": "component-id-1"
              |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks.filter(_.name == RedshiftIncGameSessionSink).head.output

          assert(df.count === 1)
          assert(df.columns.toSet === expectedRedshiftIncGameSessionColumns)

          assert[String](df, "inc_game_session_id", "a615b793-1084-4880-a3d1-98c02af5473f")
          assert[String](df, "tenant_uuid", "tenantIdVal")
          assert[String](df, "game_uuid", "gameIdVal")
          assert[String](df, "inc_game_session_title", "gameTitleVal")
          assert[Int](df, "inc_game_session_status", 3)
          assert[String](df, "inc_game_session_started_by", "teacherIdVal")
          assert[Int](df, "inc_game_session_num_players", 4)
          assert[Int](df, "inc_game_session_num_joined_players", 2)
          assert[Int](df, "inc_game_session_date_dw_id", 20190825)
          assert[Boolean](df, "inc_game_session_is_start", false)
          assert[Boolean](df, "inc_game_session_is_start_event_processed", false)

        }
      )
    }
  }

  test("transform in class game outcome finished event successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = IncGameOutcomeSource,
          value = """
              |{
              |   "tenantId":"tenantIdVal",
              |   "eventType":"InClassGameOutcomeFinishedEvent",
              |   "id":"d655a72f-9eef-4d35-87d4-5a19fefb4636",
              |   "outcomeId": "7d9c54dc-c8c4-4e1d-be76-277e9597d031",
              |   "sessionId":"168403a4-b40e-45fa-9e69-6a3049f0807d",
              |   "gameId":"481cc079-837b-4236-a6ae-61f7997d4e71",
              |   "playerId":"03956a55-7deb-410e-b544-22a38ed744a0",
              |   "mloId":"dbee653b-c9ed-4bb5-855b-d67cd4671138",
              |   "assessmentId":"102db926-a35a-4a49-a88f-49470a2b9291",
              |   "score":0.2,
              |   "scoreBreakdown":[
              |      {
              |         "code":"MA7_MLO_036_Q_30",
              |         "score":0.0,
              |         "timeSpent":3,
              |         "createdAt":"2019-08-27T06:57:39.376",
              |         "updatedAt":"2019-08-27T06:58:13.799",
              |         "question_status": "CORRECT"
              |      },
              |      {
              |         "code":"MA7_MLO_036_Q_29",
              |         "score":1.0,
              |         "timeSpent":2,
              |         "createdAt":"2019-08-27T06:57:39.376",
              |         "updatedAt":"2019-08-27T06:58:36.556",
              |         "question_status": "CORRECT"
              |      },
              |      {
              |         "code":"MA7_MLO_036_Q_34",
              |         "score":0.0,
              |         "timeSpent":1,
              |         "createdAt":"2019-08-27T06:57:39.376",
              |         "updatedAt":"2019-08-27T06:58:49.261",
              |         "question_status": "CORRECT"
              |      },
              |      {
              |         "code":"MA7_MLO_036_Q_31",
              |         "score":0.0,
              |         "timeSpent":14,
              |         "createdAt":"2019-08-27T06:57:39.376",
              |         "updatedAt":"2019-08-27T06:59:13.66",
              |         "question_status": "CORRECT"
              |      },
              |      {
              |         "code":"MA7_MLO_036_Q_40",
              |         "score":0.0,
              |         "timeSpent":4,
              |         "createdAt":"2019-08-27T06:57:39.376",
              |         "updatedAt":"2019-08-27T06:59:31.068",
              |         "question_status": "CORRECT"
              |
              |      }
              |   ],
              |   "createdAt":"2019-08-27T06:57:39.376",
              |   "updatedAt":"2019-08-27T06:59:31.068",
              |   "status":"COMPLETED",
              |   "occurredOn":"2019-08-27T07:00:08.5",
              |   "eventDateDw":20190825,
              |   "isFormativeAssessment": false,
              |   "lessonComponentId": "component-id-1"
              |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshiftSink = sinks.filter(_.name == RedshiftIncGameOutcomeSink).head.output

          assert(redshiftSink.count === 1)
          assert(redshiftSink.columns.toSet === expectedRedshiftIncGameOutcomeColumns)

          assert[String](redshiftSink, "tenant_uuid", "tenantIdVal")
          assert[Int](redshiftSink, "inc_game_outcome_status", 1)
          assert[String](redshiftSink, "inc_game_outcome_created_time", "2019-08-27 07:00:08.5")
          assert[String](redshiftSink, "lo_uuid", "dbee653b-c9ed-4bb5-855b-d67cd4671138")
          assert[String](redshiftSink, "inc_game_outcome_id", "7d9c54dc-c8c4-4e1d-be76-277e9597d031")
          assert[String](redshiftSink, "game_uuid", "481cc079-837b-4236-a6ae-61f7997d4e71")
          assert[String](redshiftSink, "session_uuid", "168403a4-b40e-45fa-9e69-6a3049f0807d")
          assert[Double](redshiftSink, "inc_game_outcome_score", 0.2)
          assert[String](redshiftSink, "player_uuid", "03956a55-7deb-410e-b544-22a38ed744a0")
          assert[Long](redshiftSink, "inc_game_outcome_date_dw_id", 20190827)

          val deltaSink = sinks
            .find(_.name == DeltaIncGameOutcomeSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$DeltaIncGameOutcomeSink is not found"))

          assert(deltaSink.columns.toSet === expectedDeltaIncGameOutcomeColumns)

          assert[String](deltaSink, "inc_game_outcome_tenant_id", "tenantIdVal")
          assert[String](deltaSink, "inc_game_outcome_id", "7d9c54dc-c8c4-4e1d-be76-277e9597d031")
          assert[String](deltaSink, "inc_game_outcome_game_id", "481cc079-837b-4236-a6ae-61f7997d4e71")
          assert[String](deltaSink, "inc_game_outcome_session_id", "168403a4-b40e-45fa-9e69-6a3049f0807d")
          assert[String](deltaSink, "inc_game_outcome_player_id", "03956a55-7deb-410e-b544-22a38ed744a0")
          assert[Int](deltaSink, "inc_game_outcome_status", 1)
          assert[String](deltaSink, "inc_game_outcome_created_time", "2019-08-27 07:00:08.5")
          assert[String](deltaSink, "inc_game_outcome_lo_id", "dbee653b-c9ed-4bb5-855b-d67cd4671138")
          assert[String](deltaSink, "inc_game_outcome_date_dw_id", "20190827")
          assert[Double](deltaSink, "inc_game_outcome_score", 0.2)
          assert[String](deltaSink, "eventdate", "2019-08-27")
        }
      )
    }
  }

  test("transform in class game session created event successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = IncGameSessionSource,
          value = """
                    |{
                    |   "tenantId": "tenantIdVal",
                    |   "eventType": "InClassGameSessionCreatedEvent",
                    |   "id":"idVal",
                    |   "sessionId": "a615b793-1084-4880-a3d1-98c02af5473f",
                    |   "occurredOn":"2019-08-25T11:14:19.789",
                    |   "gameId":"gameIdVal",
                    |   "gameTitle":"gameTitleVal",
                    |   "players":[
                    |      "playerId1",
                    |      "playerId2",
                    |      "playerId3",
                    |      "playerId4"
                    |   ],
                    |   "joinedPlayersDetails": [
                    |       { "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },
                    |       { "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }
                    |   ],
                    |   "questions":[
                    |         {
                    |           "code": "questionCode1",
                    |           "time": 60
                    |         },
                    |         {
                    |           "code": "questionCode2",
                    |           "time": 59
                    |         },
                    |         {
                    |           "code": "questionCode3",
                    |           "time": 58
                    |         }
                    |   ],
                    |   "startedOn":"2019-08-25T11:14:19.793",
                    |   "startedBy":"teacherIdVal",
                    |   "status":"OPEN",
                    |   "eventDateDw":20190825,
                    |   "isFormativeAssessment": false,
                    |   "lessonComponentId": "component-id-1"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks.filter(_.name == RedshiftIncGameSessionSink).head.output

          assert(df.count === 1)
          assert(df.columns.toSet === expectedRedshiftIncGameSessionColumns)

          assert[String](df, "inc_game_session_id", "a615b793-1084-4880-a3d1-98c02af5473f")
          assert[String](df, "tenant_uuid", "tenantIdVal")
          assert[String](df, "game_uuid", "gameIdVal")
          assert[String](df, "inc_game_session_title", "gameTitleVal")
          assert[Int](df, "inc_game_session_status", 4)
          assert[String](df, "inc_game_session_started_by", "teacherIdVal")
          assert[Int](df, "inc_game_session_num_players", 4)
          assert[Int](df, "inc_game_session_num_joined_players", 2)
          assert[Int](df, "inc_game_session_date_dw_id", 20190825)
          assert[Boolean](df, "inc_game_session_is_start", true)
          assert[Boolean](df, "inc_game_session_is_start_event_processed", false)

        }
      )
    }
  }

  test("transform in class game DELTA session events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = IncGameSessionSource,
          value =
            """
              |[
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"eventType":"InClassGameSessionCreatedEvent","loadtime":"2020-07-21T10:34:25.120Z","sessionId":"6f358fe2-b303-442b-aa13-d22a0532fba0","occurredOn":"2020-07-21 10:34:25.081","gameId":"04430a9c-4ba8-4d52-a4b5-3f78d74fd018","gameTitle":"test","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","c0f7afde-978b-4438-ab73-eda07f9a2709","cf428419-3585-4c7a-84f4-632c4b83b993","6dddd242-5300-429a-81a1-da7bd3d96916","77260b3e-ba48-4cb0-a7a0-7ed759155301","dfea525a-56ac-4bb7-90e3-30f3eec5f345","d171beb4-0483-45c8-9ab0-c340a9a4ebfa","7480f6fa-b97a-42df-ab01-851709b20a6e","2606a1ba-460c-4ddf-9fc1-bcb6db4ac126"],"questions":[{"code":"MA8_MLO_009_Q_23","time":60},{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_12","time":60},{"code":"MA8_MLO_009_Q_019_IMPORT","time":60},{"code":"MA8_MLO_009_Q_6","time":60}],"startedOn":"2020-07-21T10:34:25.081","startedBy":"71da94a8-b951-4311-9262-e87220ab649f","updatedAt":"2020-07-21T10:34:25.081","status":"OPEN","eventDateDw":"20200721"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionCancelledEvent","loadtime":"2020-07-21T10:34:29.496Z","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"sessionId":"6f358fe2-b303-442b-aa13-d22a0532fba0","occurredOn":"2020-07-21 10:34:29.453","gameId":"04430a9c-4ba8-4d52-a4b5-3f78d74fd018","gameTitle":"test","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","c0f7afde-978b-4438-ab73-eda07f9a2709","cf428419-3585-4c7a-84f4-632c4b83b993","6dddd242-5300-429a-81a1-da7bd3d96916","77260b3e-ba48-4cb0-a7a0-7ed759155301","dfea525a-56ac-4bb7-90e3-30f3eec5f345","d171beb4-0483-45c8-9ab0-c340a9a4ebfa","7480f6fa-b97a-42df-ab01-851709b20a6e","2606a1ba-460c-4ddf-9fc1-bcb6db4ac126"],"questions":[{"code":"MA8_MLO_009_Q_23","time":60},{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_12","time":60},{"code":"MA8_MLO_009_Q_019_IMPORT","time":60},{"code":"MA8_MLO_009_Q_6","time":60}],"startedOn":"2020-07-21T10:34:25.081","startedBy":"71da94a8-b951-4311-9262-e87220ab649f","updatedAt":"2020-07-21T10:34:29.453","status":"CANCELLED","eventDateDw":"20200721"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionCreatedEvent","loadtime":"2020-07-21T10:45:06.936Z","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"sessionId":"d8b84f27-25e6-467f-977f-42a118b78b86","occurredOn":"2020-07-21 10:45:06.925","gameId":"04430a9c-4ba8-4d52-a4b5-3f78d74fd018","gameTitle":"test","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","c0f7afde-978b-4438-ab73-eda07f9a2709","cf428419-3585-4c7a-84f4-632c4b83b993","6dddd242-5300-429a-81a1-da7bd3d96916","77260b3e-ba48-4cb0-a7a0-7ed759155301","dfea525a-56ac-4bb7-90e3-30f3eec5f345","d171beb4-0483-45c8-9ab0-c340a9a4ebfa","7480f6fa-b97a-42df-ab01-851709b20a6e","2606a1ba-460c-4ddf-9fc1-bcb6db4ac126"],"questions":[{"code":"MA8_MLO_009_Q_23","time":60},{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_12","time":60},{"code":"MA8_MLO_009_Q_019_IMPORT","time":60},{"code":"MA8_MLO_009_Q_6","time":60}],"startedOn":"2020-07-21T10:45:06.925","startedBy":"71da94a8-b951-4311-9262-e87220ab649f","updatedAt":"2020-07-21T10:45:06.925","status":"OPEN","eventDateDw":"20200721"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionStartedEvent","loadtime":"2020-07-21T10:45:24.050Z","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"sessionId":"d8b84f27-25e6-467f-977f-42a118b78b86","occurredOn":"2020-07-21 10:45:24.032","gameId":"04430a9c-4ba8-4d52-a4b5-3f78d74fd018","gameTitle":"test","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","c0f7afde-978b-4438-ab73-eda07f9a2709","cf428419-3585-4c7a-84f4-632c4b83b993","6dddd242-5300-429a-81a1-da7bd3d96916","77260b3e-ba48-4cb0-a7a0-7ed759155301","dfea525a-56ac-4bb7-90e3-30f3eec5f345","d171beb4-0483-45c8-9ab0-c340a9a4ebfa","7480f6fa-b97a-42df-ab01-851709b20a6e","2606a1ba-460c-4ddf-9fc1-bcb6db4ac126"],"questions":[{"code":"MA8_MLO_009_Q_23","time":60},{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_12","time":60},{"code":"MA8_MLO_009_Q_019_IMPORT","time":60},{"code":"MA8_MLO_009_Q_6","time":60}],"startedOn":"2020-07-21T10:45:24.032","startedBy":"71da94a8-b951-4311-9262-e87220ab649f","updatedAt":"2020-07-21T10:45:24.032","status":"IN_PROGRESS","eventDateDw":"20200721"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionFinishedEvent","loadtime":"2020-07-21T10:46:43.651Z","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"sessionId":"d8b84f27-25e6-467f-977f-42a118b78b86","occurredOn":"2020-07-21 10:46:43.591","gameId":"04430a9c-4ba8-4d52-a4b5-3f78d74fd018","gameTitle":"test","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","c0f7afde-978b-4438-ab73-eda07f9a2709","cf428419-3585-4c7a-84f4-632c4b83b993","6dddd242-5300-429a-81a1-da7bd3d96916","77260b3e-ba48-4cb0-a7a0-7ed759155301","dfea525a-56ac-4bb7-90e3-30f3eec5f345","d171beb4-0483-45c8-9ab0-c340a9a4ebfa","7480f6fa-b97a-42df-ab01-851709b20a6e","2606a1ba-460c-4ddf-9fc1-bcb6db4ac126"],"questions":[{"code":"MA8_MLO_009_Q_23","time":60},{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_12","time":60},{"code":"MA8_MLO_009_Q_019_IMPORT","time":60},{"code":"MA8_MLO_009_Q_6","time":60}],"startedOn":"2020-07-21T10:45:24.032","startedBy":"71da94a8-b951-4311-9262-e87220ab649f","updatedAt":"2020-07-21T10:46:43.591","status":"COMPLETED","eventDateDw":"20200721"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionCreatedEvent","loadtime":"2020-07-21T10:52:16.602Z","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"sessionId":"c454bd53-fc9c-481c-b404-8144044a0461","occurredOn":"2020-07-21 10:52:16.593","gameId":"cd3d8c66-2f27-472d-8235-bd7a5d19a02a","gameTitle":"testingdfg","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","c0f7afde-978b-4438-ab73-eda07f9a2709","cf428419-3585-4c7a-84f4-632c4b83b993"],"questions":[{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_036_FIN_IMPORT","time":60},{"code":"MA8_MLO_009_Q_018_FIN_IMPORT","time":60},{"code":"MA8_MLO_009_Q_027_IMPORT","time":60},{"code":"MA8_MLO_009_Q_002_FIN_IMPORT","time":60}],"startedOn":"2020-07-21T10:52:16.593","startedBy":"71da94a8-b951-4311-9262-e87220ab649f","updatedAt":"2020-07-21T10:52:16.593","status":"OPEN","eventDateDw":"20200721"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionStartedEvent","loadtime":"2020-07-21T10:52:30.194Z","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"sessionId":"c454bd53-fc9c-481c-b404-8144044a0461","occurredOn":"2020-07-21 10:52:30.187","gameId":"cd3d8c66-2f27-472d-8235-bd7a5d19a02a","gameTitle":"testingdfg","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","c0f7afde-978b-4438-ab73-eda07f9a2709","cf428419-3585-4c7a-84f4-632c4b83b993"],"questions":[{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_036_FIN_IMPORT","time":60},{"code":"MA8_MLO_009_Q_018_FIN_IMPORT","time":60},{"code":"MA8_MLO_009_Q_027_IMPORT","time":60},{"code":"MA8_MLO_009_Q_002_FIN_IMPORT","time":60}],"startedOn":"2020-07-21T10:52:30.187","startedBy":"71da94a8-b951-4311-9262-e87220ab649f","updatedAt":"2020-07-21T10:52:30.187","status":"IN_PROGRESS","eventDateDw":"20200721"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionFinishedEvent","loadtime":"2020-07-21T10:53:22.092Z","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"sessionId":"c454bd53-fc9c-481c-b404-8144044a0461","occurredOn":"2020-07-21 10:53:22.079","gameId":"cd3d8c66-2f27-472d-8235-bd7a5d19a02a","gameTitle":"testingdfg","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","c0f7afde-978b-4438-ab73-eda07f9a2709","cf428419-3585-4c7a-84f4-632c4b83b993"],"questions":[{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_036_FIN_IMPORT","time":60},{"code":"MA8_MLO_009_Q_018_FIN_IMPORT","time":60},{"code":"MA8_MLO_009_Q_027_IMPORT","time":60},{"code":"MA8_MLO_009_Q_002_FIN_IMPORT","time":60}],"startedOn":"2020-07-21T10:52:30.187","startedBy":"71da94a8-b951-4311-9262-e87220ab649f","updatedAt":"2020-07-21T10:53:22.079","status":"COMPLETED","eventDateDw":"20200721"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionCreatedEvent","loadtime":"2020-07-21T10:56:30.973Z","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"sessionId":"4847d666-97be-454c-ad9d-1dcadf4e5e1d","occurredOn":"2020-07-21 10:56:30.966","gameId":"6a6dbe43-316e-46b9-b0f6-d83f46177a42","gameTitle":"testxcg","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","c0f7afde-978b-4438-ab73-eda07f9a2709","cf428419-3585-4c7a-84f4-632c4b83b993"],"questions":[{"code":"MA8_MLO_009_Q_10","time":60},{"code":"MA8_MLO_009_Q_036_FIN_IMPORT","time":60},{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_18","time":60},{"code":"MA8_MLO_009_Q_12","time":60}],"startedOn":"2020-07-21T10:56:30.966","startedBy":"71da94a8-b951-4311-9262-e87220ab649f","updatedAt":"2020-07-21T10:56:30.966","status":"OPEN","eventDateDw":"20200721"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionStartedEvent","loadtime":"2020-07-21T10:56:41.132Z","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"sessionId":"4847d666-97be-454c-ad9d-1dcadf4e5e1d","occurredOn":"2020-07-21 10:56:41.119","gameId":"6a6dbe43-316e-46b9-b0f6-d83f46177a42","gameTitle":"testxcg","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","c0f7afde-978b-4438-ab73-eda07f9a2709","cf428419-3585-4c7a-84f4-632c4b83b993"],"questions":[{"code":"MA8_MLO_009_Q_10","time":60},{"code":"MA8_MLO_009_Q_036_FIN_IMPORT","time":60},{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_18","time":60},{"code":"MA8_MLO_009_Q_12","time":60}],"startedOn":"2020-07-21T10:56:41.119","startedBy":"71da94a8-b951-4311-9262-e87220ab649f","updatedAt":"2020-07-21T10:56:41.119","status":"IN_PROGRESS","eventDateDw":"20200721"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionCancelledEvent","loadtime":"2020-07-21T10:57:19.738Z","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"sessionId":"4847d666-97be-454c-ad9d-1dcadf4e5e1d","occurredOn":"2020-07-21 10:57:19.724","gameId":"6a6dbe43-316e-46b9-b0f6-d83f46177a42","gameTitle":"testxcg","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","c0f7afde-978b-4438-ab73-eda07f9a2709","cf428419-3585-4c7a-84f4-632c4b83b993"],"questions":[{"code":"MA8_MLO_009_Q_10","time":60},{"code":"MA8_MLO_009_Q_036_FIN_IMPORT","time":60},{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_18","time":60},{"code":"MA8_MLO_009_Q_12","time":60}],"startedOn":"2020-07-21T10:56:41.119","startedBy":"71da94a8-b951-4311-9262-e87220ab649f","updatedAt":"2020-07-21T10:57:19.724","status":"CANCELLED","eventDateDw":"20200721"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionCreatedEvent","loadtime":"2020-07-23T07:06:09.059Z","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"sessionId":"a257d0d6-f81d-4963-8ebe-64ab596e66fa","occurredOn":"2020-07-23 07:06:08.648","gameId":"6a6dbe43-316e-46b9-b0f6-d83f46177a42","gameTitle":"testxcg","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","6b5ecd37-f23d-4527-86a9-9e4c3bf95a7f","c0f7afde-978b-4438-ab73-eda07f9a2709","6c659350-404f-4769-98ef-95928bbe1586","cf428419-3585-4c7a-84f4-632c4b83b993","ddcc7388-1e15-4d82-b683-3251da1bf9d5","b22059b5-f98a-4598-b479-b4a432ffc129","77260b3e-ba48-4cb0-a7a0-7ed759155301","602526b4-7a5d-49de-b9de-2016a776157b"],"questions":[{"code":"MA8_MLO_009_Q_10","time":60},{"code":"MA8_MLO_009_Q_036_FIN_IMPORT","time":60},{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_18","time":60},{"code":"MA8_MLO_009_Q_12","time":60}],"startedOn":"2020-07-23T07:06:08.648","startedBy":"ca85e5e5-9734-417d-b19e-84ce258e4869","updatedAt":"2020-07-23T07:06:08.648","status":"OPEN","eventDateDw":"20200723"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionCancelledEvent","loadtime":"2020-07-23T07:06:20.158Z","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"sessionId":"a257d0d6-f81d-4963-8ebe-64ab596e66fa","occurredOn":"2020-07-23 07:06:20.138","gameId":"6a6dbe43-316e-46b9-b0f6-d83f46177a42","gameTitle":"testxcg","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","6b5ecd37-f23d-4527-86a9-9e4c3bf95a7f","c0f7afde-978b-4438-ab73-eda07f9a2709","6c659350-404f-4769-98ef-95928bbe1586","cf428419-3585-4c7a-84f4-632c4b83b993","ddcc7388-1e15-4d82-b683-3251da1bf9d5","b22059b5-f98a-4598-b479-b4a432ffc129","77260b3e-ba48-4cb0-a7a0-7ed759155301","602526b4-7a5d-49de-b9de-2016a776157b"],"questions":[{"code":"MA8_MLO_009_Q_10","time":60},{"code":"MA8_MLO_009_Q_036_FIN_IMPORT","time":60},{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_18","time":60},{"code":"MA8_MLO_009_Q_12","time":60}],"startedOn":"2020-07-23T07:06:08.648","startedBy":"ca85e5e5-9734-417d-b19e-84ce258e4869","updatedAt":"2020-07-23T07:06:20.138","status":"CANCELLED","eventDateDw":"20200723"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionCancelledEvent","loadtime":"2020-07-23T07:06:20.158Z","sessionId":"a257d0d6-f81d-4963-8ebe-64ab596SSS2","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"occurredOn":"2020-07-23 07:06:20.138","gameId":"6a6dbe43-316e-46b9-b0f6-d83f46177a42","gameTitle":"testxcg","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","6b5ecd37-f23d-4527-86a9-9e4c3bf95a7f","c0f7afde-978b-4438-ab73-eda07f9a2709","6c659350-404f-4769-98ef-95928bbe1586","cf428419-3585-4c7a-84f4-632c4b83b993","ddcc7388-1e15-4d82-b683-3251da1bf9d5","b22059b5-f98a-4598-b479-b4a432ffc129","77260b3e-ba48-4cb0-a7a0-7ed759155301","602526b4-7a5d-49de-b9de-2016a776157b"],"questions":[{"code":"MA8_MLO_009_Q_10","time":60},{"code":"MA8_MLO_009_Q_036_FIN_IMPORT","time":60},{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_18","time":60},{"code":"MA8_MLO_009_Q_12","time":60}],"startedOn":"2020-07-23T07:06:08.648","startedBy":"ca85e5e5-9734-417d-b19e-84ce258e4869","updatedAt":"2020-07-23T07:06:20.138","status":"CANCELLED","eventDateDw":"20200723"},
              |{"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","isFormativeAssessment": false,"lessonComponentId": "component-id-1","eventType":"InClassGameSessionCreatedEvent","loadtime":"2020-07-23T07:06:09.059Z","sessionId":"a257d0d6-f81d-4963-8ebe-64ab596eSSS","joinedPlayersDetails":[{ "id": "e2d94fd4-9794-4a6f-8103-77aa77744169", "status": "JOINED_FROM_START" },{ "id": "ec150a2a-8cf2-4ccb-a069-0b6809950635", "status": "JOINED_FROM_START" }],"occurredOn":"2020-07-23 07:06:08.648","gameId":"6a6dbe43-316e-46b9-b0f6-d83f46177a42","gameTitle":"testxcg","players":["f153e9fc-7d97-4415-afe1-57f654bb6714","6b5ecd37-f23d-4527-86a9-9e4c3bf95a7f","c0f7afde-978b-4438-ab73-eda07f9a2709","6c659350-404f-4769-98ef-95928bbe1586","cf428419-3585-4c7a-84f4-632c4b83b993","ddcc7388-1e15-4d82-b683-3251da1bf9d5","b22059b5-f98a-4598-b479-b4a432ffc129","77260b3e-ba48-4cb0-a7a0-7ed759155301","602526b4-7a5d-49de-b9de-2016a776157b"],"questions":[{"code":"MA8_MLO_009_Q_10","time":60},{"code":"MA8_MLO_009_Q_036_FIN_IMPORT","time":60},{"code":"MA8_MLO_009_Q_21","time":60},{"code":"MA8_MLO_009_Q_18","time":60},{"code":"MA8_MLO_009_Q_12","time":60}],"startedOn":"2020-07-23T07:06:08.648","startedBy":"ca85e5e5-9734-417d-b19e-84ce258e4869","updatedAt":"2020-07-23T07:06:08.648","status":"OPEN","eventDateDw":"20200723"}
              |]
              |""".stripMargin
        ),
        SparkFixture(
          key = IncGameSessionDeltaStagingSource,
          value =
            """
              |[
              |{"inc_game_session_started_by_id":"ca85e5e5-9734-417d-b19e-84ce258e4869","inc_game_session_is_assessment": false,"inc_game_session_date_dw_id":"20200723","inc_game_session_is_start":true,"inc_game_session_num_players":9,"inc_game_session_game_id":"6a6dbe43-316e-46b9-b0f6-d83f46177a42","inc_game_session_title":"testxcg","inc_game_session_num_joined_players":-1,"inc_game_session_id":"a257d0d6-f81d-4963-8ebe-64ab596eSSS3","inc_game_session_status":4,"inc_game_session_tenant_id":"93e4949d-7eff-4707-9201-dac917a5e013","inc_game_session_dw_created_time":"2020-09-06T08:39:56.831Z","inc_game_session_created_time":"2020-07-23T07:06:08.648Z","inc_game_session_is_start_event_processed":true},
              |{"inc_game_session_started_by_id":"ca85e5e5-9734-417d-b19e-84ce258e4869","inc_game_session_is_assessment": false,"inc_game_session_date_dw_id":"20200723","inc_game_session_is_start":true,"inc_game_session_num_players":9,"inc_game_session_game_id":"6a6dbe43-316e-46b9-b0f6-d83f46177a42","inc_game_session_title":"testxcg","inc_game_session_num_joined_players":-1,"inc_game_session_id":"a257d0d6-f81d-4963-8ebe-64ab596SSS2","inc_game_session_status":4,"inc_game_session_tenant_id":"93e4949d-7eff-4707-9201-dac917a5e013","inc_game_session_dw_created_time":"2020-09-06T08:39:56.831Z","inc_game_session_created_time":"2020-07-23T07:06:08.648Z", "inc_game_session_is_start_event_processed":true}
              |]
              """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          sinks.length shouldBe 4

          val factCols = Seq[String](
            "inc_game_session_id",
            "inc_game_session_start_time",
            "inc_game_session_end_time",
            "inc_game_session_started_by_id",
            "inc_game_session_num_players",
            "inc_game_session_game_id",
            "inc_game_session_title",
            "inc_game_session_num_joined_players",
            "inc_game_session_tenant_id",
            "inc_game_session_dw_created_time",
            "inc_game_session_date_dw_id",
            "inc_game_session_is_start",
            "inc_game_session_status",
            "inc_game_session_time_spent",
            "inc_game_session_is_start_event_processed",
            "inc_game_session_joined_players_details",
            "inc_game_session_is_assessment",
            "eventdate"
          )

          val stageCols = Seq[String](
            "inc_game_session_started_by_id",
            "inc_game_session_date_dw_id",
            "inc_game_session_is_start",
            "inc_game_session_num_players",
            "inc_game_session_game_id",
            "inc_game_session_title",
            "inc_game_session_num_joined_players",
            "inc_game_session_id",
            "inc_game_session_status",
            "inc_game_session_tenant_id",
            "inc_game_session_dw_created_time",
            "inc_game_session_created_time",
            "inc_game_session_is_start_event_processed",
            "inc_game_session_joined_players_details",
            "inc_game_session_is_assessment"
          )

          testSinkBySinkName(sinks, DeltaIncGameSessionSink, factCols, 7)
          testSinkBySinkName(sinks, IncGameSessionDeltaStagingSource, stageCols, 2)
        }
      )
    }
  }

}
