package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, explode}
import org.scalatest.matchers.should.Matchers

class AllRawEventsIncGameSpec extends SparkSuite with Matchers {

  import collection.JavaConverters._

  trait Setup {
    implicit val transformer = new AllRawEvents(spark)
  }

  val expectedIncClassGameEventColumns: Set[String] = Set(
    "eventType",
    "eventDateDw",
    "teacherId",
    "genSubject",
    "schoolId",
    "loadtime",
    "mloId",
    "groupId",
    "subjectId",
    "occurredOn",
    "id",
    "schoolGradeId",
    "title",
    "tenantId",
    "questions",
    "classId",
    "sectionId",
    "learningPathId",
    "k12Grade",
    "instructionalPlanId",
    "gameId",
    "materialId",
    "materialType",
    "lessonComponentId",
    "isFormativeAssessment"
  )

  val expectedIncClassGameSessionEventColumns: Set[String] = Set(
    "startedBy",
    "eventType",
    "players",
    "eventDateDw",
    "gameId",
    "loadtime",
    "gameTitle",
    "startedOn",
    "occurredOn",
    "id",
    "status",
    "tenantId",
    "questions",
    "updatedAt",
    "joinedPlayersDetails",
    "joinedPlayers",
    "sessionId",
    "lessonComponentId",
    "isFormativeAssessment"
  )

  test("handle IncClassGameCreatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |   "headers":{
              |      "eventType":"InClassGameCreatedEvent",
              |      "tenantId":"tenantIdVal"
              |   },
              |   "body":{
              |      "id":"idVal",
              |      "gameId": "3f37b046-d00b-4fae-951a-cbe2ccb14aee",
              |      "occurredOn":"2019-08-25T09:56:31.183",
              |      "schoolId":"schoolIdVal",
              |      "classId":"classIdVal",
              |      "materialId":"materialId",
              |      "materialType":"materialType",
              |      "sectionId":"sectionIdVal",
              |      "mloId":"mloIdVal",
              |      "title":"titleVal",
              |      "teacherId":"teacherIdVal",
              |      "subjectId":"subjectIdVal",
              |      "schoolGradeId":"gradeIdVal",
              |      "genSubject":"genSubjectVal",
              |      "k12Grade":6,
              |      "groupId":"groupIdVal",
              |      "learningPathId":"learningPathIdVal",
              |      "questions":[
              |         {
              |           "code": "questionCode1",
              |           "time": 60
              |         },
              |         {
              |           "code": "questionCode2",
              |           "time": 59
              |         }
              |      ]
              |   }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "in-class-game-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("in-class-game-sink is not found"))

          val questionsDf = explodeObj(df, "questions")

          df.columns.toSet shouldBe expectedIncClassGameEventColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "InClassGameCreatedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190825"
          fst.getAs[String]("teacherId") shouldBe "teacherIdVal"
          fst.getAs[String]("genSubject") shouldBe "genSubjectVal"
          fst.getAs[String]("schoolId") shouldBe "schoolIdVal"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("mloId") shouldBe "mloIdVal"
          fst.getAs[String]("groupId") shouldBe "groupIdVal"
          fst.getAs[String]("subjectId") shouldBe "subjectIdVal"
          fst.getAs[String]("occurredOn") shouldBe "2019-08-25 09:56:31.183"
          fst.getAs[String]("id") shouldBe "idVal"
          fst.getAs[String]("schoolGradeId") shouldBe "gradeIdVal"
          fst.getAs[String]("title") shouldBe "titleVal"
          fst.getAs[String]("tenantId") shouldBe "tenantIdVal"
          fst.getAs[String]("classId") shouldBe "classIdVal"
          fst.getAs[String]("sectionId") shouldBe "sectionIdVal"
          fst.getAs[String]("materialId") shouldBe "materialId"
          fst.getAs[String]("materialType") shouldBe "materialType"
          fst.getAs[String]("learningPathId") shouldBe "learningPathIdVal"
          fst.getAs[Int]("k12Grade") shouldBe 6
          fst.getAs[String]("gameId") shouldBe "3f37b046-d00b-4fae-951a-cbe2ccb14aee"

          val questions = questionsDf.collectAsList().asScala.map(toQuestionsSetTuple).toSet
          questions shouldBe Set(("questionCode2", 59), ("questionCode1", 60))
        }
      )
    }
  }

  test("created :: sectionId and subjectId are nulls") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |   "headers":{
              |      "eventType":"InClassGameCreatedEvent",
              |      "tenantId":"tenantIdVal"
              |   },
              |   "body":{
              |      "id":"idVal",
              |      "gameId": "3f37b046-d00b-4fae-951a-cbe2ccb14aee",
              |      "occurredOn":"2019-08-25T09:56:31.183",
              |      "schoolId":"schoolIdVal",
              |      "classId":"classIdVal",
              |      "materialId":"materialId",
              |      "materialType":"materialType",
              |      "sectionId": null,
              |      "mloId":"mloIdVal",
              |      "title":"titleVal",
              |      "teacherId":"teacherIdVal",
              |      "subjectId": null,
              |      "schoolGradeId":"gradeIdVal",
              |      "genSubject":"genSubjectVal",
              |      "k12Grade":6,
              |      "groupId":"groupIdVal",
              |      "learningPathId":"learningPathIdVal",
              |      "questions":[
              |         {
              |           "code": "questionCode1",
              |           "time": 60
              |         },
              |         {
              |           "code": "questionCode2",
              |           "time": 59
              |         }
              |      ]
              |   }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "in-class-game-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("in-class-game-sink is not found"))

          val firstRow = df.first()
          firstRow.getAs[String]("eventType") shouldBe "InClassGameCreatedEvent"
          firstRow.getAs[AnyRef]("sectionId") shouldEqual null
          firstRow.getAs[AnyRef]("subjectId") shouldEqual null
        }
      )
    }
  }

  test("handle IncClassGameUpdatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |   "headers":{
              |      "eventType":"InClassGameUpdatedEvent",
              |      "tenantId":"tenantIdVal"
              |   },
              |   "body":{
              |      "id":"idVal",
              |      "gameId": "3f37b046-d00b-4fae-951a-cbe2ccb14aee",
              |      "occurredOn":"2019-08-25T09:56:31.183",
              |      "schoolId":"schoolIdVal",
              |      "classId":"classIdVal",
              |      "sectionId":"sectionIdVal",
              |      "materialId":"materialId",
              |      "materialType":"materialType",
              |      "mloId":"mloIdVal",
              |      "title":"titleVal",
              |      "teacherId":"teacherIdVal",
              |      "subjectId":"subjectIdVal",
              |      "schoolGradeId":"gradeIdVal",
              |      "genSubject":"genSubjectVal",
              |      "k12Grade":6,
              |      "groupId":"groupIdVal",
              |      "learningPathId":"learningPathIdVal",
              |      "instructionalPlanId" : "instructionalPlanId"
              |   }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "in-class-game-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("in-class-game-sink is not found"))

          df.columns.toSet shouldBe expectedIncClassGameEventColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "InClassGameUpdatedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190825"
          fst.getAs[String]("teacherId") shouldBe "teacherIdVal"
          fst.getAs[String]("genSubject") shouldBe "genSubjectVal"
          fst.getAs[String]("schoolId") shouldBe "schoolIdVal"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("mloId") shouldBe "mloIdVal"
          fst.getAs[String]("groupId") shouldBe "groupIdVal"
          fst.getAs[String]("subjectId") shouldBe "subjectIdVal"
          fst.getAs[String]("occurredOn") shouldBe "2019-08-25 09:56:31.183"
          fst.getAs[String]("id") shouldBe "idVal"
          fst.getAs[String]("schoolGradeId") shouldBe "gradeIdVal"
          fst.getAs[String]("title") shouldBe "titleVal"
          fst.getAs[String]("tenantId") shouldBe "tenantIdVal"
          fst.getAs[String]("classId") shouldBe "classIdVal"
          fst.getAs[String]("sectionId") shouldBe "sectionIdVal"
          fst.getAs[String]("materialId") shouldBe "materialId"
          fst.getAs[String]("materialType") shouldBe "materialType"
          fst.getAs[String]("learningPathId") shouldBe "learningPathIdVal"
          fst.getAs[Int]("k12Grade") shouldBe 6
          fst.getAs[AnyRef]("questions") shouldBe null
          fst.getAs[String]("gameId") shouldBe "3f37b046-d00b-4fae-951a-cbe2ccb14aee"
        }
      )
    }
  }

  test("updated :: sectionId and subjectId are nulls") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"InClassGameUpdatedEvent",
                    |      "tenantId":"tenantIdVal"
                    |   },
                    |   "body":{
                    |      "id":"idVal",
                    |      "gameId": "3f37b046-d00b-4fae-951a-cbe2ccb14aee",
                    |      "occurredOn":"2019-08-25T09:56:31.183",
                    |      "schoolId":"schoolIdVal",
                    |      "classId":"classIdVal",
                    |      "sectionId": null,
                    |      "mloId":"mloIdVal",
                    |      "title":"titleVal",
                    |      "teacherId":"teacherIdVal",
                    |      "subjectId": null,
                    |      "schoolGradeId":"gradeIdVal",
                    |      "genSubject":"genSubjectVal",
                    |      "k12Grade":6,
                    |      "groupId":"groupIdVal",
                    |      "learningPathId":"learningPathIdVal",
                    |      "instructionalPlanId" : "instructionalPlanId"
                    |   }
                    |},
                    | "timestamp": "2019-04-20 16:24:37.501"
                    |}
                    |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "in-class-game-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("in-class-game-sink is not found"))

          val firstRow = df.first()
          firstRow.getAs[String]("eventType") shouldBe "InClassGameUpdatedEvent"
          firstRow.getAs[AnyRef]("sectionId") shouldEqual null
          firstRow.getAs[AnyRef]("subjectId") shouldEqual null
        }
      )
    }
  }

  test("handle InClassGameSessionStartedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |      "headers":{
              |        "eventType":"InClassGameSessionStartedEvent",
              |        "tenantId":"tenantIdVal"
              |      },
              |      "body":{
              |        "id":"idVal",
              |        "sessionId": "a615b793-1084-4880-a3d1-98c02af5473f",
              |        "occurredOn":"2019-08-25T11:14:19.789",
              |        "gameId":"gameIdVal",
              |        "gameTitle":"gameTitleVal",
              |        "players":[
              |           "playerId1",
              |           "playerId2"
              |        ],
              |        "questions":[
              |         {
              |           "code": "questionCode1",
              |           "time": 60
              |         },
              |         {
              |           "code": "questionCode2",
              |           "time": 59
              |         }
              |        ],
              |        "joinedPlayers":[
              |           "joinedPlayerId1",
              |           "joinedPlayerId2"
              |        ],
              |        "joinedPlayersDetails": [
              |           {
              |             "id": "ee8e8099-30be-4de5-952f-e29750e9091c",
              |             "status": "JOINED_FROM_START"
              |           },
              |           {
              |             "id": "a00c225d-5df5-498d-ae24-2e6b1fa346c1",
              |             "status": "JOINED_FROM_START"
              |           }
              |        ],
              |        "startedOn":"2019-08-25T11:14:19.793",
              |        "startedBy":"teacherIdVal",
              |        "updatedAt":"2019-08-25T11:14:19.793",
              |        "status":"IN_PROGRESS"
              |      }
              |    },
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "in-class-game-session-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("in-class-game-session-sink is not found"))

          val questionsDf = explodeObj(df, "questions")
          val playersDf = explodeArr(df, "players")
          val joinedPlayersDf = explodeArr(df, "joinedPlayers")

          df.columns.toSet shouldBe expectedIncClassGameSessionEventColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "InClassGameSessionStartedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190825"
          fst.getAs[String]("startedBy") shouldBe "teacherIdVal"
          fst.getAs[String]("gameId") shouldBe "gameIdVal"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("gameTitle") shouldBe "gameTitleVal"
          fst.getAs[String]("startedOn") shouldBe "2019-08-25T11:14:19.793"
          fst.getAs[String]("occurredOn") shouldBe "2019-08-25 11:14:19.789"
          fst.getAs[String]("id") shouldBe "idVal"
          fst.getAs[String]("status") shouldBe "IN_PROGRESS"
          fst.getAs[String]("tenantId") shouldBe "tenantIdVal"
          fst.getAs[String]("updatedAt") shouldBe "2019-08-25T11:14:19.793"
          fst.getAs[String]("sessionId") shouldBe "a615b793-1084-4880-a3d1-98c02af5473f"

          val questions = questionsDf.collectAsList().asScala.map(toQuestionsSetTuple).toSet
          questions shouldBe Set(("questionCode2", 59), ("questionCode1", 60))

          playersDf.collectAsList().asScala.map(_.getAs[String](0)).toSet shouldBe Set("playerId1", "playerId2")

          joinedPlayersDf.collectAsList().asScala.map(_.getAs[String](0)).toSet shouldBe Set("joinedPlayerId1", "joinedPlayerId2")

          assertJoinedPlayersDetailes(df)
        }
      )
    }
  }

  test("handle InClassGameSessionFinishedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |      "headers":{
              |        "eventType":"InClassGameSessionFinishedEvent",
              |        "tenantId":"tenantIdVal"
              |      },
              |      "body":{
              |        "id":"idVal",
              |        "sessionId": "a615b793-1084-4880-a3d1-98c02af5473f",
              |        "occurredOn":"2019-08-25T11:14:19.789",
              |        "gameId":"gameIdVal",
              |        "gameTitle":"gameTitleVal",
              |        "players":[
              |           "playerId1",
              |           "playerId2"
              |        ],
              |        "joinedPlayersDetails": [
              |           {
              |             "id": "ee8e8099-30be-4de5-952f-e29750e9091c",
              |             "status": "JOINED_FROM_START"
              |           },
              |           {
              |             "id": "a00c225d-5df5-498d-ae24-2e6b1fa346c1",
              |             "status": "JOINED_FROM_START"
              |           }
              |        ],
              |        "questions":[
              |         {
              |           "code": "questionCode1",
              |           "time": 60
              |         },
              |         {
              |           "code": "questionCode2",
              |           "time": 59
              |         }
              |        ],
              |        "startedOn":"2019-08-25T11:14:19.793",
              |        "startedBy":"teacherIdVal",
              |        "status":"COMPLETED"
              |      }
              |    },
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "in-class-game-session-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("in-class-game-session-sink is not found"))

          val questionsDf = explodeObj(df, "questions")
          val playersDf = explodeArr(df, "players")
          val joinedPlayersDf = explodeArr(df, "joinedPlayers")

          df.columns.toSet shouldBe expectedIncClassGameSessionEventColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "InClassGameSessionFinishedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190825"
          fst.getAs[String]("startedBy") shouldBe "teacherIdVal"
          fst.getAs[String]("gameId") shouldBe "gameIdVal"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("gameTitle") shouldBe "gameTitleVal"
          fst.getAs[String]("startedOn") shouldBe "2019-08-25T11:14:19.793"
          fst.getAs[String]("occurredOn") shouldBe "2019-08-25 11:14:19.789"
          fst.getAs[String]("id") shouldBe "idVal"
          fst.getAs[String]("status") shouldBe "COMPLETED"
          fst.getAs[String]("tenantId") shouldBe "tenantIdVal"
          fst.getAs[String]("sessionId") shouldBe "a615b793-1084-4880-a3d1-98c02af5473f"

          val questions = questionsDf.collectAsList().asScala.map(toQuestionsSetTuple).toSet
          questions shouldBe Set(("questionCode2", 59), ("questionCode1", 60))

          playersDf.collectAsList().asScala.map(_.getAs[String](0)).toSet shouldBe Set("playerId1", "playerId2")
          joinedPlayersDf.collectAsList().asScala.map(_.getAs[String](0)).toSet shouldBe Set.empty[String]
          assertJoinedPlayersDetailes(df)
        }
      )
    }
  }

  test("handle InClassGameSessionCancelledEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |      "headers":{
              |        "eventType":"InClassGameSessionCancelledEvent",
              |        "tenantId":"tenantIdVal"
              |      },
              |      "body":{
              |        "id":"idVal",
              |        "sessionId": "a615b793-1084-4880-a3d1-98c02af5473f",
              |        "occurredOn":"2019-08-25T11:14:19.789",
              |        "gameId":"gameIdVal",
              |        "gameTitle":"gameTitleVal",
              |        "players":[
              |           "playerId1",
              |           "playerId2"
              |        ],
              |        "joinedPlayers":[
              |           "joinedPlayerId1",
              |           "joinedPlayerId2"
              |        ],
              |        "joinedPlayersDetails": [
              |           {
              |             "id": "ee8e8099-30be-4de5-952f-e29750e9091c",
              |             "status": "JOINED_FROM_START"
              |           },
              |           {
              |             "id": "a00c225d-5df5-498d-ae24-2e6b1fa346c1",
              |             "status": "JOINED_FROM_START"
              |           }
              |        ],
              |        "questions":[
              |         {
              |           "code": "questionCode1",
              |           "time": 60
              |         },
              |         {
              |           "code": "questionCode2",
              |           "time": 59
              |         }
              |        ],
              |        "startedOn":"2019-08-25T11:14:19.793",
              |        "startedBy":"teacherIdVal",
              |        "status":"CANCELLED"
              |      }
              |    },
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "in-class-game-session-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("in-class-game-session-sink is not found"))

          val questionsDf = explodeObj(df, "questions")
          val playersDf = explodeArr(df, "players")
          val joinedPlayersDf = explodeArr(df, "joinedPlayers")

          df.columns.toSet shouldBe expectedIncClassGameSessionEventColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "InClassGameSessionCancelledEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190825"
          fst.getAs[String]("startedBy") shouldBe "teacherIdVal"
          fst.getAs[String]("gameId") shouldBe "gameIdVal"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("gameTitle") shouldBe "gameTitleVal"
          fst.getAs[String]("startedOn") shouldBe "2019-08-25T11:14:19.793"
          fst.getAs[String]("occurredOn") shouldBe "2019-08-25 11:14:19.789"
          fst.getAs[String]("id") shouldBe "idVal"
          fst.getAs[String]("status") shouldBe "CANCELLED"
          fst.getAs[String]("tenantId") shouldBe "tenantIdVal"
          fst.getAs[String]("sessionId") shouldBe "a615b793-1084-4880-a3d1-98c02af5473f"

          val questions = questionsDf.collectAsList().asScala.map(toQuestionsSetTuple).toSet
          questions shouldBe Set(("questionCode2", 59), ("questionCode1", 60))

          playersDf.collectAsList().asScala.map(_.getAs[String](0)).toSet shouldBe Set("playerId1", "playerId2")

          joinedPlayersDf.collectAsList().asScala.map(_.getAs[String](0)).toSet shouldBe Set("joinedPlayerId1", "joinedPlayerId2")

          assertJoinedPlayersDetailes(df)
        }
      )
    }
  }

  test("handle InClassGameSessionCreatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |      "headers":{
              |        "eventType":"InClassGameSessionCreatedEvent",
              |        "tenantId":"tenantIdVal"
              |      },
              |      "body":{
              |        "id":"idVal",
              |        "sessionId": "a615b793-1084-4880-a3d1-98c02af5473f",
              |        "occurredOn":"2019-08-25T11:14:19.789",
              |        "gameId":"gameIdVal",
              |        "gameTitle":"gameTitleVal",
              |        "players":[
              |           "playerId1",
              |           "playerId2"
              |        ],
              |        "joinedPlayers":[
              |           "joinedPlayerId1",
              |           "joinedPlayerId2"
              |        ],
              |        "joinedPlayersDetails": [
              |           {
              |             "id": "ee8e8099-30be-4de5-952f-e29750e9091c",
              |             "status": "JOINED_FROM_START"
              |           },
              |           {
              |             "id": "a00c225d-5df5-498d-ae24-2e6b1fa346c1",
              |             "status": "JOINED_FROM_START"
              |           }
              |        ],
              |        "questions":[
              |         {
              |           "code": "questionCode1",
              |           "time": 60
              |         },
              |         {
              |           "code": "questionCode2",
              |           "time": 59
              |         }
              |        ],
              |        "startedOn":"2019-08-25T11:14:19.793",
              |        "startedBy":"teacherIdVal",
              |        "updatedAt": "",
              |        "status":"CREATED"
              |      }
              |    },
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "in-class-game-session-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("in-class-game-session-sink is not found"))

          val questionsDf = explodeObj(df, "questions")
          val playersDf = explodeArr(df, "players")
          val joinedPlayersDf = explodeArr(df, "joinedPlayers")

          df.columns.toSet shouldBe expectedIncClassGameSessionEventColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "InClassGameSessionCreatedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190825"
          fst.getAs[String]("startedBy") shouldBe "teacherIdVal"
          fst.getAs[String]("gameId") shouldBe "gameIdVal"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("gameTitle") shouldBe "gameTitleVal"
          fst.getAs[String]("startedOn") shouldBe "2019-08-25T11:14:19.793"
          fst.getAs[String]("occurredOn") shouldBe "2019-08-25 11:14:19.789"
          fst.getAs[String]("id") shouldBe "idVal"
          fst.getAs[String]("status") shouldBe "CREATED"
          fst.getAs[String]("tenantId") shouldBe "tenantIdVal"
          fst.getAs[String]("sessionId") shouldBe "a615b793-1084-4880-a3d1-98c02af5473f"

          val questions = questionsDf.collectAsList().asScala.map(toQuestionsSetTuple).toSet
          questions shouldBe Set(("questionCode2", 59), ("questionCode1", 60))

          playersDf.collectAsList().asScala.map(_.getAs[String](0)).toSet shouldBe Set("playerId1", "playerId2")

          joinedPlayersDf.collectAsList().asScala.map(_.getAs[String](0)).toSet shouldBe Set("joinedPlayerId1", "joinedPlayerId2")

          assertJoinedPlayersDetailes(df)
        }
      )
    }
  }

  test("handle InClassGameOutcomeEvent") {
    new Setup {
      val expectedIncClassGameOutcomeEventColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "assessmentId",
        "gameId",
        "loadtime",
        "mloId",
        "occurredOn",
        "id",
        "sessionId",
        "createdAt",
        "playerId",
        "scoreBreakdown",
        "tenantId",
        "updatedAt",
        "status",
        "score",
        "outcomeId",
        "lessonComponentId",
        "isFormativeAssessment"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |   "headers":{
              |      "eventType":"InClassGameOutcomeFinishedEvent",
              |      "tenantId":"tenantIdVal"
              |   },
              |   "body":{
              |      "id":"idVal",
              |      "outcomeId": "7d9c54dc-c8c4-4e1d-be76-277e9597d031",
              |      "occurredOn":"2019-08-25T13:16:56.969",
              |      "sessionId":"sessionIdVal",
              |      "gameId":"gameIdVal",
              |      "playerId":"playerIdVal",
              |      "mloId":"mloIdVal",
              |      "assessmentId":"assessmentIdVal",
              |      "scoreBreakdown":[
              |         {
              |            "code":"code",
              |            "score":1.0,
              |            "timeSpent":0,
              |            "createdAt":"2019-08-25T13:16:56.969",
              |            "updatedAt":"2019-08-25T13:16:56.969",
              |            "questionStatus": "CORRECT"
              |         }
              |      ],
              |      "score": 1.0,
              |      "createdAt":"2019-08-25T13:16:56.969",
              |      "updatedAt":"2019-08-25T13:16:56.969",
              |      "status":"COMPLETED"
              |   }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "in-class-game-outcome-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("in-class-game-outcome-sink is not found"))

          val scoreBreakDownDf = explodeObj(df, "scoreBreakdown")

          df.columns.toSet shouldBe expectedIncClassGameOutcomeEventColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "InClassGameOutcomeFinishedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190825"
          fst.getAs[String]("assessmentId") shouldBe "assessmentIdVal"
          fst.getAs[String]("gameId") shouldBe "gameIdVal"
          fst.getAs[String]("playerId") shouldBe "playerIdVal"
          fst.getAs[String]("createdAt") shouldBe "2019-08-25T13:16:56.969"
          fst.getAs[String]("mloId") shouldBe "mloIdVal"
          fst.getAs[String]("sessionId") shouldBe "sessionIdVal"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2019-08-25 13:16:56.969"
          fst.getAs[String]("id") shouldBe "idVal"
          fst.getAs[String]("tenantId") shouldBe "tenantIdVal"
          fst.getAs[Double]("score") shouldBe 1.0
          fst.getAs[String]("createdAt") shouldBe "2019-08-25T13:16:56.969"
          fst.getAs[String]("updatedAt") shouldBe "2019-08-25T13:16:56.969"
          fst.getAs[String]("status") shouldBe "COMPLETED"
          fst.getAs[String]("outcomeId") shouldBe "7d9c54dc-c8c4-4e1d-be76-277e9597d031"

          val fstScoreBreakDown = scoreBreakDownDf.first()
          fstScoreBreakDown.getAs[String]("code") shouldBe "code"
          fstScoreBreakDown.getAs[Double]("score") shouldBe 1.0
          fstScoreBreakDown.getAs[Int]("timeSpent") shouldBe 0
          fstScoreBreakDown.getAs[String]("createdAt") shouldBe "2019-08-25T13:16:56.969"
          fstScoreBreakDown.getAs[String]("updatedAt") shouldBe "2019-08-25T13:16:56.969"
          fstScoreBreakDown.getAs[String]("questionStatus") shouldBe "CORRECT"
        }
      )
    }
  }

  private def assertJoinedPlayersDetailes(df: DataFrame): Unit = {
    val joinedPlayersDetailsDf = explodeObj(df, "joinedPlayersDetails")
    joinedPlayersDetailsDf.collectAsList().asScala.map(toJPSetTuple).toSet shouldBe Set(
      ("ee8e8099-30be-4de5-952f-e29750e9091c", "JOINED_FROM_START"),
      ("a00c225d-5df5-498d-ae24-2e6b1fa346c1", "JOINED_FROM_START")
    )
  }

  private def explodeObj(df: DataFrame, name: String): DataFrame = {
    explodeArr(df, name).select(col("col.*"))
  }

  private def explodeArr(df: DataFrame, name: String): DataFrame = {
    df.select(explode(col(name)))
  }

  private def toQuestionsSetTuple(row: Row): (String, Int) = (row.getAs[String]("code"), row.getAs[Int]("time"))
  private def toJPSetTuple(row: Row): (String, String) = (row.getAs[String]("id"), row.getAs[String]("status"))
}