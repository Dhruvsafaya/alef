package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.schema.pathways.ActivityProgressStatus
import com.alefeducation.util.Constants
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, explode}
import org.scalatest.matchers.should.Matchers

class PathwayProgressRawEventsTest extends SparkSuite with Matchers {
  trait Setup {
    implicit val transformer: PathwayProgressRawEvents =
      new PathwayProgressRawEvents(PathwayProgressRawEvents.name, spark)
  }

  val loadtime = "2022-09-01 16:24:37.501"
  val occurredOn = "2022-10-04 10:28:45.129"
  val tenantId = "93e4949d-7eff-4707-9201-dac917a5e013"
  val levelId = "56d39189-86bd-473a-be18-419c37535364"

  val expCommonCols: Set[String] = Set(
    "eventType",
    "tenantId",
    "learnerId",
    "classId",
    "pathwayId",
    "eventDateDw",
    "loadtime",
    "occurredOn",
    "learnerPathwayClassId",
    "uuid"
  )

  val expPathwayActivityCompletedCols: Set[String] = expCommonCols ++ Set(
    "score",
    "activityId",
    "activityType",
    "levelId",
    "timeSpent",
    "attempt",
    "learningSessionId",
    "academicYear"
  )

  val expLevelCompletedCols: Set[String] = expCommonCols ++ Set(
    "levelName",
    "totalStars",
    "completedOn",
    "adtLearningSessionId",
    "levelId",
    "academicYear",
    "score"
  )

  val expLevelsRecommendedCols: Set[String] = expCommonCols ++ Set(
    "completedLevelId",
    "recommendedLevels",
    "recommendationType",
    "recommendedOn",
    "academicYear"
  )

  test("handle PathwayActivityCompleted event") {
    val eventType = "PathwayActivityCompletedEvent"
    val fixtures = List(
      SparkFixture(
        key = PathwayProgressRawEvents.source,
        value = s"""
                   |[
                   |{
                   | "key": "key1",
                   | "value": {
                   |   "headers":{
                   |      "eventType":"$eventType",
                   |      "tenantId":"$tenantId"
                   |   },
                   |   "body":{
                   |      "learnerPathwayClassId":"fe33bf80-2a73-4917-aa6b-c62ef18931d0",
                   |      "learnerId":"0757edcf-1ea9-4f95-af70-176e98466b9a",
                   |      "classId":"0bb98aa2-18e8-47b7-a301-fc2b8866011f",
                   |      "pathwayId":"52e7b8d6-10af-47ee-bd8e-8d27e2526b65",
                   |      "levelId":"$levelId",
                   |      "activityId":"ab9f4362-8286-4270-ae35-e297c041ff9d",
                   |      "activityType":"INTERIM_CHECKPOINT",
                   |      "score":89.0,
                   |      "uuid":"6809a5b8-650d-498a-a4a1-0d64cc697fca",
                   |      "occurredOn":"$occurredOn",
                   |      "eventName":"PathwayActivityCompletedEvent",
                   |      "partitionKey":"fe33bf80-2a73-4917-aa6b-c62ef18931d0",
                   |      "academicYear": "2023-2024"
                   |   }
                   |  },
                   | "timestamp": "$loadtime"
                   |}
                   |]
                  """.stripMargin
      )
    )

    executeTest(
      fixtures,
      "learning-pathway-activity-completed-sink",
      expPathwayActivityCompletedCols,
      (_, fst) => {
        fst.getAs[String]("eventType") shouldBe eventType
        fst.getAs[Double]("score") shouldBe 89.0
        fst.getAs[String]("activityId") shouldBe "ab9f4362-8286-4270-ae35-e297c041ff9d"
        fst.getAs[String]("activityType") shouldBe "INTERIM_CHECKPOINT"
        fst.getAs[String]("levelId") shouldBe levelId
        fst.getAs[String]("academicYear") shouldBe "2023-2024"
      }
    )
  }

  test("handle LevelCompletedEvent event") {
    val eventType = "LevelCompletedEvent"
    val fixtures = List(
      SparkFixture(
        key = PathwayProgressRawEvents.source,
        value = s"""
                   |[
                   |{
                   | "key": "key1",
                   | "value": {
                   |   "headers":{
                   |      "eventType":"$eventType",
                   |      "tenantId":"$tenantId"
                   |   },
                   |   "body": {
                   |       "learnerPathwayClassId":"fe33bf80-2a73-4917-aa6b-c62ef18931d0",
                   |       "learnerId":"0757edcf-1ea9-4f95-af70-176e98466b9a",
                   |       "classId":"0bb98aa2-18e8-47b7-a301-fc2b8866011f",
                   |       "pathwayId":"52e7b8d6-10af-47ee-bd8e-8d27e2526b65",
                   |       "levelId":"$levelId",
                   |       "levelName":"Level-1",
                   |       "totalStars":8,
                   |       "completedOn":"2022-10-04T10:28:45.141313981",
                   |       "adtLearningSessionId":null,
                   |       "uuid":"a1668738-f743-46a4-9275-9e7c0c2585d7",
                   |       "occurredOn":"$occurredOn",
                   |       "eventName":"LevelCompletedEvent",
                   |       "partitionKey":"fe33bf80-2a73-4917-aa6b-c62ef18931d0",
                   |       "academicYear": "2023-2024"
                   |}
                   |  },
                   | "timestamp": "$loadtime"
                   |}
                   |]
                  """.stripMargin
      )
    )

    executeTest(
      fixtures,
      "learning-level-completed-sink",
      expLevelCompletedCols,
      (_, fst) => {
        fst.getAs[String]("eventType") shouldBe eventType
        fst.getAs[Int]("totalStars") shouldBe 8
        fst.getAs[String]("levelName") shouldBe "Level-1"
        fst.getAs[String]("completedOn") shouldBe "2022-10-04T10:28:45.141313981"
        fst.getAs[String]("adtLearningSessionId") shouldBe null
        fst.getAs[String]("levelId") shouldBe levelId
        fst.getAs[String]("academicYear") shouldBe "2023-2024"
      }
    )
  }

  test("handle LevelsRecommendedEvent event") {
    val eventType = "LevelsRecommendedEvent"
    val fixtures = List(
      SparkFixture(
        key = PathwayProgressRawEvents.source,
        value = s"""
                   |[
                   |{
                   | "key": "key1",
                   | "value": {
                   |   "headers":{
                   |      "eventType":"$eventType",
                   |      "tenantId":"$tenantId"
                   |   },
                   |   "body": {
                   |      "learnerPathwayClassId":"fe33bf80-2a73-4917-aa6b-c62ef18931d0",
                   |      "learnerId":"0757edcf-1ea9-4f95-af70-176e98466b9a",
                   |      "classId":"0bb98aa2-18e8-47b7-a301-fc2b8866011f",
                   |      "pathwayId":"52e7b8d6-10af-47ee-bd8e-8d27e2526b65",
                   |      "completedLevelId":"56d39189-86bd-473a-be18-419c37535364",
                   |      "recommendedLevels":[
                   |         {
                   |            "id":"d3588cf1-4752-43a0-b07b-9c6832cb0f84",
                   |            "name":"Level-2",
                   |            "status":"ACTIVE"
                   |         },
                   |         {
                   |            "id":"76e74a99-cd39-4575-b1bf-8fb83858e1af",
                   |            "name":"Level-3",
                   |            "status":"ACTIVE"
                   |         }
                   |      ],
                   |      "recommendedOn":"2022-10-04T10:28:45.243128681",
                   |      "recommendationType":"LEVEL_COMPLETION",
                   |      "uuid":"23f5734f-e310-4bee-b03b-2488396e4313",
                   |      "occurredOn":"$occurredOn",
                   |      "eventName":"LevelsRecommendedEvent",
                   |      "partitionKey":"fe33bf80-2a73-4917-aa6b-c62ef18931d0",
                   |      "academicYear": "2023-2024"
                   |    }
                   |  },
                   | "timestamp": "$loadtime"
                   |}
                   |]
                  """.stripMargin
      )
    )

    executeTest(
      fixtures,
      "learning-levels-recommended-sink",
      expLevelsRecommendedCols,
      (df, fst) => {
        fst.getAs[String]("eventType") shouldBe eventType
        fst.getAs[String]("completedLevelId") shouldBe "56d39189-86bd-473a-be18-419c37535364"
        fst.getAs[String]("recommendedOn") shouldBe "2022-10-04T10:28:45.243128681"
        fst.getAs[String]("recommendationType") shouldBe "LEVEL_COMPLETION"
        val rlFst = df.withColumn("rl", explode(col("recommendedLevels"))).select("rl.*").first()
        rlFst.getAs[String]("id") shouldBe "d3588cf1-4752-43a0-b07b-9c6832cb0f84"
        rlFst.getAs[String]("name") shouldBe "Level-2"
        rlFst.getAs[String]("status") shouldBe "ACTIVE"
        fst.getAs[String]("academicYear") shouldBe "2023-2024"
      }
    )
  }

  test("handle studentDomainGradeChangedEvent event") {
    val eventType = Constants.StudentDomainGradeChangedEvent
    val sink = "learning-student-domain-grade-update-sink"
    val fixtures = List(
      SparkFixture(
        key = PathwayProgressRawEvents.source,
        value = s"""
                   |{
                   |  "key" : "key1",
                   |  "timestamp": "2023-08-16 12:18:59.931",
                   |  "value" :
                   |    { "headers": { "eventType": "StudentDomainGradeChangedEvent", "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013" }, "body": { "studentId": "10361b96-cb29-414c-aacc-a596e15fbc8c", "academicYear": "2023-2024", "classId": "356b0f40-1db4-4c64-b55c-18a4213032a0", "pathwayId": "fc910047-bb7d-46bb-9ce8-5ce2a751ab9f", "currentDomainGrade": [ { "domainName": "Algebra and Algebraic Thinking", "grade": 2 }, { "domainName": "Geometry", "grade": 4 }, { "domainName": "Measurement, Data and Statistics", "grade": 3 }, { "domainName": "Numbers and Operations", "grade": 0 } ], "newDomainGrade": [ { "domainName": "Algebra and Algebraic Thinking", "grade": 2 }, { "domainName": "Geometry", "grade": 5 }, { "domainName": "Measurement, Data and Statistics", "grade": 3 }, { "domainName": "Numbers and Operations", "grade": 0 } ], "reason": "asdsf", "createdBy": "577b39c5-42f3-46c9-8587-ca39e0c86ca0", "createdOn": "2023-08-16T12:18:59.931335979", "notifyStudent": false, "notificationCustomMessage": "", "uuid": "c27fd227-f101-4f04-a07d-381fb712db43", "occurredOn": "2023-08-16T12:18:59.931" } }
                   |}
                   |""".stripMargin
      )
    )

    val expectedColumns = Set(
      "eventType",
      "eventDateDw",
      "createdOn",
      "newDomainGrade",
      "currentDomainGrade",
      "loadtime",
      "pathwayId",
      "notificationCustomMessage",
      "reason",
      "occurredOn",
      "createdBy",
      "tenantId",
      "classId",
      "notifyStudent",
      "studentId",
      "academicYear"
    )

    new Setup {
      withSparkFixtures(
        fixtures,
        sinks => {
          val df = sinks
            .find(_.name == sink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$sink is not found"))

          df.columns.toSet shouldBe expectedColumns
          val fst = df.first()
          fst.getAs[String]("pathwayId") shouldBe "fc910047-bb7d-46bb-9ce8-5ce2a751ab9f"
          fst.getAs[String]("createdBy") shouldBe "577b39c5-42f3-46c9-8587-ca39e0c86ca0"
          fst.getAs[String]("academicYear") shouldBe "2023-2024"
        }
      )
    }
  }

  test("handle studentManualPlacement event") {
    val eventType = Constants.StudentManualPlacementEvent
    val sink = "learning-student-manual-placement-sink"
    val studentId = "1c5bb814-b22a-4933-a956-519523f66978"
    val classId = "4d14293e-d253-4ad0-99a8-bb0a7e728289"
    val fixtures = List(
      SparkFixture(
        key = PathwayProgressRawEvents.source,
        value = s"""
                   |{
                   |  "key" : "key1",
                   |  "timestamp": "2023-08-16 12:18:59.931",
                   |  "value" : {
                   |	"headers": {
                   |		"eventType": "StudentManualPlacementEvent",
                   |		"tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                   |	},
                   |	"body": {
                   |		"studentId": "${studentId}",
                   |		"classId": "$classId",
                   |		"pathwayId": "e9f26394-599b-4f32-9dfa-ce72ff7fbbfa",
                   |		"domainGrades": [
                   |			{
                   |				"domainName": "Algebra and Algebraic Thinking",
                   |				"grade": 7
                   |			},
                   |			{
                   |				"domainName": "Measurement, Data and Statistics",
                   |				"grade": 7
                   |			},
                   |			{
                   |				"domainName": "Numbers and Operations",
                   |				"grade": 7
                   |			},
                   |			{
                   |				"domainName": "Geometry",
                   |				"grade": 6
                   |			}
                   |		],
                   |		"placedBy": "96049cee-81eb-4fb4-95c3-99f625dae07c",
                   |		"createdOn": "2023-08-22T10:22:28.231566524",
                   |		"overallGrade": 7,
                   |		"uuid": "7b8b2088-e17f-4895-88e9-204c1801cb75",
                   |		"occurredOn": "2023-08-22T10:22:28.329",
                   |        "academicYear": "2023-2024"
                   |	}
                   | }
                   |}
                   |""".stripMargin
      )
    )

    val expectedColumns = Set(
      "placedBy",
      "eventType",
      "eventDateDw",
      "createdOn",
      "loadtime",
      "pathwayId",
      "occurredOn",
      "overallGrade",
      "domainGrades",
      "tenantId",
      "classId",
      "studentId",
      "academicYear"
    )

    new Setup {
      withSparkFixtures(
        fixtures,
        sinks => {
          val df = sinks
            .find(_.name == sink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$sink is not found"))

          df.columns.toSet shouldBe expectedColumns
          val fst = df.first()
          fst.getAs[String]("studentId") shouldBe studentId
          fst.getAs[String]("classId") shouldBe classId
          fst.getAs[String]("occurredOn") shouldBe "2023-08-22 10:22:28.329"
          fst.getAs[String]("academicYear") shouldBe "2023-2024"
        }
      )
    }
  }

  test("handle PathwayActivitiesAssignedEvent event") {
    val eventType = Constants.PathwayActivitiesAssignedEvent
    val sink = "learning-teacher-activities-assigned-sink"
    val studentId = "1c5bb814-b22a-4933-a956-519523f66978"
    val classId = "4d14293e-d253-4ad0-99a8-bb0a7e728289"
    val fixtures = List(
      SparkFixture(
        key = PathwayProgressRawEvents.source,
        value = s"""
                   |{
                   |  "key" : "key1",
                   |  "timestamp": "2023-08-16 12:18:59.931",
                   |  "value" : {
                   |	"headers": {
                   |		"eventType": "${eventType}",
                   |		"tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                   |	},
                   |	"body": {
                   |	"studentId": "${studentId}",
                   |	"classId": "${classId}",
                   |	"pathwayId": "fc910047-bb7d-46bb-9ce8-5ce2a751ab9f",
                   |	"levelId": "852567de-a908-419a-b8cb-25714b36bf8e",
                   |	"activityIds": [
                   |		"7d79695f-1f69-446e-b005-000000028502",
                   |		"da47ee10-ca02-4727-a03f-78a780f66f24"
                   |	],
                   |	"assignedBy": "577b39c5-42f3-46c9-8587-ca39e0c86ca0",
                   |	"assignedOn": "2023-10-13T10:14:00.760403066",
                   |	"uuid": "1829511e-9bb1-470c-a1a1-41a01b0a4a96",
                   |	"occurredOn": "2023-10-13T10:14:00.797",
                   |    "activitiesProgressStatus": [
                   |        {
                   |          "activityId": "",
                   |          "activityType": "",
                   |          "progressStatus": "COMPLETED"
                   |        }
                   |     ]
                   |	}
                   | }
                   |}
                   |""".stripMargin
      )
    )

    val expectedColumns = Set(
      "eventType",
      "eventDateDw",
      "loadtime",
      "pathwayId",
      "levelId",
      "assignedBy",
      "occurredOn",
      "activityIds",
      "tenantId",
      "assignedOn",
      "classId",
      "studentId",
      "dueDate",
      "activitiesProgressStatus"
    )

    new Setup {
      withSparkFixtures(
        fixtures,
        sinks => {
          val df = sinks
            .find(_.name == sink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$sink is not found"))

          df.columns.toSet shouldBe expectedColumns
          val fst = df.first()
          fst.getAs[String]("studentId") shouldBe studentId
          fst.getAs[String]("classId") shouldBe classId
          fst.getAs[String]("occurredOn") shouldBe "2023-10-13 10:14:00.797"
        }
      )
    }
  }

  test("handle PathwaysActivityUnAssignedEvent event") {
    val eventType = Constants.PathwaysActivityUnAssignedEvent
    val sink = "learning-teacher-activity-unassigned-sink"
    val studentId = "1c5bb814-b22a-4933-a956-519523f66978"
    val classId = "4d14293e-d253-4ad0-99a8-bb0a7e728289"
    val fixtures = List(
      SparkFixture(
        key = PathwayProgressRawEvents.source,
        value = s"""
                   |{
                   |  "key" : "key1",
                   |  "timestamp": "2023-08-16 12:18:59.931",
                   |  "value" : {
                   |	"headers": {
                   |		"eventType": "${eventType}",
                   |		"tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                   |	},
                   |	"body": {
                   |	"studentId": "${studentId}",
                   |	"classId": "${classId}",
                   |
                   |	"pathwayId": "fc910047-bb7d-46bb-9ce8-5ce2a751ab9f",
                   |
                   |	"levelId": "852567de-a908-419a-b8cb-25714b36bf8e",
                   |	"activityId": "da47ee10-ca02-4727-a03f-78a780f66f24",
                   |	"unAssignedBy": "577b39c5-42f3-46c9-8587-ca39e0c86ca0",
                   |	"unAssignedOn": "2023-10-13T10:21:29.127414885",
                   |	"uuid": "8dc6102b-9259-4e50-aa60-3bc19e8f883b",
                   |	"occurredOn": "2023-10-13T10:21:29.127"
                   |	}
                   | }
                   |}
                   |""".stripMargin
      )
    )

    val expectedColumns = Set(
      "eventType",
      "eventDateDw",
      "loadtime",
      "unAssignedBy",
      "pathwayId",
      "activityId",
      "levelId",
      "occurredOn",
      "tenantId",
      "classId",
      "studentId",
      "unAssignedOn"
    )

    new Setup {
      withSparkFixtures(
        fixtures,
        sinks => {
          val df = sinks
            .find(_.name == sink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$sink is not found"))

          df.columns.toSet shouldBe expectedColumns
          val fst = df.first()
          fst.getAs[String]("studentId") shouldBe studentId
          fst.getAs[String]("classId") shouldBe classId
          fst.getAs[String]("occurredOn") shouldBe "2023-10-13 10:21:29.127"
        }
      )
    }
  }

  test("handle PlacementTestCompletedEvent event") {
    val eventType = Constants.placementTestCompletedEvent
    val sink = "learning-placement-test-completed-sink"

    val fixtures = List(
      SparkFixture(
        key = PathwayProgressRawEvents.source,
        value = s"""
                   |{
                   |"key" : "key1",
                   |"timestamp": "2023-08-16 12:18:59.931",
                   |"value": {
                   |  "body": {
                   |    "academicYear": "2024-2025",
                   |    "attemptNumber": 1,
                   |    "classId": "9bf94f97-1870-4327-86bc-af780e37261c",
                   |    "completedOn": "2024-07-25T10:44:45.755",
                   |    "learnerId": "dafd530f-5eeb-4e0e-a7c2-dd4384cd422b",
                   |    "learnerPathwayClassId": "cafd840a-086a-4267-a7dc-bf4910d4c5a9",
                   |    "learningSessionId": "59c27898-6045-4f93-ad65-8ef2ba830d9b",
                   |    "occurredOn": "2024-07-25T10:44:45.773",
                   |    "pathwayId": "f419f984-fb48-412b-baad-89d79b0a3982",
                   |    "placementTestId": "08a3a2d6-0116-4475-868b-000000028888",
                   |    "schoolId": "feaeb6d4-04af-4936-b1bb-67da0999846d",
                   |    "score": 13,
                   |    "startedOn": "2024-07-25T10:42:43.023",
                   |    "uuid": "57769eaf-c718-4ae6-b17e-815a82ace4bd"
                   |  },
                   |  "headers": {
                   |    "eventType": "PlacementTestCompletedEvent",
                   |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                   |  }
                   |}
                   |}
                   |""".stripMargin
      )
    )

    new Setup {
      withSparkFixtures(
        fixtures,
        sinks => {
          val df = sinks
            .find(_.name == sink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$sink is not found"))

          val fst = df.first()
          fst.getAs[String]("placementTestId") shouldBe "08a3a2d6-0116-4475-868b-000000028888"
          fst.getAs[String]("academicYear") shouldBe "2024-2025"
          fst.getAs[String]("learningSessionId") shouldBe "59c27898-6045-4f93-ad65-8ef2ba830d9b"
          fst.getAs[String]("learnerId") shouldBe "dafd530f-5eeb-4e0e-a7c2-dd4384cd422b"
          fst.getAs[String]("eventType") shouldBe eventType
        }
      )
    }
  }

  test("handle PlacementCompletionEvent event") {
    val eventType = Constants.placementCompletionEvent
    val sink = "learning-placement-completed-sink"

    val fixtures = List(
      SparkFixture(
        key = PathwayProgressRawEvents.source,
        value = s"""
                   |{
                   |"key" : "key1",
                   |"timestamp": "2023-08-16 12:18:59.931",
                   |"value": {
                   |  "body": {
                   |    "learnerId": "3bb00914-6525-436d-89b8-993b70b6558d",
                   |    "pathwayId": "719d9245-4ba7-4687-a859-910ec3d9b8cf",
                   |    "academicYear": "2024-2024",
                   |    "classId": "4ac450da-1d99-4663-90a0-56fb19bce379",
                   |    "recommendationType": "LEVEL_COMPLETION",
                   |    "gradeLevel": 5,
                   |    "domainGrades": [
                   |      {
                   |        "domainName": "Numbers and Operations",
                   |        "grade": 6
                   |      },
                   |      {
                   |        "domainName": "Algebra and Algebraic Thinking",
                   |        "grade": 7
                   |      },
                   |      {
                   |        "domainName": "Measurement, Data and Statistics",
                   |        "grade": 8
                   |      },
                   |      {
                   |        "domainName": "Geometry",
                   |        "grade": 1
                   |      }
                   |    ],
                   |    "recommendedLevels": [
                   |      {
                   |        "id": "7ae72f7a-5a95-464a-8114-48de4bb66645",
                   |        "name": "Level-2",
                   |        "status": "ACTIVE"
                   |      },
                   |      {
                   |        "id": "2512d24a-f36f-473b-a846-bb6c6fd0ef03",
                   |        "name": "Level-8",
                   |        "status": "ACTIVE"
                   |      },
                   |      {
                   |        "id": "43ad73d4-4eea-4853-8bc3-891495b868f3",
                   |        "name": "Level-9",
                   |        "status": "ACTIVE"
                   |      },
                   |      {
                   |        "id": "4b04835e-de1a-4a65-9bf1-e88587b27ca3",
                   |        "name": "Level-10",
                   |        "status": "UPCOMING"
                   |      },
                   |      {
                   |        "id": "9981db58-744e-4d5e-a424-76207fde1f00",
                   |        "name": "Level-11",
                   |        "status": "UPCOMING"
                   |      },
                   |      {
                   |        "id": "ae1863c2-5270-4d1e-a90c-8a1ddd4ef38c",
                   |        "name": "Level-12",
                   |        "status": "UPCOMING"
                   |      }
                   |    ],
                   |    "placedBy": null,
                   |    "isInitial": false,
                   |    "uuid": "c403efca-4b86-4907-ae13-cebac76a9312",
                   |    "hasAcceleratedDomains": false,
                   |    "occurredOn": "2024-07-25T13:09:26.727"
                   |  },
                   |  "headers": {
                   |    "eventType": "PlacementCompletionEvent",
                   |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                   |  }
                   |}
                   |}
                   |""".stripMargin
      )
    )

    new Setup {
      withSparkFixtures(
        fixtures,
        sinks => {
          val df = sinks
            .find(_.name == sink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$sink is not found"))

          val fst = df.first()
          fst.getAs[String]("recommendationType") shouldBe "LEVEL_COMPLETION"
          fst.getAs[String]("academicYear") shouldBe "2024-2024"
          fst.getAs[String]("eventType") shouldBe eventType
          fst.getAs[String]("classId") shouldBe "4ac450da-1d99-4663-90a0-56fb19bce379"
          fst.getAs[String]("pathwayId") shouldBe "719d9245-4ba7-4687-a859-910ec3d9b8cf"
          fst.getAs[Boolean]("isInitial") shouldBe false
          fst.getAs[Boolean]("hasAcceleratedDomains") shouldBe false
        }
      )
    }
  }

  test("handle AdditionalResourcesAssigned event") {
    val sink = "learning-additional-resources-assigned-sink"
    val fixtures = List(
      SparkFixture(
        key = PathwayProgressRawEvents.source,
        value = s"""
             |{
             |"key" : "key1",
             |"timestamp": "2023-08-16 12:18:59.931",
             |"value": {
             |  "body": {
             |
             |	"id": "37b93a60-6f60-41c1-9c5c-35ed91356e03",
             |	"learnerId": "85a523be-080c-40ae-97a4-3134b0ae532b",
             |	"courseId": "dc634dc2-cca6-4947-b91b-b8e7ee4a0388",
             |	"courseType": "PATHWAY",
             |	"academicYear": "2023-2024",
             |	"activities": [
             |		"3235ffc3-ca0f-4907-963e-000000104084",
             |		"dab868a3-001d-467e-8347-000000104073"
             |	],
             |	"dueDate": {
             |		"startDate": "2024-09-23",
             |		"endDate": null
             |	},
             |	"resourceInfo": [
             |        {
             |          "activityId": "3235ffc3-ca0f-4907-963e-000000104084",
             |          "activityType": "ACTIVITY",
             |          "progressStatus": "COMPLETED"
             |        }
             |  ],
             |	"assignedBy": "51dd752f-c008-459f-a097-bac731a877f5",
             |	"pathwayProgressStatus": "DIAGNOSTIC_TEST_LOCKED",
             |	"assignedOn": "2024-09-23T11:40:56.877871251",
             |	"uuid": "dc254c51-b594-4f8d-9a2e-2a78af119920",
             |    "occurredOn": "2024-07-25T13:09:26.727"
             |  },
             |  "headers": {
             |    "eventType": "AdditionalResourceAssignedEvent",
             |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
             |  }
             |}
             |}
             |""".stripMargin
      )
    )

    new Setup {
      withSparkFixtures(
        fixtures,
        sinks => {
          val df = sinks
            .find(_.name == sink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$sink is not found"))

          val fst = df.first()
          fst.getAs[String]("learnerId") shouldBe "85a523be-080c-40ae-97a4-3134b0ae532b"
          fst.getAs[String]("courseId") shouldBe "dc634dc2-cca6-4947-b91b-b8e7ee4a0388"
          fst.getAs[String]("pathwayProgressStatus") shouldBe "DIAGNOSTIC_TEST_LOCKED"
          fst.getAs[String]("uuid") shouldBe "dc254c51-b594-4f8d-9a2e-2a78af119920"
        }
      )
    }
  }

  test("handle AdditionalResourceUnAssigned event") {
    val sink = "learning-additional-resources-unassigned-sink"
    val fixtures = List(
      SparkFixture(
        key = PathwayProgressRawEvents.source,
        value = s"""
                     |{
                     |"key" : "key1",
                     |"timestamp": "2023-08-16 12:18:59.931",
                     |"value": {
                     |  "body": {
                     |	"id": "efe85b19-9faf-4004-8bbe-0072843dfc9a",
                     |	"learnerId": "4925670f-4063-4761-b095-865b9928e2c4",
                     |	"courseId": "369fb49d-5d21-481e-b034-28249294a355",
                     |	"courseType": "PATHWAY",
                     |	"academicYear": "2024-2025",
                     |	"activityId": "22385e11-7d61-4cd4-8b72-000000103879",
                     |	"unAssignedBy": "931ffd3e-c414-46b1-8687-9ecf2829bbf0",
                     |	"unAssignedOn": "2024-10-07T12:26:14.67261854",
                     |	"uuid": "d1648c73-b426-41b8-862e-8858221e4eac",
                     |	"occurredOn": "2024-10-07T12:26:14.672"
                     | },
                     |  "headers": {
                     |    "eventType": "AdditionalResourceUnAssignedEvent",
                     |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                     |  }
                     |}
                     |}
                     |""".stripMargin
      )
    )

    new Setup {
      withSparkFixtures(
        fixtures,
        sinks => {
          val df = sinks
            .find(_.name == sink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$sink is not found"))

          val fst = df.first()
          fst.getAs[String]("learnerId") shouldBe "4925670f-4063-4761-b095-865b9928e2c4"
          fst.getAs[String]("courseId") shouldBe "369fb49d-5d21-481e-b034-28249294a355"
          fst.getAs[String]("activityId") shouldBe "22385e11-7d61-4cd4-8b72-000000103879"
          fst.getAs[String]("uuid") shouldBe "d1648c73-b426-41b8-862e-8858221e4eac"
        }
      )
    }
  }

  def executeTest(fixtures: List[SparkFixture], sink: String, expCols: Set[String], f: (DataFrame, Row) => Unit): Unit = {
    new Setup {
      withSparkFixtures(
        fixtures,
        sinks => {
          val df = sinks
            .find(_.name == sink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$sink is not found"))

          df.columns.toSet shouldBe expCols

          val fst = df.first()
          fst.getAs[String]("learnerPathwayClassId") shouldBe "fe33bf80-2a73-4917-aa6b-c62ef18931d0"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("learnerId") shouldBe "0757edcf-1ea9-4f95-af70-176e98466b9a"
          fst.getAs[String]("classId") shouldBe "0bb98aa2-18e8-47b7-a301-fc2b8866011f"
          fst.getAs[String]("pathwayId") shouldBe "52e7b8d6-10af-47ee-bd8e-8d27e2526b65"
          fst.getAs[String]("occurredOn") shouldBe occurredOn
          fst.getAs[String]("loadtime") shouldBe loadtime
          fst.getAs[String]("eventDateDw") shouldBe "20221004"
          f(df, fst)
        }
      )
    }
  }
}
