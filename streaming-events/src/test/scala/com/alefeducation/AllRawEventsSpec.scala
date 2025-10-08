package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, explode}
import org.scalatest.matchers.should.Matchers
import com.alefeducation.schema.badge._

import scala.collection.SortedSet

class AllRawEventsSpec extends SparkSuite with Matchers {

  trait Setup {
    val session = spark

    import session.implicits._

    implicit val transformer = new AllRawEvents(spark)

    def explodeArr(df: DataFrame, name: String): DataFrame = {
      df.select(explode(col(name))).select($"col.*")
    }
  }

  val expectedSchoolMutatedColumns: Set[String] = Set(
    "eventType",
    "tenantId",
    "createdOn",
    "uuid",
    "addressId",
    "addressLine",
    "addressPostBox",
    "addressCity",
    "addressCountry",
    "organisation",
    "latitude",
    "longitude",
    "timeZone",
    "firstTeachingDay",
    "name",
    "composition",
    "eventDateDw",
    "occurredOn",
    "loadtime",
    "alias",
    "sourceId",
    "organisationGlobal",
    "contentRepositoryId",
    "currentAcademicYearId",
    "contentRepositoryIds"
  )

  val expectedSectionScheduleModifiedColumns: Set[String] = Set(
    "eventType",
    "eventDateDw",
    "loadtime",
    "occurredOn",
    "slots",
    "tenantId",
    "sectionId"
  )

  test("handle kt game created events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "tenantId",
        "eventType",
        "occurredOn",
        "created",
        "updated",
        "gameSessionId",
        "learnerId",
        "subjectId",
        "subjectName",
        "subjectCode",
        "trimesterId",
        "trimesterOrder",
        "schoolId",
        "gradeId",
        "grade",
        "sectionId",
        "status",
        "gameType",
        "outsideOfSchool",
        "loadtime",
        "eventDateDw",
        "academicYearId",
        "instructionalPlanId",
        "learningPathId",
        "classId",
        "materialId",
        "materialType"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "GameSessionCreatedEvent",
              |    "tenantId": "tenantId"
              |  },
              |  "body": {
              |    "occurredOn": "2019-06-24T12:59:25.137",
              |    "created": "2019-06-24T12:59:25.134",
              |    "gameSessionId": "a7039d3e-7d3e-432e-a1c6-8320f6bda437",
              |    "learnerId": "c229af81-2f50-40fe-bf7d-35bf29bea39b",
              |    "subjectId": "042db51b-5829-4128-8330-7aba99e2baec",
              |    "subjectName": "Islamic Studies",
              |    "subjectCode": "ISLAMIC_STUDIES",
              |    "consideredLos": [
              |      {
              |        "id": "6400c25f-aa70-4463-a7e9-6051f8e53d98",
              |        "code": "IS_I006_01",
              |        "title": "الأسس الإسلامية",
              |        "framework": "FF4",
              |        "thumbnail": "/content/data/ccl/mlo/thumbnails/6f/22/68/753/IS_I006_01_thumb.jpeg",
              |        "ktCollectionId": "753",
              |        "numberOfKeyTerms": 8
              |      }
              |    ],
              |    "gameConfig": {
              |      "type": "ROCKET",
              |      "question": {
              |        "type": "MULTIPLE_CHOICE",
              |        "time": "8",
              |        "min": "6",
              |        "max": "30"
              |      }
              |    },
              |    "trimesterId": "eb8b8098-b769-4ac2-994d-4a9a2279881e",
              |    "trimesterOrder": 3,
              |    "schoolId": "8fef857b-6517-4ad4-960a-a554de803ca6",
              |    "gradeId": "8885b186-f429-4569-8d77-aa2ef13edb12",
              |    "grade": 6,
              |    "sectionId": "9c7831e8-3cc0-45fe-aaf5-a554fff2fae0",
              |    "gameType": "ROCKET",
              |    "instructionalPlanId":null,
              |    "learningPathId":null,
              |    "classId": "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312",
              |    "materialId": "materialId",
              |    "materialType": "materialType"
              |  }
              |},
              | "timestamp": "2019-04-20 16:23:46.609"
              |},
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "GameSessionStartedEvent",
              |    "tenantId": "tenantId"
              |  },
              |  "body": {
              |    "occurredOn": "2019-06-24T12:59:36",
              |    "created": "2019-06-24T12:59:25.134",
              |    "updated": "2019-06-24T12:59:36",
              |    "gameSessionId": "a7039d3e-7d3e-432e-a1c6-8320f6bda437",
              |    "learnerId": "c229af81-2f50-40fe-bf7d-35bf29bea39b",
              |    "subjectId": "042db51b-5829-4128-8330-7aba99e2baec",
              |    "subjectName": "Islamic Studies",
              |    "subjectCode": "ISLAMIC_STUDIES",
              |    "trimesterId": "eb8b8098-b769-4ac2-994d-4a9a2279881e",
              |    "trimesterOrder": 3,
              |    "schoolId": "8fef857b-6517-4ad4-960a-a554de803ca6",
              |    "gradeId": "8885b186-f429-4569-8d77-aa2ef13edb12",
              |    "grade": 6,
              |    "sectionId": "9c7831e8-3cc0-45fe-aaf5-a554fff2fae0",
              |    "status": "IN_PROGRESS",
              |    "gameType": "ROCKET",
              |    "outsideOfSchool": true,
              |    "academicYearId": "a375801a-582c-4c9d-927b-fa2ecd33f88c",
              |    "instructionalPlanId":null,
              |    "learningPathId":null,
              |    "classId": "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312",
              |    "materialId": "materialId",
              |    "materialType": "materialType"
              |  }
              |},
              | "timestamp": "2019-04-20 16:23:46.609"
              |},
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "GameQuestionSessionStartedEvent",
              |    "tenantId": "tenantId"
              |  },
              |  "body": {
              |    "occurredOn": "2019-06-24T12:59:37.246",
              |    "questionId": "84a7c7d4-4c4d-41d8-87f6-843ead34e6f5",
              |    "language": "ARABIC",
              |    "keyTermId": "0221554d-2583-4ed6-a458-c62c424cf76a",
              |    "questionType": "MULTIPLE_CHOICE",
              |    "time": 8,
              |    "learningObjectiveId": "e135f1ca-c1b5-453d-a49b-58b91bf7bed3",
              |    "status": "OPEN",
              |    "gameSessionId": "a7039d3e-7d3e-432e-a1c6-8320f6bda437",
              |    "learnerId": "c229af81-2f50-40fe-bf7d-35bf29bea39b",
              |    "subjectId": "042db51b-5829-4128-8330-7aba99e2baec",
              |    "subjectName": "Islamic Studies",
              |    "subjectCode": "ISLAMIC_STUDIES",
              |    "trimesterId": "eb8b8098-b769-4ac2-994d-4a9a2279881e",
              |    "trimesterOrder": 3,
              |    "schoolId": "8fef857b-6517-4ad4-960a-a554de803ca6",
              |    "gradeId": "8885b186-f429-4569-8d77-aa2ef13edb12",
              |    "grade": 6,
              |    "sectionId": "9c7831e8-3cc0-45fe-aaf5-a554fff2fae0",
              |    "gameType": "ROCKET",
              |    "outsideOfSchool": true,
              |    "instructionalPlanId":null,
              |    "learningPathId":null,
              |    "classId": "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312",
              |    "materialId": "materialId",
              |    "materialType": "materialType"
              |  }
              |},
              | "timestamp": "2019-04-20 16:23:46.609"
              |},
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "GameQuestionSessionFinishedEvent",
              |    "tenantId": "tenantId"
              |  },
              |  "body": {
              |    "occurredOn": "2019-06-24T12:59:37.245",
              |    "questionId": "849861d5-ed55-4875-8016-faaa642bee4f",
              |    "language": "ARABIC",
              |    "keyTermId": "c3250f71-becf-4b6c-9dc8-910c5db26bd8",
              |    "questionType": "MULTIPLE_CHOICE",
              |    "time": 8,
              |    "learningObjectiveId": "e135f1ca-c1b5-453d-a49b-58b91bf7bed3",
              |    "status": "COMPLETED",
              |    "gameSessionId": "a7039d3e-7d3e-432e-a1c6-8320f6bda437",
              |    "learnerId": "c229af81-2f50-40fe-bf7d-35bf29bea39b",
              |    "subjectId": "042db51b-5829-4128-8330-7aba99e2baec",
              |    "subjectName": "Islamic Studies",
              |    "subjectCode": "ISLAMIC_STUDIES",
              |    "trimesterId": "eb8b8098-b769-4ac2-994d-4a9a2279881e",
              |    "trimesterOrder": 3,
              |    "schoolId": "8fef857b-6517-4ad4-960a-a554de803ca6",
              |    "gradeId": "8885b186-f429-4569-8d77-aa2ef13edb12",
              |    "grade": 6,
              |    "sectionId": "9c7831e8-3cc0-45fe-aaf5-a554fff2fae0",
              |    "gameType": "ROCKET",
              |    "outsideOfSchool": true,
              |    "instructionalPlanId":null,
              |    "learningPathId":null,
              |    "classId": "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312",
              |    "materialId": "materialId",
              |    "materialType": "materialType",
              |    "scoreBreakdown": {
              |      "answer": "التحفظ هو مفهوم يعود (على الأقل) إلى البنيويين. فكرة عامة. والفكرة هي أنه يمكن تقسيم التمثيل اللغوي إلى وحدات صغيرة منفصلة يمكن بعد ذلك إعادة دمجها مع وحدات صغيرة منفصلة أخرى لإنشاء تمثيلات لغوية جديدة.",
              |      "isAttended": true,
              |      "maxScore": 1,
              |      "score": 0,
              |      "timeSpent": 0
              |    }
              |  }
              |},
              | "timestamp": "2019-04-20 16:23:46.609"
              |},
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "GameSessionFinishedEvent",
              |    "tenantId": "tenantId"
              |  },
              |  "body": {
              |    "occurredOn": "2019-06-24T13:00:23.16",
              |    "created": "2019-06-24T12:59:25.134",
              |    "updated": "2019-06-24T13:00:21.787",
              |    "gameSessionId": "a7039d3e-7d3e-432e-a1c6-8320f6bda437",
              |    "learnerId": "c229af81-2f50-40fe-bf7d-35bf29bea39b",
              |    "subjectId": "042db51b-5829-4128-8330-7aba99e2baec",
              |    "subjectName": "Islamic Studies",
              |    "subjectCode": "ISLAMIC_STUDIES",
              |    "trimesterId": "eb8b8098-b769-4ac2-994d-4a9a2279881e",
              |    "trimesterOrder": 3,
              |    "schoolId": "8fef857b-6517-4ad4-960a-a554de803ca6",
              |    "gradeId": "8885b186-f429-4569-8d77-aa2ef13edb12",
              |    "grade": 6,
              |    "sectionId": "9c7831e8-3cc0-45fe-aaf5-a554fff2fae0",
              |    "status": "IN_PROGRESS",
              |    "gameType": "ROCKET",
              |    "outsideOfSchool": true,
              |    "score": 0,
              |    "stars": 0,
              |    "instructionalPlanId":null,
              |    "learningPathId":null,
              |    "classId": "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312",
              |    "materialId": "materialId",
              |    "materialType": "materialType"
              |  }
              |},
              | "timestamp": "2019-04-20 16:23:46.609"
              |},
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "GameSessionCreationSkippedEvent",
              |    "tenantId": "tenantId"
              |  },
              |  "body": {
              |    "occurredOn": "2019-06-24T11:10:17.157",
              |    "learnerId": "aaecfd11-be02-4dba-b15c-43bc5ca4c530",
              |    "subjectId": "3541ed79-bd53-401b-8e27-24b0a9e15b7b",
              |    "subjectName": "Math",
              |    "subjectCode": "MATH",
              |    "consideredLos": [
              |      {
              |        "id": "6b1a81c2-472f-4137-b47b-2122cd96cf76",
              |        "code": "MA6_SP_MLO_01",
              |        "title": "Scalar Product 0KT",
              |        "framework": "FF4",
              |        "thumbnail": "",
              |        "ktCollectionId": null,
              |        "numberOfKeyTerms": null
              |      }
              |    ],
              |    "trimesterId": "eb8b8098-b769-4ac2-994d-4a9a2279881e",
              |    "trimesterOrder": 3,
              |    "schoolId": "8fef857b-6517-4ad4-960a-a554de803ca6",
              |    "gradeId": "8885b186-f429-4569-8d77-aa2ef13edb12",
              |    "grade": 6,
              |    "sectionId": "9c7831e8-3cc0-45fe-aaf5-a554fff2fae0",
              |    "instructionalPlanId":null,
              |    "learningPathId":null,
              |    "classId": "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312",
              |    "materialId": "materialId",
              |    "materialType": "materialType",
              |    "gameConfig": {
              |      "type": "ROCKET",
              |      "question": {
              |        "type": "MULTIPLE_CHOICE",
              |        "time": "8",
              |        "min": "6",
              |        "max": "30"
              |      }
              |    }
              |  }
              |},
              | "timestamp": "2019-04-20 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val gameSinks = sinks.filter(_.name.contains("kt-game"))
          gameSinks.size shouldBe 6

          val gameQuestionSessionStartedSinks = gameSinks.filter(_.name === "kt-game-question-session-started-sink")
          gameQuestionSessionStartedSinks.size shouldBe 1
          val gameQuestionSessionStartedDf = gameQuestionSessionStartedSinks.map(_.input).head
          gameQuestionSessionStartedDf.count shouldBe 1
          gameQuestionSessionStartedDf.first.getAs[String]("eventType") shouldBe "GameQuestionSessionStartedEvent"
          gameQuestionSessionStartedDf.first.getAs[String]("classId") shouldBe "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312"
          gameQuestionSessionStartedDf.first.getAs[String]("sectionId") shouldBe "9c7831e8-3cc0-45fe-aaf5-a554fff2fae0"
          gameQuestionSessionStartedDf.first.getAs[String]("subjectId") shouldBe "042db51b-5829-4128-8330-7aba99e2baec"

          val gameQuestionSessionFinishedSinks = gameSinks.filter(_.name === "kt-game-question-session-finished-sink")
          gameQuestionSessionFinishedSinks.size shouldBe 1
          val gameQuestionSessionFinishedDf = gameQuestionSessionFinishedSinks.map(_.input).head
          gameQuestionSessionFinishedDf.count shouldBe 1
          val scoreAnswer = gameQuestionSessionFinishedDf.select("scoreBreakdown.answer").first()
          scoreAnswer.getAs[String]("answer") contains "التحفظ هو مفهوم"
          val scoreAnswers = gameQuestionSessionFinishedDf.select("scoreBreakdown.answers").first()
          scoreAnswers.getAs[String]("answers") shouldBe null
          gameQuestionSessionFinishedDf.first.getAs[String]("eventType") shouldBe "GameQuestionSessionFinishedEvent"
          gameQuestionSessionFinishedDf.first.getAs[String]("classId") shouldBe "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312"
          gameQuestionSessionFinishedDf.first.getAs[String]("sectionId") shouldBe "9c7831e8-3cc0-45fe-aaf5-a554fff2fae0"
          gameQuestionSessionFinishedDf.first.getAs[String]("subjectId") shouldBe "042db51b-5829-4128-8330-7aba99e2baec"

          val gameSessionStartedSinks = gameSinks.filter(_.name === "kt-game-session-started-sink")
          gameSessionStartedSinks.size shouldBe 1
          gameSessionStartedSinks.flatMap(_.input.columns).toSet shouldBe expectedColumns
          val gameSessionStartedDf = gameSessionStartedSinks.map(_.input).head
          gameSessionStartedDf.count shouldBe 1
          val df = gameSessionStartedDf.first()
          df.getAs[String]("tenantId") shouldBe "tenantId"
          df.getAs[String]("eventType") shouldBe "GameSessionStartedEvent"
          df.getAs[String]("gameSessionId") shouldBe "a7039d3e-7d3e-432e-a1c6-8320f6bda437"
          df.getAs[String]("learnerId") shouldBe "c229af81-2f50-40fe-bf7d-35bf29bea39b"
          df.getAs[String]("loadtime") shouldBe "2019-04-20 16:23:46.609"
          df.getAs[String]("eventDateDw") shouldBe "20190624"
          df.getAs[String]("occurredOn") shouldBe "2019-06-24 12:59:36"
          df.getAs[String]("academicYearId") shouldBe "a375801a-582c-4c9d-927b-fa2ecd33f88c"
          df.getAs[String]("learningPathId") shouldBe null
          df.getAs[String]("instructionalPlanId") shouldBe null
          df.getAs[String]("classId") shouldBe "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312"
          df.getAs[String]("sectionId") shouldBe "9c7831e8-3cc0-45fe-aaf5-a554fff2fae0"
          df.getAs[String]("subjectId") shouldBe "042db51b-5829-4128-8330-7aba99e2baec"
        }
      )
    }
  }

  test("handle SchoolCreatedEvent") {
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
              |      "eventType":"SchoolCreatedEvent",
              |      "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |   },
              |   "body":{
              |      "createdOn":1565175907269,
              |      "uuid":"ddb082b5-12b8-4395-a68c-987227b2073a",
              |      "addressId":"360ab4f1-624a-4d6f-837f-8a854d6c1078",
              |      "addressLine":"Madinat Zayed",
              |      "addressPostBox":"213123",
              |      "addressCity":"ABU DHABI",
              |      "addressCountry":"UAE",
              |      "organisation":"MOE",
              |      "latitude":"30.123",
              |      "longitude":"43.123",
              |      "timeZone":"Asia/Dubai",
              |      "firstTeachingDay":"SUNDAY",
              |      "name":"TestSchoolAS",
              |      "composition":"MIXED",
              |      "alias":"AS",
              |      "sourceId":"schoolSourceId",
              |      "contentRepositoryIds": [
              |         "d7ea240b-e5bd-4a7c-b9ae-b97be44aee18",
              |         "a582e7fc-1bc6-4e81-9bc4-e2d03bcf96a5",
              |         "8acc30c6-9fba-4ee9-a0ad-cc4098f32a5c"
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
            .find(_.name == "admin-school-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-school-sink is not found"))

          df.columns.toSet shouldBe expectedSchoolMutatedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "SchoolCreatedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[Long]("createdOn") shouldBe 1565175907269L
          fst.getAs[String]("uuid") shouldBe "ddb082b5-12b8-4395-a68c-987227b2073a"
          fst.getAs[String]("addressId") shouldBe "360ab4f1-624a-4d6f-837f-8a854d6c1078"
          fst.getAs[String]("addressLine") shouldBe "Madinat Zayed"
          fst.getAs[String]("addressPostBox") shouldBe "213123"
          fst.getAs[String]("addressCity") shouldBe "ABU DHABI"
          fst.getAs[String]("addressCountry") shouldBe "UAE"
          fst.getAs[String]("organisation") shouldBe "MOE"

          fst.getAs[String]("latitude") shouldBe "30.123"
          fst.getAs[String]("longitude") shouldBe "43.123"
          fst.getAs[String]("timeZone") shouldBe "Asia/Dubai"
          fst.getAs[String]("firstTeachingDay") shouldBe "SUNDAY"
          fst.getAs[String]("name") shouldBe "TestSchoolAS"
          fst.getAs[String]("composition") shouldBe "MIXED"
          fst.getAs[String]("eventDateDw") shouldBe "20190807"
          fst.getAs[String]("occurredOn") shouldBe "2019-08-07 11:05:07.269"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("alias") shouldBe "AS"
          fst.getAs[String]("sourceId") shouldBe "schoolSourceId"
          fst.getAs[List[String]]("contentRepositoryIds") shouldBe List(
            "d7ea240b-e5bd-4a7c-b9ae-b97be44aee18",
            "a582e7fc-1bc6-4e81-9bc4-e2d03bcf96a5",
            "8acc30c6-9fba-4ee9-a0ad-cc4098f32a5c"
          )
        }
      )
    }
  }

  test("handle SchoolUpdatedEvent") {
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
              |      "eventType":"SchoolUpdatedEvent",
              |      "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |   },
              |   "body":{
              |      "createdOn":1565175703599,
              |      "uuid":"84a27533-7652-4b06-8a58-0745e55f6fd4",
              |      "addressId":"2dd902de-f382-4e7a-bf8f-3c04f2aa1f6c",
              |      "addressLine":"1,2345",
              |      "addressPostBox":"",
              |      "addressCity":"ABU DHABI",
              |      "addressCountry":"UAE",
              |      "organisation":"MOE",
              |      "latitude":"1.234",
              |      "longitude":"1.345",
              |      "timeZone":"Asia/Dubai",
              |      "firstTeachingDay":"SUNDAY",
              |      "name":"0 the Academic Year",
              |      "composition":"BOYS",
              |      "alias":"0AY",
              |      "sourceId":"schoolSourceId",
              |      "contentRepositoryIds": [
              |         "d7ea240b-e5bd-4a7c-b9ae-b97be44aee18",
              |         "a582e7fc-1bc6-4e81-9bc4-e2d03bcf96a5",
              |         "8acc30c6-9fba-4ee9-a0ad-cc4098f32a5c"
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
            .find(_.name == "admin-school-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-school-sink is not found"))

          df.columns.toSet shouldBe expectedSchoolMutatedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "SchoolUpdatedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[Long]("createdOn") shouldBe 1565175703599L
          fst.getAs[String]("uuid") shouldBe "84a27533-7652-4b06-8a58-0745e55f6fd4"
          fst.getAs[String]("addressId") shouldBe "2dd902de-f382-4e7a-bf8f-3c04f2aa1f6c"
          fst.getAs[String]("addressLine") shouldBe "1,2345"
          fst.getAs[String]("addressPostBox") shouldBe ""
          fst.getAs[String]("addressCity") shouldBe "ABU DHABI"
          fst.getAs[String]("addressCountry") shouldBe "UAE"
          fst.getAs[String]("organisation") shouldBe "MOE"

          fst.getAs[String]("latitude") shouldBe "1.234"
          fst.getAs[String]("longitude") shouldBe "1.345"
          fst.getAs[String]("timeZone") shouldBe "Asia/Dubai"
          fst.getAs[String]("firstTeachingDay") shouldBe "SUNDAY"
          fst.getAs[String]("name") shouldBe "0 the Academic Year"
          fst.getAs[String]("composition") shouldBe "BOYS"
          fst.getAs[String]("eventDateDw") shouldBe "20190807"
          fst.getAs[String]("occurredOn") shouldBe "2019-08-07 11:01:43.599"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("alias") shouldBe "0AY"
          fst.getAs[String]("sourceId") shouldBe "schoolSourceId"
          fst.getAs[List[String]]("contentRepositoryIds") shouldBe List(
            "d7ea240b-e5bd-4a7c-b9ae-b97be44aee18",
            "a582e7fc-1bc6-4e81-9bc4-e2d03bcf96a5",
            "8acc30c6-9fba-4ee9-a0ad-cc4098f32a5c"
          )
        }
      )
    }
  }

  test("handle SchoolDeletedEvent") {
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
              |      "eventType":"SchoolDeletedEvent",
              |      "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |   },
              |   "body":{
              |      "createdOn":1565182528360,
              |      "uuid":"707fc70f-7e7d-4b0e-8232-289f4a63f67a",
              |      "addressId":"425cb9cd-4cdc-467a-a6ef-47f4236f68b0",
              |      "addressLine":"1,2345",
              |      "addressPostBox":"213123",
              |      "addressCity":"ABU DHABI",
              |      "addressCountry":"UAE",
              |      "organisation":"MOE",
              |      "latitude":"-1",
              |      "longitude":"-3",
              |      "timeZone":"Asia/Dubai",
              |      "firstTeachingDay":"SUNDAY",
              |      "name":"TestSchoolAY11",
              |      "composition":"BOYS",
              |      "alias":"0AY11",
              |      "sourceId":"schoolSourceId",
              |      "contentRepositoryIds": [
              |         "d7ea240b-e5bd-4a7c-b9ae-b97be44aee18",
              |         "a582e7fc-1bc6-4e81-9bc4-e2d03bcf96a5",
              |         "8acc30c6-9fba-4ee9-a0ad-cc4098f32a5c"
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
            .find(_.name == "admin-school-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-school-sink is not found"))

          df.columns.toSet shouldBe expectedSchoolMutatedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "SchoolDeletedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[Long]("createdOn") shouldBe 1565182528360L
          fst.getAs[String]("uuid") shouldBe "707fc70f-7e7d-4b0e-8232-289f4a63f67a"
          fst.getAs[String]("addressId") shouldBe "425cb9cd-4cdc-467a-a6ef-47f4236f68b0"
          fst.getAs[String]("addressLine") shouldBe "1,2345"
          fst.getAs[String]("addressPostBox") shouldBe "213123"
          fst.getAs[String]("addressCity") shouldBe "ABU DHABI"
          fst.getAs[String]("addressCountry") shouldBe "UAE"
          fst.getAs[String]("organisation") shouldBe "MOE"

          fst.getAs[String]("latitude") shouldBe "-1"
          fst.getAs[String]("longitude") shouldBe "-3"
          fst.getAs[String]("timeZone") shouldBe "Asia/Dubai"
          fst.getAs[String]("firstTeachingDay") shouldBe "SUNDAY"
          fst.getAs[String]("name") shouldBe "TestSchoolAY11"
          fst.getAs[String]("composition") shouldBe "BOYS"
          fst.getAs[String]("eventDateDw") shouldBe "20190807"
          fst.getAs[String]("occurredOn") shouldBe "2019-08-07 12:55:28.360"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("alias") shouldBe "0AY11"
          fst.getAs[String]("sourceId") shouldBe "schoolSourceId"
          fst.getAs[List[String]]("contentRepositoryIds") shouldBe List(
            "d7ea240b-e5bd-4a7c-b9ae-b97be44aee18",
            "a582e7fc-1bc6-4e81-9bc4-e2d03bcf96a5",
            "8acc30c6-9fba-4ee9-a0ad-cc4098f32a5c"
          )
        }
      )
    }
  }

  test("handle SchoolActivatedEvent") {
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
                    |      "eventType":"SchoolActivatedEvent",
                    |      "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |   },
                    |   "body":{
                    |      "occurredOn":1565175703599,
                    |      "schoolId":"84a27533-7652-4b06-8a58-0745e55f6fd4"
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
            .find(_.name == "school-status-toggle-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("school-status-toggle-sink is not found"))

          df.columns.toSet shouldBe Set(
            "eventType",
            "tenantId",
            "occurredOn",
            "schoolId",
            "eventDateDw",
            "loadtime"
          )
        }
      )
    }
  }

  test("handle SchoolDeActivatedEvent") {
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
                    |      "eventType":"SchoolDeactivatedEvent",
                    |      "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |   },
                    |   "body":{
                    |      "occurredOn":1565175703599,
                    |      "schoolId":"84a27533-7652-4b06-8a58-0745e55f6fd4"
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
            .find(_.name == "school-status-toggle-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("school-status-toggle-sink is not found"))

          df.columns.toSet shouldBe Set(
            "eventType",
            "tenantId",
            "occurredOn",
            "schoolId",
            "eventDateDw",
            "loadtime"
          )
        }
      )
    }
  }

  test("handle NoMloForSkillsFound") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "recommendedSkillsWithNoMlo",
        "tenantId",
        "learningObjectiveId",
        "learningObjectiveCode",
        "learningObjectiveTitle",
        "occurredOn",
        "loadtime"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "NoMloForSkillsFound",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |    "occurredOn": 1555777477000,
              |    "learningObjectiveId": "02175a5e-6be7-4b00-9450-bb806d7eda38",
              |    "learningObjectiveCode": "LO-Code",
              |    "learningObjectiveTitle": "LO-Title",
              |    "recommendedSkillsWithNoMlo": [
              |      {
              |        "uuid": "924095a3-a0ce-4cf1-8e7e-604dbd901fb2",
              |        "code": "sk.m151",
              |        "name": "Find the area and perimeter of polygons on a coordinate plane"
              |      },     {
              |        "uuid": "095a3924-a0ce-4cf1-8e7e-604dbd901fb3",
              |        "code": "sk.m152",
              |        "name": "Find the area and perimeter of rhombus on a coordinate plane"
              |      }
              |    ]
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "no-skill-content-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("no-skill-content-sink is not found"))

          val skillsDf = explodeArr(df, "recommendedSkillsWithNoMlo")

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "NoMloForSkillsFound"
          fst.getAs[String]("eventDateDw") shouldBe "20190420"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2019-04-20 16:24:37.000"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("learningObjectiveId") shouldBe "02175a5e-6be7-4b00-9450-bb806d7eda38"

          val fstSkillWithNoMLO = skillsDf.first()
          fstSkillWithNoMLO.getAs[String]("uuid") shouldBe "924095a3-a0ce-4cf1-8e7e-604dbd901fb2"
          fstSkillWithNoMLO.getAs[String]("code") shouldBe "sk.m151"
          fstSkillWithNoMLO.getAs[String]("name") shouldBe "Find the area and perimeter of polygons on a coordinate plane"

          val secondSkillWithNoMLO = skillsDf.filter(col("code") === "sk.m152").first()
          secondSkillWithNoMLO.getAs[String]("uuid") shouldBe "095a3924-a0ce-4cf1-8e7e-604dbd901fb3"
          secondSkillWithNoMLO.getAs[String]("code") shouldBe "sk.m152"
          secondSkillWithNoMLO.getAs[String]("name") shouldBe "Find the area and perimeter of rhombus on a coordinate plane"
        }
      )
    }
  }

  test("handle NoMloForSkillsFound when occurredOn is not present") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "recommendedSkillsWithNoMlo",
        "tenantId",
        "learningObjectiveId",
        "learningObjectiveCode",
        "learningObjectiveTitle",
        "occurredOn",
        "loadtime"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |  "headers": {
                    |    "eventType": "NoMloForSkillsFound",
                    |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                    |  },
                    |  "body": {
                    |    "learningObjectiveId": "02175a5e-6be7-4b00-9450-bb806d7eda38",
                    |    "learningObjectiveCode": "LO-Code",
                    |    "learningObjectiveTitle": "LO-Title",
                    |    "recommendedSkillsWithNoMlo": [
                    |      {
                    |        "uuid": "924095a3-a0ce-4cf1-8e7e-604dbd901fb2",
                    |        "code": "sk.m151",
                    |        "name": "Find the area and perimeter of polygons on a coordinate plane"
                    |      },     {
                    |        "uuid": "095a3924-a0ce-4cf1-8e7e-604dbd901fb3",
                    |        "code": "sk.m152",
                    |        "name": "Find the area and perimeter of rhombus on a coordinate plane"
                    |      }
                    |    ]
                    |  }
                    |},
                    | "timestamp": "2019-04-20 16:24:37.501"
                    |}
                    |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "no-skill-content-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("no-skill-content-sink is not found"))

          val skillsDf = explodeArr(df, "recommendedSkillsWithNoMlo")

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "NoMloForSkillsFound"
          fst.getAs[String]("eventDateDw") shouldBe "19700101"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "1970-01-01 00:00:00.000"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("learningObjectiveId") shouldBe "02175a5e-6be7-4b00-9450-bb806d7eda38"
        }
      )
    }
  }

  test("handle lesson feedback submitted/cancelled events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "curriculumGradeId", "eventType", "eventDateDw", "ratingText", "learnerId", "learningPathId", "teachingPeriodId", "teachingPeriodTitle", "schoolId", "loadtime", "academicYearId", "section", "instructionalPlanId", "subjectId", "occurredOn", "trimesterId", "contentAcademicYear", "rating", "learningSessionId", "trimesterOrder", "comment", "teachingPeriodId", "curriculumSubjectId", "tenantId", "teachingPeriodTitle", "classId", "gradeId", "learningObjectiveId", "curriculumId", "feedbackId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "LessonFeedbackSubmitted",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |    "feedbackId":"feedback-id-1",
              |    "schoolId":"school-id-1",
              |    "academicYearId":"ay-id-1",
              |    "gradeId":"grade-id-1",
              |    "section":"section-id-1",
              |    "subjectId":"subject-id-1",
              |    "learnerId":"learner-id-1",
              |    "learningSessionId":"ls-id-1",
              |    "learningObjectiveId":"lo-id-1",
              |    "trimesterId":"trimester-id-1",
              |    "trimesterOrder":1,
              |    "curriculumId":"1234",
              |    "curriculumSubjectId":"4567",
              |    "curriculumGradeId":"3456",
              |    "contentAcademicYear":"2020",
              |    "rating":3,
              |    "ratingText":"Neutral",
              |    "comment":"Good",
              |    "learningPathId":"lp1",
              |    "teachingPeriodId": "teaching-period-id-1",
              |    "teachingPeriodTitle": "teaching-period-title-1",
              |    "instructionalPlanId":"ip1",
              |    "occurredOn":1574252447190
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |},
              |{
              | "key": "key2",
              | "value":{
              |  "headers": {
              |    "eventType": "LessonFeedbackCancelled",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |    "feedbackId":"feedback-id-1",
              |    "schoolId":"school-id-1",
              |    "academicYearId":"ay-id-1",
              |    "gradeId":"grade-id-1",
              |    "section":"section-id-1",
              |    "subjectId":"subject-id-1",
              |    "learnerId":"learner-id-1",
              |    "learningSessionId":"ls-id-1",
              |    "learningObjectiveId":"lo-id-1",
              |    "trimesterId":"trimester-id-1",
              |    "trimesterOrder":1,
              |    "curriculumId":"1234",
              |    "curriculumSubjectId":"4356",
              |    "curriculumGradeId":"567",
              |    "contentAcademicYear":"2020",
              |    "learningPathId":"lp1",
              |    "teachingPeriodId": "teaching-period-id-1",
              |    "teachingPeriodTitle": "teaching-period-title-1",
              |    "instructionalPlanId":"ip1",
              |    "occurredOn":1574252447191
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.502"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val feedbackDf: DataFrame = sinks
            .find(_.name == "lesson-feedback-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("lesson-feedback-sink is not found"))

          feedbackDf.columns.toSet shouldBe expectedColumns

          val feedbackSubmitted = feedbackDf.filter("eventType = 'LessonFeedbackSubmitted'").first()
          feedbackSubmitted.getAs[String]("eventDateDw") shouldBe "20191120"
          feedbackSubmitted.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          feedbackSubmitted.getAs[String]("occurredOn") shouldBe "2019-11-20 12:20:47.190"
          feedbackSubmitted.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          feedbackSubmitted.getAs[String]("feedbackId") shouldBe "feedback-id-1"
          feedbackSubmitted.getAs[String]("schoolId") shouldBe "school-id-1"
          feedbackSubmitted.getAs[String]("academicYearId") shouldBe "ay-id-1"
          feedbackSubmitted.getAs[String]("gradeId") shouldBe "grade-id-1"
          feedbackSubmitted.getAs[String]("section") shouldBe "section-id-1"
          feedbackSubmitted.getAs[String]("subjectId") shouldBe "subject-id-1"
          feedbackSubmitted.getAs[String]("learnerId") shouldBe "learner-id-1"
          feedbackSubmitted.getAs[String]("learningSessionId") shouldBe "ls-id-1"
          feedbackSubmitted.getAs[String]("learningObjectiveId") shouldBe "lo-id-1"
          feedbackSubmitted.getAs[String]("trimesterId") shouldBe "trimester-id-1"
          feedbackSubmitted.getAs[Int]("trimesterOrder") shouldBe 1
          feedbackSubmitted.getAs[String]("curriculumId") shouldBe "1234"
          feedbackSubmitted.getAs[String]("curriculumSubjectId") shouldBe "4567"
          feedbackSubmitted.getAs[String]("curriculumGradeId") shouldBe "3456"
          feedbackSubmitted.getAs[String]("contentAcademicYear") shouldBe "2020"
          feedbackSubmitted.getAs[Int]("rating") shouldBe 3
          feedbackSubmitted.getAs[String]("ratingText") shouldBe "Neutral"
          feedbackSubmitted.getAs[String]("comment") shouldBe "Good"
          feedbackSubmitted.getAs[String]("learningPathId") shouldBe "lp1"
          feedbackSubmitted.getAs[String]("instructionalPlanId") shouldBe "ip1"

          val feedbackCancelled = feedbackDf.filter("eventType = 'LessonFeedbackCancelled'").first()
          feedbackCancelled.getAs[String]("eventDateDw") shouldBe "20191120"
          feedbackCancelled.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.502"
          feedbackCancelled.getAs[String]("occurredOn") shouldBe "2019-11-20 12:20:47.191"
          feedbackCancelled.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          feedbackCancelled.getAs[String]("feedbackId") shouldBe "feedback-id-1"
          feedbackCancelled.getAs[String]("schoolId") shouldBe "school-id-1"
          feedbackCancelled.getAs[String]("academicYearId") shouldBe "ay-id-1"
          feedbackCancelled.getAs[String]("gradeId") shouldBe "grade-id-1"
          feedbackCancelled.getAs[String]("section") shouldBe "section-id-1"
          feedbackCancelled.getAs[String]("subjectId") shouldBe "subject-id-1"
          feedbackCancelled.getAs[String]("learnerId") shouldBe "learner-id-1"
          feedbackCancelled.getAs[String]("learningSessionId") shouldBe "ls-id-1"
          feedbackCancelled.getAs[String]("learningObjectiveId") shouldBe "lo-id-1"
          feedbackCancelled.getAs[String]("trimesterId") shouldBe "trimester-id-1"
          feedbackCancelled.getAs[Int]("trimesterOrder") shouldBe 1
          feedbackCancelled.getAs[String]("curriculumId") shouldBe "1234"
          feedbackCancelled.getAs[String]("curriculumSubjectId") shouldBe "4356"
          feedbackCancelled.getAs[String]("curriculumGradeId") shouldBe "567"
          feedbackCancelled.getAs[String]("contentAcademicYear") shouldBe "2020"
          feedbackCancelled.getAs[String]("ratingText") shouldBe null
          feedbackCancelled.getAs[String]("comment") shouldBe null
          feedbackSubmitted.getAs[String]("learningPathId") shouldBe "lp1"
          feedbackSubmitted.getAs[String]("instructionalPlanId") shouldBe "ip1"
        }
      )
    }
  }

  test("handle SectionCreatedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "schoolId",
        "tenantId",
        "gradeName",
        "sourceId",
        "gradeId",
        "occurredOn",
        "loadtime",
        "uuid",
        "enabled",
        "section",
        "name",
        "createdOn"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "SectionCreatedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "uuid":"8bd54419-32f8-48b1-abc8-4f754a56afc5",
              |     "createdOn":1575965594444,
              |     "enabled":true,
              |     "schoolId":"33a4dded-4e14-4d7b-90ef-0ecab66d5410",
              |     "gradeId":"166b68fa-975c-4c8e-b76f-7a0cfd63e337",
              |     "gradeName":"7",
              |     "name":"2",
              |     "section":"C1",
              |     "academicYearId":"33a4dded-4e14-4d7b-90ef-0ecab66d5410",
              |     "rolledOver":false,
              |     "sourceId":"sourceIdVal"
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-section-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-section-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "SectionCreatedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 08:13:14.444"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("uuid") shouldBe "8bd54419-32f8-48b1-abc8-4f754a56afc5"
          fst.getAs[Boolean]("enabled") shouldBe true
          fst.getAs[String]("schoolId") shouldBe "33a4dded-4e14-4d7b-90ef-0ecab66d5410"
          fst.getAs[String]("gradeId") shouldBe "166b68fa-975c-4c8e-b76f-7a0cfd63e337"
          fst.getAs[String]("gradeName") shouldBe "7"
          fst.getAs[String]("name") shouldBe "2"
          fst.getAs[String]("section") shouldBe "C1"
          fst.getAs[String]("sourceId") shouldBe "sourceIdVal"
        }
      )
    }
  }

  test("handle SectionUpdatedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "schoolId",
        "tenantId",
        "gradeName",
        "sourceId",
        "gradeId",
        "occurredOn",
        "loadtime",
        "uuid",
        "enabled",
        "section",
        "name",
        "createdOn"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "SectionUpdatedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "uuid":"358fef02-c67a-4c45-8976-820020b81f8b",
              |     "createdOn":1575965540631,
              |     "enabled":true,
              |     "schoolId":"33a4dded-4e14-4d7b-90ef-0ecab66d5410",
              |     "gradeId":"166b68fa-975c-4c8e-b76f-7a0cfd63e337",
              |     "gradeName":"7",
              |     "name":"1",
              |     "section":"C3",
              |     "sourceId":"sourceIdVal"
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
          val df: DataFrame = sinks
            .find(_.name == "admin-section-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-section-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "SectionUpdatedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 08:12:20.631"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("uuid") shouldBe "358fef02-c67a-4c45-8976-820020b81f8b"
          fst.getAs[Boolean]("enabled") shouldBe true
          fst.getAs[String]("schoolId") shouldBe "33a4dded-4e14-4d7b-90ef-0ecab66d5410"
          fst.getAs[String]("gradeId") shouldBe "166b68fa-975c-4c8e-b76f-7a0cfd63e337"
          fst.getAs[String]("gradeName") shouldBe "7"
          fst.getAs[String]("name") shouldBe "1"
          fst.getAs[String]("section") shouldBe "C3"
          fst.getAs[String]("sourceId") shouldBe "sourceIdVal"
        }
      )
    }
  }

  test("handle SectionDeletedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "createdOn",
        "loadtime",
        "uuid",
        "occurredOn",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "SectionDeletedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "uuid":"8bd54419-32f8-48b1-abc8-4f754a56afc5",
              |     "createdOn":1575965594444
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-section-state-changed-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-section-state-changed-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "SectionDeletedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 08:13:14.444"
        }
      )
    }
  }

  test("handle SectionEnabledEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "createdOn",
        "loadtime",
        "uuid",
        "occurredOn",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "SectionEnabledEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "uuid":"8bd54419-32f8-48b1-abc8-4f754a56afc5",
              |     "createdOn":1575965594444
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-section-state-changed-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-section-state-changed-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "SectionEnabledEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 08:13:14.444"
        }
      )
    }
  }

  test("handle SectionDisabledEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "createdOn",
        "loadtime",
        "uuid",
        "occurredOn",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "SectionDisabledEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "uuid":"8bd54419-32f8-48b1-abc8-4f754a56afc5",
              |     "createdOn":1575965594444
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-section-state-changed-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-section-state-changed-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "SectionDisabledEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 08:13:14.444"
        }
      )
    }
  }

  test("handle StudentSectionUpdatedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "newSectionId",
        "eventDateDw",
        "schoolId",
        "loadtime",
        "occurredOn",
        "id",
        "grade",
        "oldSectionId",
        "tenantId",
        "gradeId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "StudentSectionUpdatedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "id":"8bd54419-32f8-48b1-abc8-4f754a56afc5",
              |     "occurredOn":1575965594444,
              |     "grade": "6G",
              |     "gradeId": "6c56d3b2-89b9-4c01-8f6f-614fa907151b",
              |     "schoolId": "026d37c7-8aef-49e3-a1fa-fb8e1fc38a3a",
              |     "oldSectionId": "c4740a3e-a7be-44cf-ad25-2e255c06edfd",
              |     "newSectionId": "cf39c65a-3bf9-4389-a108-f2a6f0e8a789"
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-student-section-updated-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-student-section-updated-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "StudentSectionUpdatedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("grade") shouldBe "6G"
          fst.getAs[String]("gradeId") shouldBe "6c56d3b2-89b9-4c01-8f6f-614fa907151b"
          fst.getAs[String]("schoolId") shouldBe "026d37c7-8aef-49e3-a1fa-fb8e1fc38a3a"
          fst.getAs[String]("oldSectionId") shouldBe "c4740a3e-a7be-44cf-ad25-2e255c06edfd"
          fst.getAs[String]("newSectionId") shouldBe "cf39c65a-3bf9-4389-a108-f2a6f0e8a789"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 08:13:14.444"
        }
      )
    }
  }

  test("handle TeacherCreatedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "schoolId",
        "tenantId",
        "occurredOn",
        "loadtime",
        "uuid",
        "enabled",
        "createdOn"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "TeacherCreatedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "uuid":"66b39648-39ae-4d8c-acac-087da60152cf",
              |     "createdOn":1575968797880,
              |     "enabled":true,
              |     "schoolId":"33a4dded-4e14-4d7b-90ef-0ecab66d5410"
              | }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-teacher-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-teacher-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "TeacherCreatedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 09:06:37.880"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("uuid") shouldBe "66b39648-39ae-4d8c-acac-087da60152cf"
          fst.getAs[Boolean]("enabled") shouldBe true
          fst.getAs[String]("schoolId") shouldBe "33a4dded-4e14-4d7b-90ef-0ecab66d5410"
        }
      )
    }
  }

  test("handle TeacherUpdatedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "schoolId",
        "tenantId",
        "occurredOn",
        "loadtime",
        "uuid",
        "enabled",
        "createdOn"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "TeacherUpdatedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "uuid":"66b39648-39ae-4d8c-acac-087da60152cf",
              |     "createdOn":1575969622457,
              |     "enabled":true,
              |     "schoolId":"33a4dded-4e14-4d7b-90ef-0ecab66d5410"
              | }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-teacher-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-teacher-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "TeacherUpdatedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 09:20:22.457"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("uuid") shouldBe "66b39648-39ae-4d8c-acac-087da60152cf"
          fst.getAs[Boolean]("enabled") shouldBe true
          fst.getAs[String]("schoolId") shouldBe "33a4dded-4e14-4d7b-90ef-0ecab66d5410"
        }
      )
    }
  }

  test("handle StudentMovedBetweenGrades") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "occurredOn",
        "studentId",
        "oldSchoolId",
        "oldSectionId",
        "oldGradeId",
        "oldK12Grade",
        "targetSchoolId",
        "targetGradeId",
        "targetK12Grade",
        "eventDateDw",
        "loadtime",
        "eventType",
        "tenantId",
        "targetSectionId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "StudentMovedBetweenGrades",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "occurredOn": 1575969622457,
              |     "studentId": "98e5e2ef-1d2f-4e42-855c-e0074669ae1c",
              |     "oldSchoolId": "d2ca995f-3658-4c9f-89da-c5071dcb9f48",
              |     "oldSectionId": "608c3a3c-06c1-4463-bd25-24b0d6d7da08",
              |     "oldGradeId": "a8a4212d-374b-4fe5-ab05-a665353e2456",
              |     "oldK12Grade": 5,
              |     "targetSchoolId": "969c8b83-9007-4015-833f-ad5a1755c939",
              |     "targetSectionId": "66816255-9dea-488b-bb39-4be77461237a",
              |     "targetGradeId": "48da3c6e-1ddf-41ab-a23c-39f05d1761a6",
              |     "targetK12Grade": 6
              | }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-student-school-grade-move-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-student-school-grade-move-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "StudentMovedBetweenGrades"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 09:20:22.457"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("studentId") shouldBe "98e5e2ef-1d2f-4e42-855c-e0074669ae1c"
          fst.getAs[String]("oldSchoolId") shouldBe "d2ca995f-3658-4c9f-89da-c5071dcb9f48"
          fst.getAs[String]("oldSectionId") shouldBe "608c3a3c-06c1-4463-bd25-24b0d6d7da08"
          fst.getAs[String]("oldGradeId") shouldBe "a8a4212d-374b-4fe5-ab05-a665353e2456"
          fst.getAs[Long]("oldK12Grade") shouldBe 5
          fst.getAs[String]("targetSchoolId") shouldBe "969c8b83-9007-4015-833f-ad5a1755c939"
          fst.getAs[String]("targetSectionId") shouldBe "66816255-9dea-488b-bb39-4be77461237a"
          fst.getAs[String]("targetGradeId") shouldBe "48da3c6e-1ddf-41ab-a23c-39f05d1761a6"
          fst.getAs[Long]("targetK12Grade") shouldBe 6
        }
      )
    }
  }

  test("handle StudentMovedBetweenSchools") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "occurredOn",
        "studentId",
        "oldSchoolId",
        "oldSectionId",
        "oldGradeId",
        "oldK12Grade",
        "targetSchoolId",
        "targetGradeId",
        "targetK12Grade",
        "eventDateDw",
        "loadtime",
        "eventType",
        "tenantId",
        "targetSectionId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "StudentMovedBetweenSchools",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "occurredOn": 1575969622457,
              |     "studentId": "98e5e2ef-1d2f-4e42-855c-e0074669ae1c",
              |     "oldSchoolId": "d2ca995f-3658-4c9f-89da-c5071dcb9f48",
              |     "oldSectionId": "608c3a3c-06c1-4463-bd25-24b0d6d7da08",
              |     "oldGradeId": "a8a4212d-374b-4fe5-ab05-a665353e2456",
              |     "oldK12Grade": 5,
              |     "targetSchoolId": "969c8b83-9007-4015-833f-ad5a1755c939",
              |     "targetSectionId": "66816255-9dea-488b-bb39-4be77461237a",
              |     "targetGradeId": "48da3c6e-1ddf-41ab-a23c-39f05d1761a6",
              |     "targetK12Grade": 6
              | }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-student-school-grade-move-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-student-school-grade-move-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "StudentMovedBetweenSchools"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 09:20:22.457"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("studentId") shouldBe "98e5e2ef-1d2f-4e42-855c-e0074669ae1c"
          fst.getAs[String]("oldSchoolId") shouldBe "d2ca995f-3658-4c9f-89da-c5071dcb9f48"
          fst.getAs[String]("oldSectionId") shouldBe "608c3a3c-06c1-4463-bd25-24b0d6d7da08"
          fst.getAs[String]("oldGradeId") shouldBe "a8a4212d-374b-4fe5-ab05-a665353e2456"
          fst.getAs[Long]("oldK12Grade") shouldBe 5
          fst.getAs[String]("targetSchoolId") shouldBe "969c8b83-9007-4015-833f-ad5a1755c939"
          fst.getAs[String]("targetSectionId") shouldBe "66816255-9dea-488b-bb39-4be77461237a"
          fst.getAs[String]("targetGradeId") shouldBe "48da3c6e-1ddf-41ab-a23c-39f05d1761a6"
          fst.getAs[Long]("targetK12Grade") shouldBe 6
        }
      )
    }
  }

  test("handle StudentEnabledEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "createdOn",
        "username",
        "schoolId",
        "loadtime",
        "uuid",
        "occurredOn",
        "sectionId",
        "tenantId",
        "gradeId",
        "k12Grade"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "StudentEnabledEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "uuid":"8bd54419-32f8-48b1-abc8-4f754a56afc5",
              |     "createdOn":1575965594444,
              |     "username": "usernameVal",
              |     "k12Grade": 5,
              |     "gradeId": "6c56d3b2-89b9-4c01-8f6f-614fa907151b",
              |     "schoolId": "026d37c7-8aef-49e3-a1fa-fb8e1fc38a3a",
              |     "sectionId": "cf39c65a-3bf9-4389-a108-f2a6f0e8a789"
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-student-status-toggle-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-student-status-toggle-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "StudentEnabledEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("username") shouldBe "usernameVal"
          fst.getAs[String]("gradeId") shouldBe "6c56d3b2-89b9-4c01-8f6f-614fa907151b"
          fst.getAs[String]("schoolId") shouldBe "026d37c7-8aef-49e3-a1fa-fb8e1fc38a3a"
          fst.getAs[String]("gradeId") shouldBe "6c56d3b2-89b9-4c01-8f6f-614fa907151b"
          fst.getAs[String]("sectionId") shouldBe "cf39c65a-3bf9-4389-a108-f2a6f0e8a789"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 08:13:14.444"
        }
      )
    }
  }

  test("handle StudentDisabledEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "createdOn",
        "username",
        "schoolId",
        "loadtime",
        "uuid",
        "occurredOn",
        "sectionId",
        "tenantId",
        "gradeId",
        "k12Grade"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "StudentDisabledEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "uuid":"8bd54419-32f8-48b1-abc8-4f754a56afc5",
              |     "createdOn":1575965594444,
              |     "username": "usernameVal",
              |     "k12Grade": 5,
              |     "gradeId": "6c56d3b2-89b9-4c01-8f6f-614fa907151b",
              |     "schoolId": "026d37c7-8aef-49e3-a1fa-fb8e1fc38a3a",
              |     "sectionId": "cf39c65a-3bf9-4389-a108-f2a6f0e8a789"
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-student-status-toggle-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-student-status-toggle-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "StudentDisabledEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("username") shouldBe "usernameVal"
          fst.getAs[String]("gradeId") shouldBe "6c56d3b2-89b9-4c01-8f6f-614fa907151b"
          fst.getAs[String]("schoolId") shouldBe "026d37c7-8aef-49e3-a1fa-fb8e1fc38a3a"
          fst.getAs[String]("gradeId") shouldBe "6c56d3b2-89b9-4c01-8f6f-614fa907151b"
          fst.getAs[String]("sectionId") shouldBe "cf39c65a-3bf9-4389-a108-f2a6f0e8a789"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 08:13:14.444"
        }
      )
    }
  }

  test("handle StudentPromotedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "rolledOverSectionId",
        "tenantId",
        "rolledOverGradeId",
        "studentId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "StudentPromotedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "occurredOn":1575965594444,
              |     "rolledOverGradeId": "6c56d3b2-89b9-4c01-8f6f-614fa907151b",
              |     "rolledOverSectionId": "026d37c7-8aef-49e3-a1fa-fb8e1fc38a3a",
              |     "studentId": "cf39c65a-3bf9-4389-a108-f2a6f0e8a789"
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-student-promoted-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-student-promoted-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "StudentPromotedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("rolledOverGradeId") shouldBe "6c56d3b2-89b9-4c01-8f6f-614fa907151b"
          fst.getAs[String]("rolledOverSectionId") shouldBe "026d37c7-8aef-49e3-a1fa-fb8e1fc38a3a"
          fst.getAs[String]("studentId") shouldBe "cf39c65a-3bf9-4389-a108-f2a6f0e8a789"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 08:13:14.444"
        }
      )
    }
  }

  test("handle StudentCreatedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "createdOn",
        "username",
        "schoolId",
        "tags",
        "loadtime",
        "uuid",
        "occurredOn",
        "grade",
        "sectionId",
        "tenantId",
        "gradeId",
        "specialNeeds"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "StudentCreatedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "createdOn":1575965594444,
              |     "uuid": "6c56d3b2-89b9-4c01-8f6f-614fa907151b",
              |     "username": "efde315b-6697-4389-b260-038c8689b4ca",
              |     "grade": "grade1",
              |     "gradeId": "026d37c7-8aef-49e3-a1fa-fb8e1fc38a3a",
              |     "sectionId": "cf39c65a-3bf9-4389-a108-f2a6f0e8a789",
              |     "schoolId": "e64bf79a-830b-415c-80db-d179c4e2307c",
              |     "tags": [
              |       "one tag",
              |       "two tag"
              |     ]
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-student-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-student-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "StudentCreatedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("username") shouldBe "efde315b-6697-4389-b260-038c8689b4ca"
          fst.getAs[String]("grade") shouldBe "grade1"
          fst.getAs[String]("gradeId") shouldBe "026d37c7-8aef-49e3-a1fa-fb8e1fc38a3a"

          fst.getAs[String]("sectionId") shouldBe "cf39c65a-3bf9-4389-a108-f2a6f0e8a789"
          fst.getAs[String]("schoolId") shouldBe "e64bf79a-830b-415c-80db-d179c4e2307c"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 08:13:14.444"

          fst.getAs[Array[String]]("tags") shouldBe Array("one tag", "two tag")
        }
      )
    }
  }

  test("handle StudentUpdatedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "createdOn",
        "username",
        "schoolId",
        "tags",
        "loadtime",
        "uuid",
        "occurredOn",
        "grade",
        "sectionId",
        "tenantId",
        "gradeId",
        "specialNeeds"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "StudentUpdatedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "createdOn":1575965594444,
              |     "uuid": "6c56d3b2-89b9-4c01-8f6f-614fa907151b",
              |     "username": "efde315b-6697-4389-b260-038c8689b4ca",
              |     "grade": "grade1",
              |     "gradeId": "026d37c7-8aef-49e3-a1fa-fb8e1fc38a3a",
              |     "sectionId": "cf39c65a-3bf9-4389-a108-f2a6f0e8a789",
              |     "schoolId": "e64bf79a-830b-415c-80db-d179c4e2307c",
              |     "tags": [
              |       "one tag",
              |       "two tag"
              |     ]
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-student-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-student-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "StudentUpdatedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("username") shouldBe "efde315b-6697-4389-b260-038c8689b4ca"
          fst.getAs[String]("grade") shouldBe "grade1"
          fst.getAs[String]("gradeId") shouldBe "026d37c7-8aef-49e3-a1fa-fb8e1fc38a3a"

          fst.getAs[String]("sectionId") shouldBe "cf39c65a-3bf9-4389-a108-f2a6f0e8a789"
          fst.getAs[String]("schoolId") shouldBe "e64bf79a-830b-415c-80db-d179c4e2307c"
          fst.getAs[String]("occurredOn") shouldBe "2019-12-10 08:13:14.444"

          fst.getAs[Array[String]]("tags") shouldBe Array("one tag", "two tag")
        }
      )
    }
  }

  test("handle StudentsDeletedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "createdOn",
        "schoolId",
        "loadtime",
        "occurredOn",
        "tenantId",
        "events"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "StudentsDeletedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "createdOn":1575965594444,
              |     "schoolId": "6c56d3b2-89b9-4c01-8f6f-614fa907151b",
              |     "events": [
              |       {
              |         "studentId": "973cecb9-2e9f-410d-8e57-c1ef3ec191b6",
              |         "gradeName": 5,
              |         "sectionId": "fe53a055-95f2-4312-90e6-662949b0eca6",
              |         "gradeId": "0c08f921-f3e3-4c82-a33c-576764a2f69a",
              |         "userName": "userName1"
              |       }
              |     ]
              |  }
              |},
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-students-deleted-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-students-deleted-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "StudentsDeletedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("eventDateDw") shouldBe "20191210"
          fst.getAs[String]("schoolId") shouldBe "6c56d3b2-89b9-4c01-8f6f-614fa907151b"

          val eventRow = explodeArr(df, "events").first()
          eventRow.getAs[String]("studentId") shouldBe "973cecb9-2e9f-410d-8e57-c1ef3ec191b6"
          eventRow.getAs[Int]("gradeName") shouldBe 5
          eventRow.getAs[String]("sectionId") shouldBe "fe53a055-95f2-4312-90e6-662949b0eca6"
          eventRow.getAs[String]("gradeId") shouldBe "0c08f921-f3e3-4c82-a33c-576764a2f69a"
          eventRow.getAs[String]("userName") shouldBe "userName1"
        }
      )
    }
  }

  test("handle TaggedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "name",
        "loadtime",
        "occurredOn",
        "tenantId",
        "type",
        "id",
        "tagId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |  "headers": {
                    |    "eventType": "TaggedEvent",
                    |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                    |  },
                    |  "body": {
                    |     "id":"6c934e4b-ba4d-4973-9971-8c713cd0aeb3",
                    |     "tagId":"132d01ac-c83f-460f-b252-3c37edd84b18",
                    |     "name":"nit5",
                    |     "type":"SCHOOL",
                    |     "occurredOn":1600593881438
                    |  }
                    |},
                    | "timestamp": "2019-04-20 16:24:37.501"
                    |}
                    |]
                  """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-tag-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-tag-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "TaggedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("eventDateDw") shouldBe "20200920"
          fst.getAs[String]("id") shouldBe "6c934e4b-ba4d-4973-9971-8c713cd0aeb3"
          fst.getAs[String]("tagId") shouldBe "132d01ac-c83f-460f-b252-3c37edd84b18"
          fst.getAs[String]("name") shouldBe "nit5"
          fst.getAs[String]("type") shouldBe "SCHOOL"
          fst.getAs[String]("occurredOn") shouldBe "2020-09-20 09:24:41.438"
        }
      )
    }
  }

  test("handle UntaggedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "name",
        "loadtime",
        "occurredOn",
        "tenantId",
        "type",
        "id",
        "tagId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |  "headers": {
                    |    "eventType": "UntaggedEvent",
                    |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                    |  },
                    |  "body": {
                    |     "id":"6c934e4b-ba4d-4973-9971-8c713cd0aeb4",
                    |     "tagId":"132d01ac-c83f-460f-b252-3c37edd84b19",
                    |     "name":"nit6",
                    |     "type":"SCHOOL",
                    |     "occurredOn":1600593881439
                    |  }
                    |},
                    | "timestamp": "2019-04-20 16:24:37.501"
                    |}
                    |]
                  """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-tag-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-tag-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "UntaggedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("eventDateDw") shouldBe "20200920"
          fst.getAs[String]("id") shouldBe "6c934e4b-ba4d-4973-9971-8c713cd0aeb4"
          fst.getAs[String]("tagId") shouldBe "132d01ac-c83f-460f-b252-3c37edd84b19"
          fst.getAs[String]("name") shouldBe "nit6"
          fst.getAs[String]("type") shouldBe "SCHOOL"
          fst.getAs[String]("occurredOn") shouldBe "2020-09-20 09:24:41.439"
        }
      )
    }
  }

  test("handle ClassScheduleSlotAddedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "startTime",
        "loadtime",
        "endTime",
        "occurredOn",
        "tenantId",
        "classId",
        "day"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |  "headers": {
                    |    "eventType": "ClassScheduleSlotAddedEvent",
                    |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                    |  },
                    |  "body": {
                    |     "day":"THURSDAY",
                    |     "startTime":"09:00",
                    |     "endTime":"10:00",
                    |     "classId":"6fa5886a-1633-4a26-94cf-341e01051165",
                    |     "eventType":"ClassScheduleSlotAddedEvent",
                    |     "occurredOn":1601212093782,
                    |     "partitionKey":"6fa5886a-1633-4a26-94cf-341e01051165",
                    |     "_class":"ClassScheduleSlotAddedEvent",
                    |     "id":"5f708ebd1616ba38035a5c2e"
                    |   }
                    |},
                    | "timestamp": "2020-04-20 16:24:37.501"
                    |}
                    |]
                  """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "class-schedule-slot-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("class-schedule-slot-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "ClassScheduleSlotAddedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("eventDateDw") shouldBe "20200927"
          fst.getAs[String]("classId") shouldBe "6fa5886a-1633-4a26-94cf-341e01051165"
          fst.getAs[String]("day") shouldBe "THURSDAY"
          fst.getAs[String]("startTime") shouldBe "09:00"
          fst.getAs[String]("endTime") shouldBe "10:00"
          fst.getAs[String]("loadtime") shouldBe "2020-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2020-09-27 13:08:13.782"
        }
      )
    }
  }

  test("handle ClassScheduleSlotDeletedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "startTime",
        "loadtime",
        "endTime",
        "occurredOn",
        "tenantId",
        "classId",
        "day"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |  "headers": {
                    |    "eventType": "ClassScheduleSlotDeletedEvent",
                    |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                    |  },
                    |  "body": {
                    |     "day":"SUNDAY",
                    |     "startTime":"09:00",
                    |     "endTime":"10:00",
                    |     "classId":"78b9007f-9e36-467d-894b-75d93cc63473",
                    |     "eventType":"ClassScheduleSlotDeletedEvent",
                    |     "occurredOn":1600942562226,
                    |     "partitionKey":"78b9007f-9e36-467d-894b-75d93cc63473",
                    |     "_class":"ClassScheduleSlotDeletedEvent",
                    |     "id":"5f6c71e23a8ff460688bd76e"
                    |   }
                    |},
                    | "timestamp": "2020-04-20 16:24:37.501"
                    |}
                    |]
                  """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "class-schedule-slot-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("class-schedule-slot-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "ClassScheduleSlotDeletedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("eventDateDw") shouldBe "20200924"
          fst.getAs[String]("classId") shouldBe "78b9007f-9e36-467d-894b-75d93cc63473"
          fst.getAs[String]("day") shouldBe "SUNDAY"
          fst.getAs[String]("startTime") shouldBe "09:00"
          fst.getAs[String]("endTime") shouldBe "10:00"
          fst.getAs[String]("loadtime") shouldBe "2020-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2020-09-24 10:16:02.226"
        }
      )
    }
  }

  test("handle AcademicYear Started event") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "id",
        "eventDateDw",
        "startDate",
        "loadtime",
        "endDate",
        "occurredOn",
        "tenantId",
        "schoolId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |  "headers": {
                    |    "eventType": "AcademicYearStarted",
                    |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                    |  },
                    |  "body": {
                    |     "startDate":1600942562226,
                    |     "endDate":1600942562226,
                    |     "id":"ay1",
                    |     "schoolId":"school-1",
                    |     "occurredOn":1600942562226
                    |  }
                    | },
                    | "timestamp": "2020-04-20 16:24:37.501"
                    |}
                    |]
                  """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-academic-year-started-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-academic-year-started-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "AcademicYearStarted"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("eventDateDw") shouldBe "20200924"
          fst.getAs[String]("id") shouldBe "ay1"
          fst.getAs[Long]("startDate") shouldBe 1600942562226L
          fst.getAs[Long]("endDate") shouldBe 1600942562226L
          fst.getAs[String]("schoolId") shouldBe "school-1"
          fst.getAs[String]("loadtime") shouldBe "2020-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2020-09-24 10:16:02.226"
        }
      )
    }
  }

  test("handle AcademicYear Updated event") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "id",
        "eventDateDw",
        "startDate",
        "loadtime",
        "endDate",
        "occurredOn",
        "tenantId",
        "schoolId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |  "headers": {
                    |    "eventType": "AcademicYearDateRangeChanged",
                    |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                    |  },
                    |  "body": {
                    |     "startDate":1600942562226,
                    |     "endDate":1600942562226,
                    |     "id":"ay1",
                    |     "schoolId":"school-1",
                    |     "occurredOn":1600942562226
                    |  }
                    | },
                    | "timestamp": "2020-04-20 16:24:37.501"
                    |}
                    |]
                  """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "admin-academic-year-date-updated-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("admin-academic-year-date-updated-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "AcademicYearDateRangeChanged"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("eventDateDw") shouldBe "20200924"
          fst.getAs[String]("id") shouldBe "ay1"
          fst.getAs[Long]("startDate") shouldBe 1600942562226L
          fst.getAs[Long]("endDate") shouldBe 1600942562226L
          fst.getAs[String]("schoolId") shouldBe "school-1"
          fst.getAs[String]("loadtime") shouldBe "2020-04-20 16:24:37.501"
          fst.getAs[String]("occurredOn") shouldBe "2020-09-24 10:16:02.226"
        }
      )
    }
  }

  test("handle BadgesMetaDataUpdatedEvent event") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "id",
        "type",
        "order",
        "title",
        "category",
        "tenantCode",
        "finalDescription",
        "createdAt",
        "k12Grades",
        "rules",
        "occurredOn",
        "eventDateDw",
        "loadtime",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |   {
              |      "key":"key1",
              |      "value":{
              |         "headers":{
              |            "eventType":"BadgesMetaDataUpdatedEvent",
              |            "contentType":"application/json",
              |            "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |         },
              |         "body":{
              |            "id":"8b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |            "type":"WAVE",
              |            "order":1,
              |            "title":"Wave Badge",
              |            "category":"LOGIN",
              |            "tenantCode":"moe",
              |            "finalDescription":"You have got every badge possible in this type.",
              |            "createdAt":"2023-04-18T12:19:37.503648",
              |            "k12Grades":[
              |               "5",
              |               "6",
              |               "7",
              |               "8"
              |            ],
              |            "rules":[
              |               {
              |                  "tier":"BRONZE",
              |                  "order":1,
              |                  "defaultThreshold":5,
              |                  "defaultCompletionMessage":"",
              |                  "defaultDescription":"Continue to log in to earn a Bronze Badge.",
              |                  "rulesByGrade":[
              |                     {
              |                        "k12Grade":"6",
              |                        "threshold":7,
              |                        "description":"Login 7 times",
              |                        "completionMessage":""
              |                     }
              |                  ]
              |               },
              |               {
              |                  "tier":"SILVER",
              |                  "order":2,
              |                  "defaultThreshold":10,
              |                  "defaultCompletionMessage":"",
              |                  "defaultDescription":"Continue to log in to earn a Silver Badge.",
              |                  "rulesByGrade":[
              |                     {
              |                        "k12Grade":"6",
              |                        "threshold":15,
              |                        "description":"Login 15 times to earn silver badge",
              |                        "completionMessage":""
              |                     }
              |                  ]
              |               },
              |               {
              |                  "tier":"GOLD",
              |                  "order":3,
              |                  "defaultThreshold":20,
              |                  "defaultDescription":"Continue to log in to earn a Gold Badge.",
              |                  "rulesByGrade":[
              |                     {
              |                        "k12Grade":"6",
              |                        "threshold":25,
              |                        "description":"Login 25 times to get platinum badge"
              |                     }
              |                  ]
              |               }
              |            ],
              |            "occurredOn":1682581172000
              |         }
              |      },
              |      "timestamp":"2023-04-27T12:19:37.50364"
              |   }
              |]
              """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "badge-updated-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("badge-updated-sink is not found"))

          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "BadgesMetaDataUpdatedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("id") shouldBe "8b8cf877-c6e8-4450-aaaf-2333a92abb3e"
          fst.getAs[String]("type") shouldBe "WAVE"
          fst.getAs[Int]("order") shouldBe 1
          fst.getAs[String]("title") shouldBe "Wave Badge"
          fst.getAs[String]("category") shouldBe "LOGIN"
          fst.getAs[String]("tenantCode") shouldBe "moe"
          fst.getAs[String]("finalDescription") shouldBe "You have got every badge possible in this type."
          fst.getAs[String]("createdAt") shouldBe "2023-04-18T12:19:37.503648"
        }
      )
    }
  }

  test("handle leaderboard events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "leaders",
        "eventType",
        "eventDateDw",
        "endDate",
        "loadtime",
        "academicYearId",
        "occurredOn",
        "id",
        "className",
        "tenantId",
        "type",
        "startDate",
        "classId",
        "gradeId",
        "pathwayId"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"PathwayLeaderboardUpdatedEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |     "id": "965f13c3-46b3-456f-aecd-2ad25d79d7f2",
              |     "type": "PATHWAY",
              |	    "classId": "33e5e113-fb5e-4341-9e27-a3efe3d00b88",
              |     "pathwayId": "feb981fd-ca5b-45bf-95de-2f02a8f95e64",
              |	    "gradeId": "9184acd2-02f7-49e5-9f4e-f0c034188e6b",
              |	    "className": "Dragons-Pathway-DNT",
              |	    "academicYearId": "3d47b897-830b-45f5-b5da-fbcfbad9541e",
              |	    "startDate": "2023-06-05",
              |	    "endDate": "2023-06-11",
              |	    "leaders": [
              |	    	{
              |	    		"studentId": "8a3335dd-c6ba-468d-8453-111d847d8cf0",
              |	    		"name": "Dragons-e2e-s11  Dragons-e2e-s11",
              |	    		"avatar": "avatar_48",
              |	    		"order": 1,
              |	    		"progress": 2,
              |	    		"averageScore": 100,
              |	    		"totalStars": 26
              |	    	},
              |	    	{
              |	    		"studentId": "305d307e-4fc8-4dc1-8c7e-9230fe5df117",
              |	    		"name": "Dragons-e2e-s12  Dragons-e2e-s12",
              |	    		"avatar": "avatar_50",
              |	    		"order": 2,
              |	    		"progress": 1,
              |	    		"averageScore": 80,
              |	    		"totalStars": 6
              |	    	}
              |	    ],
              |	  "occurredOn": 1686028630319
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
          val dfOpt = sinks.find(_.name == "leaderboard-events-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "PathwayLeaderboardUpdatedEvent"
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("id") shouldBe "965f13c3-46b3-456f-aecd-2ad25d79d7f2"
            fst.getAs[String]("type") shouldBe "PATHWAY"
            fst.getAs[String]("classId") shouldBe "33e5e113-fb5e-4341-9e27-a3efe3d00b88"
            fst.getAs[String]("pathwayId") shouldBe "feb981fd-ca5b-45bf-95de-2f02a8f95e64"
            fst.getAs[String]("gradeId") shouldBe "9184acd2-02f7-49e5-9f4e-f0c034188e6b"
            fst.getAs[String]("className") shouldBe "Dragons-Pathway-DNT"
            fst.getAs[String]("academicYearId") shouldBe "3d47b897-830b-45f5-b5da-fbcfbad9541e"
            fst.getAs[String]("startDate") shouldBe "2023-06-05"
            fst.getAs[String]("endDate") shouldBe "2023-06-11"
            fst.getAs[Long]("occurredOn") shouldBe "2023-06-06 05:17:10.319"
          }
        }
      )
    }
  }

  test("handle certificate events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "tenantId",
        "academicYearId",
        "occurredOn",
        "certificateId",
        "studentId",
        "academicYear",
        "gradeId",
        "classId",
        "awardedBy",
        "category",
        "purpose",
        "language"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"CertificateAwardedEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |     "certificateId": "965f13c3-46b3-456f-aecd-2ad25d79d7f2",
              |     "studentId": "897f13c3-46b3-956f-aecd-7ad25d79d7f5",
              |     "academicYearId": "3d47b897-830b-45f5-b5da-fbcfbad9541e",
              |     "academicYear": "2023",
              |	    "classId": "33e5e113-fb5e-4341-9e27-a3efe3d00b88",
              |     "gradeId": "feb981fd-ca5b-45bf-95de-2f02a8f95e64",
              |	    "awardedBy": "9184acd2-02f7-49e5-9f4e-f0c034188e6b",
              |	    "category": "Certificate of Appreciation",
              |     "purpose": "for Consistent Improvement",
              |	    "language": "ENGLISH",
              |     "occurredOn": 1686028630319
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
          val dfOpt = sinks.find(_.name == "certificate-events-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "CertificateAwardedEvent"
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("certificateId") shouldBe "965f13c3-46b3-456f-aecd-2ad25d79d7f2"
            fst.getAs[String]("studentId") shouldBe "897f13c3-46b3-956f-aecd-7ad25d79d7f5"
            fst.getAs[String]("academicYearId") shouldBe "3d47b897-830b-45f5-b5da-fbcfbad9541e"
            fst.getAs[String]("academicYear") shouldBe "2023"
            fst.getAs[String]("classId") shouldBe "33e5e113-fb5e-4341-9e27-a3efe3d00b88"
            fst.getAs[String]("gradeId") shouldBe "feb981fd-ca5b-45bf-95de-2f02a8f95e64"
            fst.getAs[String]("awardedBy") shouldBe "9184acd2-02f7-49e5-9f4e-f0c034188e6b"
            fst.getAs[String]("category") shouldBe "Certificate of Appreciation"
            fst.getAs[String]("purpose") shouldBe "for Consistent Improvement"
            fst.getAs[String]("language") shouldBe "ENGLISH"
            fst.getAs[Long]("occurredOn") shouldBe "2023-06-06 05:17:10.319"
            fst.getAs[String]("eventDateDw") shouldBe "20230606"
            fst.getAs[String]("loadtime") shouldBe "2023-05-15 16:23:46.609"
          }
        }
      )
    }
  }

  test("handle avatar selected events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "tenantId",
        "occurredOn",
        "avatarId",
        "uuid",
        "gradeId",
        "schoolId",
        "avatarFileId",
        "avatarType"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"UserAvatarSelectedEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "uuid": "eb942a90-3d43-4095-bada-acab15701c40",
              |	      "avatarId": "avatar_42",
              |	      "gradeId": "d3296950-4eac-4706-aeb8-de071963fdbb",
              |	      "schoolId": "3a207ff4-4b86-405b-abe0-7b9771edf905",
              |	      "occurredOn": 1693803257300
              |   }
              | },
              | "timestamp": "2023-09-15 16:23:46.609"
              |}
              |]
              """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "user-avatar-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "UserAvatarSelectedEvent"
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("uuid") shouldBe "eb942a90-3d43-4095-bada-acab15701c40"
            fst.getAs[String]("avatarId") shouldBe "avatar_42"
            fst.getAs[String]("gradeId") shouldBe "d3296950-4eac-4706-aeb8-de071963fdbb"
            fst.getAs[String]("schoolId") shouldBe "3a207ff4-4b86-405b-abe0-7b9771edf905"
            fst.getAs[Long]("occurredOn") shouldBe "2023-09-04 04:54:17.300"
            fst.getAs[String]("eventDateDw") shouldBe "20230904"
            fst.getAs[String]("loadtime") shouldBe "2023-09-15 16:23:46.609"
          }
        }
      )
    }
  }

  test("handle avatar updated events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "tenantId",
        "occurredOn",
        "avatarId",
        "uuid",
        "gradeId",
        "schoolId",
        "avatarFileId",
        "avatarType"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"UserAvatarUpdatedEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "uuid": "5ba72728-f6eb-4ddd-ab85-9aa20815b5f6",
              |	      "avatarId": "avatar_43",
              |	      "gradeId": "d3296950-4eac-4706-aeb8-de071963fdbb",
              |	      "schoolId": "3a207ff4-4b86-405b-abe0-7b9771edf905",
              |	      "occurredOn": 1693803431746
              |   }
              | },
              | "timestamp": "2023-09-16 16:23:46.609"
              |}
              |]
              """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "user-avatar-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "UserAvatarUpdatedEvent"
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("uuid") shouldBe "5ba72728-f6eb-4ddd-ab85-9aa20815b5f6"
            fst.getAs[String]("avatarId") shouldBe "avatar_43"
            fst.getAs[String]("gradeId") shouldBe "d3296950-4eac-4706-aeb8-de071963fdbb"
            fst.getAs[String]("schoolId") shouldBe "3a207ff4-4b86-405b-abe0-7b9771edf905"
            fst.getAs[Long]("occurredOn") shouldBe "2023-09-04 04:57:11.746"
            fst.getAs[String]("eventDateDw") shouldBe "20230904"
            fst.getAs[String]("loadtime") shouldBe "2023-09-16 16:23:46.609"
          }
        }
      )
    }
  }

  test("handle pathway target mutated events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "tenantId",
        "uuid",
        "pathwayTargetId",
        "pathwayId",
        "classId",
        "gradeId",
        "schoolId",
        "startDate",
        "endDate",
        "occurredOn",
        "teacherId",
        "targetStatus"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              | "value":{
              |    "headers":{
              |       "eventType":"PathwayTargetMutatedEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "uuid": "965f13c3-46b3-456f-aecd-2ad25d79d7f2",
              |       "pathwayTargetId": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              |       "pathwayId": "feb981fd-ca5b-45bf-95de-2f02a8f95e64",
              |       "classId": "33e5e113-fb5e-4341-9e27-a3efe3d00b88",
              |	      "gradeId": "9184acd2-02f7-49e5-9f4e-f0c034188e6b",
              |       "schoolId": "4f0763cf-c429-4d8c-b76e-10c6a1066a21",
              |       "startDate": "2023-10-25",
              |       "endDate": "2023-11-24",
              |       "occurredOn": 1698296264199,
              |	      "teacherId": "21768a14-17fe-4018-8a14-0a95ac36c997",
              |       "targetStatus": "CREATED"
              |   }
              | },
              | "timestamp": "2023-10-26 16:23:46.609"
              |}
              |]
              """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "pathway-class-target-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "PathwayTargetMutatedEvent"
            fst.getAs[String]("eventDateDw") shouldBe "20231026"
            fst.getAs[String]("loadtime") shouldBe "2023-10-26 16:23:46.609"
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("uuid") shouldBe "965f13c3-46b3-456f-aecd-2ad25d79d7f2"
            fst.getAs[String]("pathwayTargetId") shouldBe "a12ffa9d-18bb-430c-b0b8-ad35c964f590"
            fst.getAs[String]("pathwayId") shouldBe "feb981fd-ca5b-45bf-95de-2f02a8f95e64"
            fst.getAs[String]("classId") shouldBe "33e5e113-fb5e-4341-9e27-a3efe3d00b88"
            fst.getAs[String]("gradeId") shouldBe "9184acd2-02f7-49e5-9f4e-f0c034188e6b"
            fst.getAs[String]("schoolId") shouldBe "4f0763cf-c429-4d8c-b76e-10c6a1066a21"
            fst.getAs[String]("startDate") shouldBe "2023-10-25"
            fst.getAs[String]("endDate") shouldBe "2023-11-24"
            fst.getAs[Long]("occurredOn") shouldBe "2023-10-26 04:57:44.199"
            fst.getAs[String]("teacherId") shouldBe "21768a14-17fe-4018-8a14-0a95ac36c997"
            fst.getAs[String]("targetStatus") shouldBe "CREATED"
          }
        }
      )
    }
  }

  test("handle student pathway target mutated events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "tenantId",
        "uuid",
        "pathwayTargetId",
        "pathwayId",
        "classId",
        "gradeId",
        "schoolId",
        "studentTargetId",
        "studentId",
        "recommendedTarget",
        "finalizedTarget",
        "occurredOn",
        "teacherId",
        "targetStatus",
        "levelsCompleted",
        "earnedStars"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              | "value":{
              |    "headers":{
              |       "eventType":"StudentPathwayTargetMutatedEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |     	"uuid": "06a63d54-034a-43ef-a55c-74e8cf028b82",
              |     	"pathwayTargetId": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              |     	"pathwayId": "feb981fd-ca5b-45bf-95de-2f02a8f95e64",
              |     	"classId": "33e5e113-fb5e-4341-9e27-a3efe3d00b88",
              |     	"gradeId": "9184acd2-02f7-49e5-9f4e-f0c034188e6b",
              |     	"schoolId": "4f0763cf-c429-4d8c-b76e-10c6a1066a21",
              |     	"studentTargetId": "a21c448f-e968-4ebc-9688-85487d25f6bc",
              |     	"studentId": "b44ae6a9-457e-44e9-b6f7-d8c13623f533",
              |     	"recommendedTarget": 10,
              |     	"finalizedTarget": 10,
              |     	"occurredOn": 1698296264207,
              |     	"teacherId": "21768a14-17fe-4018-8a14-0a95ac36c997",
              |     	"targetStatus": "CREATED",
              |     	"levelsCompleted": 0,
              |       "earnedStars": 0
              |     }
              | },
              | "timestamp": "2023-10-26 16:23:46.609"
              |}
              |]
              """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "pathway-student-target-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "StudentPathwayTargetMutatedEvent"
            fst.getAs[String]("eventDateDw") shouldBe "20231026"
            fst.getAs[String]("loadtime") shouldBe "2023-10-26 16:23:46.609"
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("uuid") shouldBe "06a63d54-034a-43ef-a55c-74e8cf028b82"
            fst.getAs[String]("pathwayTargetId") shouldBe "a12ffa9d-18bb-430c-b0b8-ad35c964f590"
            fst.getAs[String]("pathwayId") shouldBe "feb981fd-ca5b-45bf-95de-2f02a8f95e64"
            fst.getAs[String]("classId") shouldBe "33e5e113-fb5e-4341-9e27-a3efe3d00b88"
            fst.getAs[String]("gradeId") shouldBe "9184acd2-02f7-49e5-9f4e-f0c034188e6b"
            fst.getAs[String]("schoolId") shouldBe "4f0763cf-c429-4d8c-b76e-10c6a1066a21"
            fst.getAs[String]("studentTargetId") shouldBe "a21c448f-e968-4ebc-9688-85487d25f6bc"
            fst.getAs[String]("studentId") shouldBe "b44ae6a9-457e-44e9-b6f7-d8c13623f533"
            fst.getAs[Int]("recommendedTarget") shouldBe 10
            fst.getAs[Int]("finalizedTarget") shouldBe 10
            fst.getAs[Long]("occurredOn") shouldBe "2023-10-26 04:57:44.207"
            fst.getAs[String]("teacherId") shouldBe "21768a14-17fe-4018-8a14-0a95ac36c997"
            fst.getAs[String]("targetStatus") shouldBe "CREATED"
            fst.getAs[Int]("levelsCompleted") shouldBe 0
            fst.getAs[Int]("earnedStars") shouldBe 0
          }
        }
      )
    }
  }

  test("handle marketplace item purchased event") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "tenantId",
        "purchaseId",
        "itemType",
        "itemId",
        "itemTitle",
        "itemDescription",
        "gradeId",
        "sectionId",
        "schoolId",
        "transactionId",
        "academicYear",
        "academicYearId",
        "occurredOn",
        "studentId",
        "redeemedStars",
        "eventId"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              | "value":{
              |    "headers":{
              |       "eventType":"ItemPurchasedEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |         "purchaseId": "8b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |         "itemType": "AVATAR",
              |         "itemId": "9184acd2-02f7-49e5-9f4e-f0c034188e6b",
              |         "itemTitle": "test.png",
              |         "itemDescription": "test description",
              |         "occurredOn": 1698296264207,
              |         "studentId": "2b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |         "gradeId": "3b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |         "sectionId": "4b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |         "schoolId": "7b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |         "transactionId": "8b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |         "academicYearId": "9b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |         "academicYear": "2024",
              |         "redeemedStars": 10,
              |         "eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c"
              |     }
              | },
              | "timestamp": "2023-10-26 16:23:46.609"
              |}
              |]
              """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "marketplace-purchase-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "ItemPurchasedEvent"
            fst.getAs[String]("purchaseId") shouldBe "8b8cf877-c6e8-4450-aaaf-2333a92abb3e"
            fst.getAs[String]("itemType") shouldBe "AVATAR"
            fst.getAs[String]("itemId") shouldBe "9184acd2-02f7-49e5-9f4e-f0c034188e6b"
            fst.getAs[String]("itemTitle") shouldBe "test.png"
            fst.getAs[String]("itemDescription") shouldBe "test description"
            fst.getAs[Long]("occurredOn") shouldBe "2023-10-26 04:57:44.207"
            fst.getAs[String]("studentId") shouldBe "2b8cf877-c6e8-4450-aaaf-2333a92abb3e"
            fst.getAs[String]("gradeId") shouldBe "3b8cf877-c6e8-4450-aaaf-2333a92abb3e"
            fst.getAs[String]("sectionId") shouldBe "4b8cf877-c6e8-4450-aaaf-2333a92abb3e"
            fst.getAs[String]("schoolId") shouldBe "7b8cf877-c6e8-4450-aaaf-2333a92abb3e"
            fst.getAs[String]("transactionId") shouldBe "8b8cf877-c6e8-4450-aaaf-2333a92abb3e"
            fst.getAs[String]("academicYear") shouldBe "2024"
            fst.getAs[String]("academicYearId") shouldBe "9b8cf877-c6e8-4450-aaaf-2333a92abb3e"
            fst.getAs[Int]("redeemedStars") shouldBe 10
            fst.getAs[String]("eventId") shouldBe "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c"
          }
        }
      )
    }
  }

  test("handle reward transactions event") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "loadtime",
        "tenantId",
        "eventDateDw",
        "studentId",
        "availableStars",
        "itemCost",
        "starBalance",
        "itemId",
        "itemType",
        "gradeId",
        "sectionId",
        "schoolId",
        "transactionId",
        "academicYear",
        "academicYearId",
        "occurredOn",
        "eventId"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              | "value":{
              |    "headers":{
              |       "eventType":"PurchaseTransactionEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |         "availableStars": 100,
              |         "itemCost": 10,
              |         "starBalance": 90,
              |         "itemId": "9184acd2-02f7-49e5-9f4e-f0c034188e6b",
              |         "itemType": "AVATAR",
              |         "gradeId": "3b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |         "sectionId": "4b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |         "schoolId": "7b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |         "transactionId": "8b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |         "academicYearId": "9b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |         "academicYear": "2024",
              |         "occurredOn": 1698296264207,
              |         "studentId": "2b8cf877-c6e8-4450-aaaf-2333a92abb3e",
              |         "eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c"
              |     }
              | },
              | "timestamp": "2023-10-26 16:23:46.609"
              |}
              |]
              """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "reward-purchase-transaction-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "PurchaseTransactionEvent"
            fst.getAs[Int]("availableStars") shouldBe 100
            fst.getAs[Int]("itemCost") shouldBe 10
            fst.getAs[Int]("starBalance") shouldBe 90
            fst.getAs[Long]("occurredOn") shouldBe "2023-10-26 04:57:44.207"
            fst.getAs[String]("itemId") shouldBe "9184acd2-02f7-49e5-9f4e-f0c034188e6b"
            fst.getAs[String]("gradeId") shouldBe "3b8cf877-c6e8-4450-aaaf-2333a92abb3e"
            fst.getAs[String]("sectionId") shouldBe "4b8cf877-c6e8-4450-aaaf-2333a92abb3e"
            fst.getAs[String]("schoolId") shouldBe "7b8cf877-c6e8-4450-aaaf-2333a92abb3e"
            fst.getAs[String]("transactionId") shouldBe "8b8cf877-c6e8-4450-aaaf-2333a92abb3e"
            fst.getAs[String]("academicYear") shouldBe "2024"
            fst.getAs[String]("academicYearId") shouldBe "9b8cf877-c6e8-4450-aaaf-2333a92abb3e"
            fst.getAs[String]("studentId") shouldBe "2b8cf877-c6e8-4450-aaaf-2333a92abb3e"
            fst.getAs[String]("eventId") shouldBe "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c"
          }
        }
      )
    }
  }

  test("handle avatar created event") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "tenantId",
        "id",
        "type",
        "status",
        "name",
        "fileId",
        "description",
        "validFrom",
        "validTill",
        "organizations",
        "customization",
        "createdAt",
        "createdBy",
        "updatedAt",
        "updatedBy",
        "starCost",
        "genders",
        "category",
        "tenants",
        "eventId",
        "isEnabledForAllOrgs",
        "occurredOn"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              | "value":{
              |    "headers":{
              |       "eventType":"AvatarCreatedEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |	        "id": "b95a7743-4e45-4ffc-986a-99de62396038",
              |	        "type": "PREMIUM",
              |	        "status": "ENABLED",
              |	        "name": "meme.png",
              |	        "fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
              |	        "description": "some description",
              |	        "validFrom": "2024-04-23T10:35:36.418102203",
              |	        "validTill": "2024-04-29T20:00:00",
              |	        "organizations": [
              |	        	"MoE-AbuDhabi(Public)",
              |	        	"MoE-North (Public)"
              |	        ],
              |	        "customization": null,
              |	        "createdAt": "2024-04-23T10:35:36.41813526",
              |	        "createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
              |	        "updatedAt": "2024-04-23T10:35:36.418140282",
              |	        "updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
              |	        "starCost": 1,
              |	        "genders": [
              |	        	"MALE"
              |	        ],
              |	        "category": "CHARACTERS",
              |	        "tenants": [
              |	        	"e2e"
              |	        ],
              |	        "eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
              |	        "isEnabledForAllOrgs": true,
              |	        "occurredOn": 1698296264207
              |     }
              | },
              | "timestamp": "2023-10-26 16:23:46.609"
              |}
              |]
                      """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "marketplace-avatar-created-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "AvatarCreatedEvent"
            fst.getAs[String]("type") shouldBe "PREMIUM"
            fst.getAs[String]("status") shouldBe "ENABLED"
            fst.getAs[String]("name") shouldBe "meme.png"
            fst.getAs[String]("fileId") shouldBe "5744b599-8a80-4d9d-a299-ae72e7cfd42c"
            fst.getAs[String]("description") shouldBe "some description"
            fst.getAs[String]("validFrom") shouldBe "2024-04-23T10:35:36.418102203"
            fst.getAs[String]("validTill") shouldBe "2024-04-29T20:00:00"
            fst.getAs[Seq[String]]("organizations") shouldBe Seq("MoE-AbuDhabi(Public)", "MoE-North (Public)")
            fst.getAs[String]("createdAt") shouldBe "2024-04-23T10:35:36.41813526"
            fst.getAs[String]("createdBy") shouldBe "ebe4d97e-c2c0-481f-b90a-ccd5138234ee"
            fst.getAs[String]("updatedAt") shouldBe "2024-04-23T10:35:36.418140282"
            fst.getAs[String]("updatedBy") shouldBe "ebe4d97e-c2c0-481f-b90a-ccd5138234ee"
            fst.getAs[Int]("starCost") shouldBe 1
            fst.getAs[Boolean]("isEnabledForAllOrgs") shouldBe true
            fst.getAs[Seq[String]]("genders") shouldBe Seq("MALE")
            fst.getAs[String]("category") shouldBe "CHARACTERS"
            fst.getAs[Seq[String]]("tenants") shouldBe Seq("e2e")
            fst.getAs[String]("eventId") shouldBe "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c"
            fst.getAs[Long]("occurredOn") shouldBe "2023-10-26 04:57:44.207"
          }
        }
      )
    }
  }

  test("handle avatar created event for custom avatars") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "tenantId",
        "id",
        "type",
        "status",
        "name",
        "fileId",
        "description",
        "validFrom",
        "validTill",
        "organizations",
        "customization",
        "createdAt",
        "createdBy",
        "updatedAt",
        "updatedBy",
        "starCost",
        "genders",
        "category",
        "tenants",
        "eventId",
        "isEnabledForAllOrgs",
        "occurredOn"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              | "value":{
              |    "headers":{
              |       "eventType":"AvatarCreatedEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |	        "id": "b95a7743-4e45-4ffc-986a-99de62396038",
              |	        "type": "PREMIUM",
              |	        "status": "ENABLED",
              |	        "name": "meme.png",
              |	        "fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
              |	        "description": "some description",
              |	        "validFrom": "2024-04-23T10:35:36.418102203",
              |	        "validTill": "2024-04-29T20:00:00",
              |	        "organizations": [
              |	        	"MoE-AbuDhabi(Public)",
              |	        	"MoE-North (Public)"
              |	        ],
              |         "customization": ["customization-item-id", "customization-colors-id"],
              |	        "createdAt": "2024-04-23T10:35:36.41813526",
              |	        "createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
              |	        "updatedAt": "2024-04-23T10:35:36.418140282",
              |	        "updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
              |	        "starCost": 1,
              |	        "genders": [
              |	        	"MALE"
              |	        ],
              |	        "category": "CHARACTERS",
              |	        "tenants": [
              |	        	"e2e"
              |	        ],
              |	        "eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
              |	        "isEnabledForAllOrgs": true,
              |	        "occurredOn": 1698296264207
              |     }
              | },
              | "timestamp": "2023-10-26 16:23:46.609"
              |}
              |]
                      """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "marketplace-avatar-created-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "AvatarCreatedEvent"
            fst.getAs[String]("type") shouldBe "PREMIUM"
            fst.getAs[String]("status") shouldBe "ENABLED"
            fst.getAs[String]("name") shouldBe "meme.png"
            fst.getAs[String]("fileId") shouldBe "5744b599-8a80-4d9d-a299-ae72e7cfd42c"
            fst.getAs[String]("description") shouldBe "some description"
            fst.getAs[String]("validFrom") shouldBe "2024-04-23T10:35:36.418102203"
            fst.getAs[String]("validTill") shouldBe "2024-04-29T20:00:00"
            fst.getAs[Seq[String]]("organizations") shouldBe Seq("MoE-AbuDhabi(Public)", "MoE-North (Public)")
            fst.getAs[String]("createdAt") shouldBe "2024-04-23T10:35:36.41813526"
            fst.getAs[String]("createdBy") shouldBe "ebe4d97e-c2c0-481f-b90a-ccd5138234ee"
            fst.getAs[String]("updatedAt") shouldBe "2024-04-23T10:35:36.418140282"
            fst.getAs[String]("updatedBy") shouldBe "ebe4d97e-c2c0-481f-b90a-ccd5138234ee"
            fst.getAs[Int]("starCost") shouldBe 1
            fst.getAs[Boolean]("isEnabledForAllOrgs") shouldBe true
            fst.getAs[Seq[String]]("genders") shouldBe Seq("MALE")
            fst.getAs[String]("category") shouldBe "CHARACTERS"
            fst.getAs[Seq[String]]("tenants") shouldBe Seq("e2e")
            fst.getAs[String]("eventId") shouldBe "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c"
            fst.getAs[Long]("occurredOn") shouldBe "2023-10-26 04:57:44.207"
            fst.getAs[Seq[String]]("customization") shouldBe Seq("customization-item-id", "customization-colors-id")
          }
        }
      )
    }
  }

  test("handle avatar updated event") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "tenantId",
        "id",
        "type",
        "status",
        "name",
        "fileId",
        "description",
        "validFrom",
        "validTill",
        "organizations",
        "customization",
        "createdAt",
        "createdBy",
        "updatedAt",
        "updatedBy",
        "starCost",
        "genders",
        "category",
        "tenants",
        "eventId",
        "isEnabledForAllOrgs",
        "occurredOn"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              | "value":{
              |    "headers":{
              |       "eventType":"AvatarUpdatedEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |	        "id": "b95a7743-4e45-4ffc-986a-99de62396038",
              |	        "type": "PREMIUM",
              |	        "status": "ENABLED",
              |	        "name": "meme.png",
              |	        "fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
              |	        "description": "some description",
              |	        "validFrom": "2024-04-23T10:35:36.418102203",
              |	        "validTill": "2024-04-29T20:00:00",
              |	        "organizations": [
              |	        	"MoE-AbuDhabi(Public)",
              |	        	"MoE-North (Public)"
              |	        ],
              |	        "customization": null,
              |	        "createdAt": "2024-04-23T10:35:36.41813526",
              |	        "createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
              |	        "updatedAt": "2024-04-23T10:35:36.418140282",
              |	        "updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
              |	        "starCost": 1,
              |	        "genders": [
              |	        	"MALE"
              |	        ],
              |	        "category": "CHARACTERS",
              |	        "tenants": [
              |	        	"e2e"
              |	        ],
              |	        "eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
              |         "isEnabledForAllOrgs": true,
              |	        "occurredOn": 1698296264207
              |     }
              | },
              | "timestamp": "2023-10-26 16:23:46.609"
              |}
              |]
                      """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "marketplace-avatar-updated-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "AvatarUpdatedEvent"
            fst.getAs[String]("type") shouldBe "PREMIUM"
            fst.getAs[String]("status") shouldBe "ENABLED"
            fst.getAs[String]("name") shouldBe "meme.png"
            fst.getAs[String]("fileId") shouldBe "5744b599-8a80-4d9d-a299-ae72e7cfd42c"
            fst.getAs[String]("description") shouldBe "some description"
            fst.getAs[String]("validFrom") shouldBe "2024-04-23T10:35:36.418102203"
            fst.getAs[String]("validTill") shouldBe "2024-04-29T20:00:00"
            fst.getAs[Seq[String]]("organizations") shouldBe Seq("MoE-AbuDhabi(Public)", "MoE-North (Public)")
            fst.getAs[String]("createdAt") shouldBe "2024-04-23T10:35:36.41813526"
            fst.getAs[String]("createdBy") shouldBe "ebe4d97e-c2c0-481f-b90a-ccd5138234ee"
            fst.getAs[String]("updatedAt") shouldBe "2024-04-23T10:35:36.418140282"
            fst.getAs[String]("updatedBy") shouldBe "ebe4d97e-c2c0-481f-b90a-ccd5138234ee"
            fst.getAs[Int]("starCost") shouldBe 1
            fst.getAs[Seq[String]]("genders") shouldBe Seq("MALE")
            fst.getAs[String]("category") shouldBe "CHARACTERS"
            fst.getAs[Seq[String]]("tenants") shouldBe Seq("e2e")
            fst.getAs[String]("eventId") shouldBe "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c"
            fst.getAs[Long]("occurredOn") shouldBe "2023-10-26 04:57:44.207"
          }
        }
      )
    }
  }

  test("handle avatar deleted event") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "tenantId",
        "id",
        "name",
        "fileId",
        "deletedBy",
        "deletedAt",
        "type",
        "category",
        "description",
        "validFrom",
        "validTill",
        "organizations",
        "customization",
        "starCost",
        "genders",
        "tenants",
        "isEnabledForAllOrgs",
        "eventId",
        "occurredOn"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              | "value":{
              |    "headers":{
              |       "eventType":"AvatarDeletedEvent",
              |       "contentType":"application/json"
              |    },
              |    "body":{
              |	        "id": "33beeb9a-4cbc-4913-959b-3f3692a76e5b",
              |         "fileId": "81dd2671-06f6-47b7-afb1-6929fb03d83b",
              |         "name": "name",
              |         "deletedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
              |         "deletedAt": "2024-04-23T10:42:25.553661907",
              |         "id": "33beeb9a-4cbc-4913-959b-3f3692a76e5b",
              |         "fileId": "81dd2671-06f6-47b7-afb1-6929fb03d83b",
              |         "deletedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
              |         "deletedAt": "2024-04-23T10:42:25.553661907",
              |         "type": "PREMIUM",
              |         "category": "OTHERS",
              |         "description": "description",
              |         "validFrom": "2024-04-23T10:35:36.418102203",
              |	        "validTill": "2024-04-29T20:00:00",
              |	        "organizations": [
              |	        	"MoE-AbuDhabi(Public)",
              |	        	"MoE-North (Public)"
              |	        ],
              |	        "customization": null,
              |	        "starCost": 0,
              |         "genders": [
              |	        	"MALE"
              |	        ],
              |	        "tenants": [
              |	        	"e2e"
              |	        ],
              |         "isEnabledForAllOrgs": true,
              |         "eventId": "610dbdaa-8680-41f7-821a-1d42e65dc967",
              |         "occurredOn": 1698296264207
              |     }
              | },
              | "timestamp": "2023-10-26 16:23:46.609"
              |}
              |]
                """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "marketplace-avatar-deleted-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("id") shouldBe "33beeb9a-4cbc-4913-959b-3f3692a76e5b"
            fst.getAs[String]("fileId") shouldBe "81dd2671-06f6-47b7-afb1-6929fb03d83b"
            fst.getAs[String]("deletedBy") shouldBe "ebe4d97e-c2c0-481f-b90a-ccd5138234ee"
            fst.getAs[String]("deletedAt") shouldBe "2024-04-23T10:42:25.553661907"
            fst.getAs[String]("type") shouldBe "PREMIUM"
            fst.getAs[String]("category") shouldBe "OTHERS"
            fst.getAs[Boolean]("isEnabledForAllOrgs") shouldBe true
            fst.getAs[String]("eventId") shouldBe "610dbdaa-8680-41f7-821a-1d42e65dc967"
            fst.getAs[Long]("occurredOn") shouldBe "2023-10-26 04:57:44.207"
          }
        }
      )
    }
  }

  test("handle activity settings open path events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "classId",
        "activityId",
        "teacherId",
        "schoolId",
        "subjectName",
        "gradeId",
        "gradeLevel",
        "openPathEnabled",
        "eventType",
        "eventDateDw",
        "occurredOn",
        "tenantId",
        "loadtime"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              | "value":{
              |    "headers":{
              |       "eventType":"ActivitySettingsOpenPathEnabledEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |     	"classId": "06a63d54-034a-43ef-a55c-74e8cf028b82",
              |     	"activityId": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              |     	"teacherId": "feb981fd-ca5b-45bf-95de-2f02a8f95e64",
              |     	"schoolId": "33e5e113-fb5e-4341-9e27-a3efe3d00b88",
              |     	"subjectName": "MATHS",
              |     	"gradeId": "4f0763cf-c429-4d8c-b76e-10c6a1066a21",
              |     	"gradeLevel": "a21c448f-e968-4ebc-9688-85487d25f6bc",
              |     	"openPathEnabled": true,
              |       "occurredOn": "2023-12-05T16:03:46.264"
              |     }
              | },
              | "timestamp": "2023-10-26 16:23:46.609"
              |}
              |]
              """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "activity-settings-open-path-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "ActivitySettingsOpenPathEnabledEvent"
            fst.getAs[String]("eventDateDw") shouldBe "20231205"
            fst.getAs[String]("loadtime") shouldBe "2023-10-26 16:23:46.609"
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("classId") shouldBe "06a63d54-034a-43ef-a55c-74e8cf028b82"
            fst.getAs[String]("activityId") shouldBe "a12ffa9d-18bb-430c-b0b8-ad35c964f590"
            fst.getAs[String]("teacherId") shouldBe "feb981fd-ca5b-45bf-95de-2f02a8f95e64"
            fst.getAs[String]("schoolId") shouldBe "33e5e113-fb5e-4341-9e27-a3efe3d00b88"
            fst.getAs[String]("subjectName") shouldBe "MATHS"
            fst.getAs[String]("gradeId") shouldBe "4f0763cf-c429-4d8c-b76e-10c6a1066a21"
            fst.getAs[String]("gradeLevel") shouldBe "a21c448f-e968-4ebc-9688-85487d25f6bc"
            fst.getAs[Boolean]("openPathEnabled") shouldBe true
          }
        }
      )
    }
  }

  test("handle activity settings component visibility show event") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "openPathEnabled",
        "classId",
        "activityId",
        "teacherId",
        "schoolId",
        "subjectName",
        "gradeId",
        "gradeLevel",
        "componentId",
        "componentStatus",
        "eventType",
        "eventDateDw",
        "occurredOn",
        "tenantId",
        "loadtime"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              | "value":{
              |    "headers":{
              |       "eventType":"ActivitySettingsShowComponentEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |     	"openPathEnabled": true,
              |     	"classId": "06a63d54-034a-43ef-a55c-74e8cf028b82",
              |     	"activityId": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              |     	"teacherId": "feb981fd-ca5b-45bf-95de-2f02a8f95e64",
              |     	"schoolId": "33e5e113-fb5e-4341-9e27-a3efe3d00b88",
              |     	"subjectName": "MATHS",
              |     	"gradeId": "4f0763cf-c429-4d8c-b76e-10c6a1066a21",
              |     	"gradeLevel": 7,
              |     	"componentId": "609fdbf8-dd37-4a5a-8db0-1f4897e207eb",
              |	        "componentStatus": "VISIBLE",
              |       "occurredOn": "2023-12-05T16:03:46.264"
              |     }
              | },
              | "timestamp": "2023-10-26 16:23:46.609"
              |}
              |]
              """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "activity-settings-component-visibility-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "ActivitySettingsShowComponentEvent"
            fst.getAs[String]("eventDateDw") shouldBe "20231205"
            fst.getAs[String]("loadtime") shouldBe "2023-10-26 16:23:46.609"
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[Boolean]("openPathEnabled") shouldBe true
            fst.getAs[String]("classId") shouldBe "06a63d54-034a-43ef-a55c-74e8cf028b82"
            fst.getAs[String]("activityId") shouldBe "a12ffa9d-18bb-430c-b0b8-ad35c964f590"
            fst.getAs[String]("teacherId") shouldBe "feb981fd-ca5b-45bf-95de-2f02a8f95e64"
            fst.getAs[String]("schoolId") shouldBe "33e5e113-fb5e-4341-9e27-a3efe3d00b88"
            fst.getAs[String]("subjectName") shouldBe "MATHS"
            fst.getAs[String]("gradeId") shouldBe "4f0763cf-c429-4d8c-b76e-10c6a1066a21"
            fst.getAs[Int]("gradeLevel") shouldBe 7
            fst.getAs[String]("componentId") shouldBe "609fdbf8-dd37-4a5a-8db0-1f4897e207eb"
            fst.getAs[String]("componentStatus") shouldBe "VISIBLE"
          }
        }
      )
    }
  }

  test("handle activity settings component visibility hide event") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "openPathEnabled",
        "classId",
        "activityId",
        "teacherId",
        "schoolId",
        "subjectName",
        "gradeId",
        "gradeLevel",
        "componentId",
        "componentStatus",
        "eventType",
        "eventDateDw",
        "occurredOn",
        "tenantId",
        "loadtime"
      )

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              | "value":{
              |    "headers":{
              |       "eventType":"ActivitySettingsHideComponentEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |     	"openPathEnabled": true,
              |     	"classId": "06a63d54-034a-43ef-a55c-74e8cf028b82",
              |     	"activityId": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              |     	"teacherId": "feb981fd-ca5b-45bf-95de-2f02a8f95e64",
              |     	"schoolId": "33e5e113-fb5e-4341-9e27-a3efe3d00b88",
              |     	"subjectName": "MATHS",
              |     	"gradeId": "4f0763cf-c429-4d8c-b76e-10c6a1066a21",
              |     	"gradeLevel": 7,
              |     	"componentId": "609fdbf8-dd37-4a5a-8db0-1f4897e207eb",
              |	        "componentStatus": "HIDDEN",
              |       "occurredOn": "2023-12-05T16:03:46.264"
              |     }
              | },
              | "timestamp": "2023-10-26 16:23:46.609"
              |}
              |]
              """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "activity-settings-component-visibility-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "ActivitySettingsHideComponentEvent"
            fst.getAs[String]("eventDateDw") shouldBe "20231205"
            fst.getAs[String]("loadtime") shouldBe "2023-10-26 16:23:46.609"
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[Boolean]("openPathEnabled") shouldBe true
            fst.getAs[String]("classId") shouldBe "06a63d54-034a-43ef-a55c-74e8cf028b82"
            fst.getAs[String]("activityId") shouldBe "a12ffa9d-18bb-430c-b0b8-ad35c964f590"
            fst.getAs[String]("teacherId") shouldBe "feb981fd-ca5b-45bf-95de-2f02a8f95e64"
            fst.getAs[String]("schoolId") shouldBe "33e5e113-fb5e-4341-9e27-a3efe3d00b88"
            fst.getAs[String]("subjectName") shouldBe "MATHS"
            fst.getAs[String]("gradeId") shouldBe "4f0763cf-c429-4d8c-b76e-10c6a1066a21"
            fst.getAs[Int]("gradeLevel") shouldBe 7
            fst.getAs[String]("componentId") shouldBe "609fdbf8-dd37-4a5a-8db0-1f4897e207eb"
            fst.getAs[String]("componentStatus") shouldBe "HIDDEN"
          }
        }
      )
    }
  }

  test("handle PacingGuideCreatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """[
              |	{
              |		"key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              |		"value": {
              |			"headers": {
              |				"eventType": "PacingGuideCreatedEvent",
              |				"tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |			},
              |			"body": {
              |				"id": "8965370d-0b6a-4a36-8f41-95ff2868af5e",
              |				"classId": "effa3e2a-cfdd-4982-b365-c2e61b6c8675",
              |				"courseId": "edb3216b-c96d-44ed-b663-7a8f1213616f",
              |				"instructionalPlanId": "aab3216b-c96d-44ed-b663-7a8f1213616f",
              |				"activities": [
              |					{
              |						"activity": {
              |							"order": 1,
              |							"metadata": {
              |								"tags": [
              |									{
              |										"key": "Grade",
              |										"values": [
              |											"7"
              |										],
              |										"attributes": [
              |											{
              |												"value": "7",
              |												"color": "lightGray"
              |											}
              |										],
              |										"type": "LIST"
              |									},
              |									{
              |										"key": "Domain",
              |										"values": [
              |											{
              |												"name": "Measurement, Data and Statistics",
              |												"icon": "MeasurementDataAndStatistics",
              |												"color": "lightPurple"
              |											}
              |										],
              |										"type": "DOMAIN"
              |									}
              |								],
              |								"version": "1"
              |							},
              |							"mappedLearningOutcomes": [
              |								{
              |									"id": 14973,
              |									"type": "SKILL",
              |									"curriculumId": 392027,
              |									"gradeId": 596550,
              |									"subjectId": 571671
              |								}
              |							],
              |							"isJointParentActivity": false,
              |							"type": "ACTIVITY",
              |							"id": "c3464b3f-8132-4b28-a37f-000000028858",
              |							"settings": {
              |								"pacing": "UN_LOCKED",
              |								"hideWhenPublishing": false,
              |								"isOptional": false
              |							},
              |							"associatedLevel": null,
              |							"legacyId": 28858
              |						},
              |						"associations": [
              |							{
              |								"id": "7809cbf2-1c39-497a-a468-2c2fd12a91c4",
              |								"startDate": "2023-01-01",
              |								"endDate": "2023-01-15",
              |								"type": "TERM",
              |								"label": "Term"
              |							},
              |							{
              |								"id": "7809cbf2-1c39-497a-a468-2c2fd12a91c4",
              |								"startDate": "2023-01-01",
              |								"endDate": "2023-01-15",
              |								"type": "WEEK",
              |								"label": "Week"
              |							},
              |							{
              |								"id": "cbb01725-2407-44ff-8fac-d337573845bf",
              |								"type": "UNIT",
              |								"label": "Unit"
              |							}
              |						],
              |						"activityState": {
              |							"lockedStatus": "UN_LOCKED",
              |							"highlighted": false,
              |							"modifiedOn": "2024-02-26T10:33:10.336",
              |							"modifiedBy": null
              |						}
              |					}
              |				],
              |				"eventType": "PacingGuideCreatedEvent",
              |				"occurredOn": 1708929191264
              |			}
              |		},
              |		"timestamp": "2023-10-26 16:23:46.609"
              |	}
              |]""".stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "authoring-course-pacing-guide-created-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("authoring-course-pacing-guide-created-sink is not found"))

          val expectedColumns = SortedSet(
            "id",
            "classId",
            "courseId",
            "instructionalPlanId",
            "academicYearId",
            "academicCalendarId",
            "activities",
            "eventType",
            "tenantId",
            "occurredOn",
            "eventDateDw",
            "loadtime"
          )

          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }

  test("handle PacingGuideUpdatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """[
              |	{
              |		"key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              |		"value": {
              |			"headers": {
              |				"eventType": "PacingGuideUpdatedEvent",
              |				"tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |			},
              |			"body": {
              |				"id": "8965370d-0b6a-4a36-8f41-95ff2868af5e",
              |				"classId": "effa3e2a-cfdd-4982-b365-c2e61b6c8675",
              |				"courseId": "edb3216b-c96d-44ed-b663-7a8f1213616f",
              |				"instructionalPlanId": "aab3216b-c96d-44ed-b663-7a8f1213616f",
              |				"activities": [
              |					{
              |						"activity": {
              |							"order": 1,
              |							"metadata": {
              |								"tags": [
              |									{
              |										"key": "Grade",
              |										"values": [
              |											"7"
              |										],
              |										"attributes": [
              |											{
              |												"value": "7",
              |												"color": "lightGray"
              |											}
              |										],
              |										"type": "LIST"
              |									},
              |									{
              |										"key": "Domain",
              |										"values": [
              |											{
              |												"name": "Measurement, Data and Statistics",
              |												"icon": "MeasurementDataAndStatistics",
              |												"color": "lightPurple"
              |											}
              |										],
              |										"type": "DOMAIN"
              |									}
              |								],
              |								"version": "1"
              |							},
              |							"mappedLearningOutcomes": [
              |								{
              |									"id": 14973,
              |									"type": "SKILL",
              |									"curriculumId": 392027,
              |									"gradeId": 596550,
              |									"subjectId": 571671
              |								}
              |							],
              |							"isJointParentActivity": false,
              |							"type": "ACTIVITY",
              |							"id": "c3464b3f-8132-4b28-a37f-000000028858",
              |							"settings": {
              |								"pacing": "UN_LOCKED",
              |								"hideWhenPublishing": false,
              |								"isOptional": false
              |							},
              |							"associatedLevel": null,
              |							"legacyId": 28858
              |						},
              |						"associations": [
              |							{
              |								"id": "7809cbf2-1c39-497a-a468-2c2fd12a91c4",
              |								"startDate": "2023-01-01",
              |								"endDate": "2023-01-15",
              |								"type": "TERM",
              |								"label": "Term"
              |							},
              |							{
              |								"id": "7809cbf2-1c39-497a-a468-2c2fd12a91c4",
              |								"startDate": "2023-01-01",
              |								"endDate": "2023-01-15",
              |								"type": "WEEK",
              |								"label": "Week"
              |							},
              |							{
              |								"id": "cbb01725-2407-44ff-8fac-d337573845bf",
              |								"type": "UNIT",
              |								"label": "Unit"
              |							}
              |						],
              |						"activityState": {
              |							"lockedStatus": "UN_LOCKED",
              |							"highlighted": false,
              |							"modifiedOn": "2024-02-26T10:33:10.336",
              |							"modifiedBy": null
              |						}
              |					}
              |				],
              |				"eventType": "PacingGuideUpdatedEvent",
              |				"occurredOn": 1708929191264
              |			}
              |		},
              |		"timestamp": "2023-10-26 16:23:46.609"
              |	}
              |]""".stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "authoring-course-pacing-guide-updated-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("authoring-course-pacing-guide-updated-sink is not found"))

          val expectedColumns = SortedSet(
            "id",
            "classId",
            "courseId",
            "instructionalPlanId",
            "academicYearId",
            "academicCalendarId",
            "activities",
            "eventType",
            "tenantId",
            "occurredOn",
            "eventDateDw",
            "loadtime"
          )

          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }

  test("handle PacingGuideDeletedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """[
              |	{
              |		"key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              |		"value": {
              |			"headers": {
              |				"eventType": "PacingGuideDeletedEvent",
              |				"tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |			},
              |			"body": {
              |				"id": "8965370d-0b6a-4a36-8f41-95ff2868af5e",
              |				"classId": "effa3e2a-cfdd-4982-b365-c2e61b6c8675",
              |				"courseId": "edb3216b-c96d-44ed-b663-7a8f1213616f",
              |				"instructionalPlanId": "aab3216b-c96d-44ed-b663-7a8f1213616f",
              |				"activities": [
              |					{
              |						"activity": {
              |							"order": 1,
              |							"metadata": {
              |								"tags": [
              |									{
              |										"key": "Grade",
              |										"values": [
              |											"7"
              |										],
              |										"attributes": [
              |											{
              |												"value": "7",
              |												"color": "lightGray"
              |											}
              |										],
              |										"type": "LIST"
              |									},
              |									{
              |										"key": "Domain",
              |										"values": [
              |											{
              |												"name": "Measurement, Data and Statistics",
              |												"icon": "MeasurementDataAndStatistics",
              |												"color": "lightPurple"
              |											}
              |										],
              |										"type": "DOMAIN"
              |									}
              |								],
              |								"version": "1"
              |							},
              |							"mappedLearningOutcomes": [],
              |							"isJointParentActivity": false,
              |							"type": "ACTIVITY",
              |							"id": "c3464b3f-8132-4b28-a37f-000000028858",
              |							"settings": {
              |								"pacing": "UN_LOCKED",
              |								"hideWhenPublishing": false,
              |								"isOptional": false
              |							},
              |							"associatedLevel": null,
              |							"legacyId": 28858
              |						},
              |						"associations": [
              |							{
              |								"id": "7809cbf2-1c39-497a-a468-2c2fd12a91c4",
              |								"startDate": "2023-01-01",
              |								"endDate": "2023-01-15",
              |								"type": "TERM",
              |								"label": "Term"
              |							}
              |						],
              |						"activityState": {
              |							"lockedStatus": "UN_LOCKED",
              |							"highlighted": false,
              |							"modifiedOn": "2024-02-26T10:33:10.336",
              |							"modifiedBy": null
              |						}
              |					}
              |				],
              |				"eventType": "PacingGuideDeletedEvent",
              |				"occurredOn": 1708929191264
              |			}
              |		},
              |		"timestamp": "2023-10-26 16:23:46.609"
              |	}
              |]""".stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "authoring-course-pacing-guide-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("authoring-course-pacing-guide-deleted-sink is not found"))

          val expectedColumns = SortedSet(
            "id",
            "classId",
            "courseId",
            "instructionalPlanId",
            "activities",
            "eventType",
            "tenantId",
            "occurredOn",
            "eventDateDw",
            "loadtime"
          )

          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }
}
