package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.should.Matchers

class AllRawEventsChallengeGameSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer = new AllRawEvents(spark)
  }

  test("handle AlefGameChallengeProgressEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "tenantId",
        "id",
        "state",
        "studentId",
        "gameId",
        "schoolId",
        "grade",
        "score",
        "organization",
        "academicYearTag",
        "playerSessionId",
        "academicYearId",
        "occurredOn",
        "gameConfig"
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
              |       "eventType":"AlefGameChallengeProgressEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "id": "id1",
              |       "state": "JOINED",
              |       "studentId": "studentId1",
              |       "gameId": "gameId1",
              |       "schoolId": "schoolId1",
              |       "grade": "1",
              |       "score": 0,
              |       "organization": "organization1",
              |       "academicYearTag": "academicYearTag1",
              |       "academicYearId": "academicYearId1",
              |       "occurredOn": "1582026527000",
              |       "gameConfig": "{}"
              |     }
              | },
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
                    """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "challenge-game-progress-event-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("id") shouldBe "id1"
            fst.getAs[String]("state") shouldBe "JOINED"
            fst.getAs[String]("studentId") shouldBe "studentId1"
            fst.getAs[String]("gameId") shouldBe "gameId1"
            fst.getAs[String]("schoolId") shouldBe "schoolId1"
            fst.getAs[String]("grade") shouldBe "1"
            fst.getAs[Int]("score") shouldBe 0
            fst.getAs[String]("organization") shouldBe "organization1"
            fst.getAs[String]("academicYearTag") shouldBe "academicYearTag1"
            fst.getAs[String]("academicYearId") shouldBe "academicYearId1"
            fst.getAs[String]("playerSessionId") shouldBe null
            fst.getAs[String]("occurredOn") shouldBe "2020-02-18 11:48:47.000"
            fst.getAs[String]("gameConfig") shouldBe "{}"
          }
        }
      )
    }
  }

}