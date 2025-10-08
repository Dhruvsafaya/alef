package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.should.Matchers

class AllRawEventsBadgeAwardedSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer = new AllRawEvents(spark)
  }

  test("handle StudentBadgeAwardedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "nextChallengeDescription",
        "tier",
        "schoolId",
        "loadtime",
        "badgeType",
        "badgeTypeId",
        "academicYearId",
        "occurredOn",
        "id",
        "awardDescription",
        "awardedMessage",
        "sectionId",
        "tenantId",
        "gradeId",
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
                    |    "headers":{
                    |       "eventType":"StudentBadgeAwardedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |	    "id": "457ae0bc-30b7-4ea9-b97b-7d6221dedbdf",
                    |	    "studentId": "14e9276f-2197-41bf-a2b2-d8237407cb2d",
                    |	    "badgeType": "MOUNTAIN",
                    |	    "badgeTypeId": "8b8cf877-c6e8-4450-aaaf-2333a92abb3f",
                    |	    "tier": "BRONZE",
                    |	    "nextChallengeDescription": "Receive more stars from your teacher to earn a Silver Badge.",
                    |	    "awardedMessage": "You have earned a Bronze Badge by receiving awarded stars.",
                    |	    "awardDescription": "Receive more stars from your teacher to earn a Bronze Badge.",
                    |	    "academicYearId": "0a3a5afd-3b50-447f-a937-7480133bde95",
                    |	    "occurredOn": 1684731280416,
                    |	    "sectionId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
                    |	    "gradeId": "df840323-9ba3-4ffa-a279-464e010fcdd0",
                    |	    "schoolId": "e73fa736-59ca-42df-ade6-87558c7df8c2"
                    |   }
                    | },
                    | "timestamp": "2023-04-27 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "badge-awarded-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          val fst = dfOpt.get.first()
          fst.getAs[String]("tenantId") shouldBe "tenantId"
          fst.getAs[String]("eventType") shouldBe "StudentBadgeAwardedEvent"
          fst.getAs[String]("id") shouldBe "457ae0bc-30b7-4ea9-b97b-7d6221dedbdf"
          fst.getAs[String]("studentId") shouldBe "14e9276f-2197-41bf-a2b2-d8237407cb2d"
          fst.getAs[String]("badgeType") shouldBe "MOUNTAIN"
          fst.getAs[String]("badgeTypeId") shouldBe "8b8cf877-c6e8-4450-aaaf-2333a92abb3f"
          fst.getAs[String]("tier") shouldBe "BRONZE"
          fst.getAs[String]("nextChallengeDescription") shouldBe "Receive more stars from your teacher to earn a Silver Badge."
          fst.getAs[String]("awardedMessage") shouldBe "You have earned a Bronze Badge by receiving awarded stars."
          fst.getAs[String]("awardDescription") shouldBe "Receive more stars from your teacher to earn a Bronze Badge."
          fst.getAs[String]("academicYearId") shouldBe "0a3a5afd-3b50-447f-a937-7480133bde95"
          fst.getAs[String]("occurredOn") shouldBe "2023-05-22 04:54:40.416"
          fst.getAs[String]("sectionId") shouldBe "fd605223-dbe9-426e-a8f4-67c76d6357c1"
          fst.getAs[String]("gradeId") shouldBe "df840323-9ba3-4ffa-a279-464e010fcdd0"
          fst.getAs[String]("schoolId") shouldBe "e73fa736-59ca-42df-ade6-87558c7df8c2"
        }
      )
    }
  }
}
