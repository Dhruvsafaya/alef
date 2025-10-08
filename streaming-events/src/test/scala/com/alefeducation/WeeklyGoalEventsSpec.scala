package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.should.Matchers

class WeeklyGoalEventsSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer = new WeeklyGoalEvents(spark)
  }

  test("handle WeeklyGoalCreated events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "goalId",
        "weeklyGoalId",
        "occurredOn",
        "tenantId",
        "classId",
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
                    |       "eventType":"WeeklyGoalCreated",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
                    |       "studentId": "546daba6-677a-4110-8843-54aaacb549b9",
                    |       "classId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
                    |       "goalId": "c38e0c92-68d0-4f96-a677-d169a0ae0d68",
                    |       "occurredOn": 1624426404921
                    |     }
                    | },
                    | "timestamp": "2022-05-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "student-weekly-goal-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "WeeklyGoalCreated"
            fst.getAs[String]("weeklyGoalId") shouldBe "830a4d57-6e58-4554-ad71-f82e2cf9c6b3"
            fst.getAs[String]("studentId") shouldBe "546daba6-677a-4110-8843-54aaacb549b9"
            fst.getAs[String]("classId") shouldBe "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f"
            fst.getAs[String]("goalId") shouldBe "c38e0c92-68d0-4f96-a677-d169a0ae0d68"
            fst.getAs[String]("occurredOn") shouldBe "2021-06-23 05:33:24.921"
          }
        }
      )
    }
  }

  test("handle WeeklyGoalProgress events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "weeklyGoalId",
        "completedActivity",
        "occurredOn",
        "status",
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
                    |    "headers":{
                    |       "eventType":"WeeklyGoalProgress",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
                    |       "completedActivity": "B0AFBAMCAQAPDg0MCwoJgA==",
                    |       "status": "Ongoing",
                    |       "occurredOn": 1624426404921
                    |     }
                    | },
                    | "timestamp": "2022-05-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "student-weekly-goal-activity-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "WeeklyGoalProgress"
            fst.getAs[String]("weeklyGoalId") shouldBe "830a4d57-6e58-4554-ad71-f82e2cf9c6b3"
            fst.getAs[String]("status") shouldBe "Ongoing"
            fst.getAs[String]("completedActivity") shouldBe "B0AFBAMCAQAPDg0MCwoJgA=="
            fst.getAs[String]("occurredOn") shouldBe "2021-06-23 05:33:24.921"
          }
        }
      )
    }
  }

  test("handle WeeklyGoalCompleted events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "goalId",
        "weeklyGoalId",
        "occurredOn",
        "tenantId",
        "classId",
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
                    |       "eventType":"WeeklyGoalCompleted",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
                    |       "studentId": "546daba6-677a-4110-8843-54aaacb549b9",
                    |       "classId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
                    |       "goalId": "c38e0c92-68d0-4f96-a677-d169a0ae0d68",
                    |       "occurredOn": 1624426404921
                    |     }
                    | },
                    | "timestamp": "2022-05-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "student-weekly-goal-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "WeeklyGoalCompleted"
            fst.getAs[String]("weeklyGoalId") shouldBe "830a4d57-6e58-4554-ad71-f82e2cf9c6b3"
            fst.getAs[String]("occurredOn") shouldBe "2021-06-23 05:33:24.921"
          }
        }
      )
    }
  }

  test("handle WeeklyGoalStarEarned events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "stars",
        "loadtime",
        "academicYearId",
        "weeklyGoalId",
        "occurredOn",
        "tenantId",
        "classId",
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
                    |       "eventType":"WeeklyGoalStarEarned",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
                    |       "studentId": "546daba6-677a-4110-8843-54aaacb549b9",
                    |       "classId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
                    |       "stars": 6,
                    |       "academicYearId": "911bb80a-c012-4963-8ab4-23e9050183fd",
                    |       "occurredOn": 1624426404921
                    |     }
                    | },
                    | "timestamp": "2022-05-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "student-weekly-goal-star-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "WeeklyGoalStarEarned"
            fst.getAs[String]("weeklyGoalId") shouldBe "830a4d57-6e58-4554-ad71-f82e2cf9c6b3"
            fst.getAs[String]("studentId") shouldBe "546daba6-677a-4110-8843-54aaacb549b9"
            fst.getAs[String]("classId") shouldBe "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f"
            fst.getAs[Int]("stars") shouldBe 6
            fst.getAs[String]("academicYearId") shouldBe "911bb80a-c012-4963-8ab4-23e9050183fd"
            fst.getAs[String]("occurredOn") shouldBe "2021-06-23 05:33:24.921"
          }
        }
      )
    }
  }
}
