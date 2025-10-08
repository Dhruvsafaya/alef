package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.should.Matchers

class AllRawEventsAcademicYearSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: AllRawEvents = new AllRawEvents(spark)
  }

  test("handle AcademicYearCreatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "uuid",
        "type",
        "status",
        "startDate",
        "endDate",
        "schoolId",
        "organization",
        "createdBy",
        "updatedBy",
        "createdOn",
        "updatedOn",
        "tenantId"
      )
      val fixtures: List[SparkFixture] = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |	  "headers": {
              |		  "eventType": "AcademicYearCreatedEvent",
              |		  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |	  },
              |	  "body": {
              |		  "occurredOn": 1710311338448,
              |		  "uuid": "e98cee21-3b48-4e90-94b9-450d939f10ee",
              |		  "type": "SCHOOL",
              |		  "status": "CURRENT",
              |		  "startDate": 1567935191123,
              |		  "endDate": 1599557591124,
              |		  "schoolId": "abd37289-ce2b-4139-9ded-c79b4a96b612",
              |		  "organization": "MOEUAE",
              |		  "createdBy": "59b5c790-a676-4752-bc71-a96b392ec6db",
              |		  "updatedBy": "59b5c790-a676-4752-bc71-a96b392ec6db",
              |		  "createdOn": 1710311338407,
              |		  "updatedOn": 1710311338407
              |	}
              |},
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "academic-year-created-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "AcademicYearCreatedEvent"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-13 06:28:58.448"
            fst.getAs[String]("uuid") shouldBe "e98cee21-3b48-4e90-94b9-450d939f10ee"
            fst.getAs[String]("type") shouldBe "SCHOOL"
            fst.getAs[String]("status") shouldBe "CURRENT"
            fst.getAs[Long]("startDate") shouldBe 1567935191123L
            fst.getAs[Long]("endDate") shouldBe 1599557591124L
            fst.getAs[String]("schoolId") shouldBe "abd37289-ce2b-4139-9ded-c79b4a96b612"
            fst.getAs[String]("organization") shouldBe "MOEUAE"
            fst.getAs[String]("createdBy") shouldBe "59b5c790-a676-4752-bc71-a96b392ec6db"
            fst.getAs[String]("updatedBy") shouldBe "59b5c790-a676-4752-bc71-a96b392ec6db"
            fst.getAs[Long]("createdOn") shouldBe 1710311338407L
            fst.getAs[Long]("updatedOn") shouldBe 1710311338407L
          }
        }
      )
    }


  }

  test("handle AcademicYearUpdatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "uuid",
        "type",
        "status",
        "startDate",
        "endDate",
        "schoolId",
        "organization",
        "createdBy",
        "updatedBy",
        "createdOn",
        "updatedOn",
        "tenantId"
      )
      val fixtures: List[SparkFixture] = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |	  "headers": {
              |		  "eventType": "AcademicYearUpdatedEvent",
              |		  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |	  },
              |	  "body": {
              |		  "occurredOn": 1710329224964,
              |		  "uuid": "84409e2c-26fd-4afc-9eef-ab18e4f31e08",
              |		  "type": "ORGANIZATION",
              |		  "status": "CURRENT",
              |		  "startDate": 1704916800000,
              |		  "endDate": 1741046400000,
              |		  "schoolId": null,
              |		  "organization": "HCZ",
              |		  "createdBy": null,
              |		  "updatedBy": "30f74d8c-d679-4f9a-b115-032020dab51a",
              |		  "createdOn": null,
              |		  "updatedOn": 1710243685000
              |	  }
              |},
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "academic-year-updated-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "AcademicYearUpdatedEvent"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-13 11:27:04.964"
            fst.getAs[String]("uuid") shouldBe "84409e2c-26fd-4afc-9eef-ab18e4f31e08"
            fst.getAs[String]("type") shouldBe "ORGANIZATION"
            fst.getAs[String]("status") shouldBe "CURRENT"
            fst.getAs[Long]("startDate") shouldBe 1704916800000L
            fst.getAs[Long]("endDate") shouldBe 1741046400000L
            fst.getAs[String]("schoolId") shouldBe null
            fst.getAs[String]("organization") shouldBe "HCZ"
            fst.getAs[String]("createdBy") shouldBe null
            fst.getAs[String]("updatedBy") shouldBe "30f74d8c-d679-4f9a-b115-032020dab51a"
            fst.getAs[String]("createdOn") shouldBe null
            fst.getAs[Long]("updatedOn") shouldBe 1710243685000L
          }
        }
      )
    }
  }

  test("handle SchoolAcademicYearSwitched events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "tenantId",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "currentAcademicYearId",
        "currentAcademicYearType",
        "currentAcademicYearStartDate",
        "currentAcademicYearEndDate",
        "currentAcademicYearStatus",
        "oldAcademicYearId",
        "oldAcademicYearType",
        "oldAcademicYearStartDate",
        "oldAcademicYearEndDate",
        "organization",
        "schoolId",
        "updatedBy",
      )
      val fixtures: List[SparkFixture] = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |	  "headers": {
              |		  "eventType": "SchoolAcademicYearSwitched",
              |		  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |	  },
              |	  "body": {
              |      "currentAcademicYearId": "3f362bf4-6235-4fbc-a667-f94304e6cfc6",
              |      "currentAcademicYearType": "SCHOOL",
              |      "currentAcademicYearStartDate": 1571052193,
              |      "currentAcademicYearEndDate": 1571052193,
              |      "currentAcademicYearStatus": "CURRENT",
              |      "oldAcademicYearId": "3f362bf4-6235-4fbc-a667-f94304e6cfc6",
              |      "oldAcademicYearType": "ORGANISATION",
              |      "oldAcademicYearStartDate": 1571052193,
              |      "oldAcademicYearEndDate": 1571052193,
              |      "organization": "MOE",
              |      "schoolId": "1cece8e0-8185-4323-b33c-0a1e7b698af5",
              |      "updatedBy": "1a8d4564-c694-4b02-a5ff-4333f6b498d1",
              |      "occurredOn": 1710329224964
              |  }
              |},
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "admin-school-academic-year-switched-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "SchoolAcademicYearSwitched"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-13 11:27:04.964"
            fst.getAs[String]("currentAcademicYearId") shouldBe "3f362bf4-6235-4fbc-a667-f94304e6cfc6"
            fst.getAs[String]("currentAcademicYearType") shouldBe "SCHOOL"
            fst.getAs[Long]("currentAcademicYearStartDate") shouldBe 1571052193
            fst.getAs[Long]("currentAcademicYearEndDate") shouldBe 1571052193
            fst.getAs[String]("currentAcademicYearStatus") shouldBe "CURRENT"
            fst.getAs[String]("oldAcademicYearId") shouldBe "3f362bf4-6235-4fbc-a667-f94304e6cfc6"
            fst.getAs[String]("oldAcademicYearType") shouldBe "ORGANISATION"
            fst.getAs[Long]("oldAcademicYearStartDate") shouldBe 1571052193
            fst.getAs[Long]("oldAcademicYearEndDate") shouldBe 1571052193
            fst.getAs[String]("organization") shouldBe "MOE"
            fst.getAs[String]("schoolId") shouldBe "1cece8e0-8185-4323-b33c-0a1e7b698af5"
            fst.getAs[String]("updatedBy") shouldBe "1a8d4564-c694-4b02-a5ff-4333f6b498d1"
          }
        }
      )
    }
  }

  test("handle AcademicYearRollOverCompleted school events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "tenantId",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "id",
        "schoolId",
        "previousId",
      )
      val fixtures: List[SparkFixture] = List(
        SparkFixture(
          key = "raw-events-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |	  "headers": {
              |		  "eventType": "AcademicYearRollOverCompleted",
              |		  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |	  },
              |	  "body": {
              |      "id": "ay_id1",
              |      "schoolId": "school1",
              |      "previousId": "ay_id0",
              |      "occurredOn": 1710329224964
              |  }
              |},
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "admin-school-academic-year-roll-over-completed-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "AcademicYearRollOverCompleted"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-13 11:27:04.964"
            fst.getAs[String]("id") shouldBe "ay_id1"
            fst.getAs[String]("schoolId") shouldBe "school1"
            fst.getAs[String]("previousId") shouldBe "ay_id0"
          }
        }
      )
    }
  }

}
