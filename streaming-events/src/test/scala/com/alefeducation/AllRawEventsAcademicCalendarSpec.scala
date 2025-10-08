package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.scalatest.matchers.should.Matchers

class AllRawEventsAcademicCalendarSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: AllRawEvents = new AllRawEvents(spark)
  }

  test("handle AcademicCalendarCreatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "uuid",
        "title",
        "default",
        "type",
        "academicYearId",
        "schoolId",
        "organization",
        "teachingPeriods",
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
              |		  "eventType": "AcademicCalendarCreatedEvent",
              |		  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |	  },
              |	  "body": {
              |		  "occurredOn": 1710314413446,
              |		  "uuid": "40ed5f87-bf30-4c58-bd55-1fc828eeee89",
              |		  "title": "HCZ Org Calendar",
              |		  "default": true,
              |		  "type": "ORGANIZATION",
              |		  "academicYearId": "84409e2c-26fd-4afc-9eef-ab18e4f31e08",
              |		  "schoolId": null,
              |		  "organization": "HCZ",
              |		  "teachingPeriods": [
              |			  {
              |				  "uuid": "a5d4bd39-f5b3-4f6e-bd5f-f311a4cc12a4",
              |				  "title": "Period I",
              |				  "startDate": 1704657600000,
              |				  "endDate": 1709150400000,
              |				  "current": false,
              |				  "createdBy": "30f74d8c-d679-4f9a-b115-032020dab51a",
              |				  "updatedBy": "30f74d8c-d679-4f9a-b115-032020dab51a",
              |				  "createdOn": 1710314413443,
              |				  "updatedOn": 1710314413443
              |			  }
              |		  ],
              |		  "createdBy": "30f74d8c-d679-4f9a-b115-032020dab51a",
              |		  "updatedBy": "30f74d8c-d679-4f9a-b115-032020dab51a",
              |		  "createdOn": 1710314413443,
              |		  "updatedOn": 1710314413443
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
          val dfOpt = sinks.find(_.name == "academic-calendar-created-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "AcademicCalendarCreatedEvent"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-13 07:20:13.446"
            fst.getAs[String]("uuid") shouldBe "40ed5f87-bf30-4c58-bd55-1fc828eeee89"
            fst.getAs[String]("title") shouldBe "HCZ Org Calendar"
            fst.getAs[Boolean]("default") shouldBe true
            fst.getAs[String]("type") shouldBe "ORGANIZATION"
            fst.getAs[String]("academicYearId") shouldBe "84409e2c-26fd-4afc-9eef-ab18e4f31e08"
            fst.getAs[String]("schoolId") shouldBe null
            fst.getAs[String]("organization") shouldBe "HCZ"
            fst.getAs[String]("createdBy") shouldBe "30f74d8c-d679-4f9a-b115-032020dab51a"
            fst.getAs[String]("updatedBy") shouldBe "30f74d8c-d679-4f9a-b115-032020dab51a"
            fst.getAs[Long]("createdOn") shouldBe 1710314413443L
            fst.getAs[Long]("updatedOn") shouldBe 1710314413443L

            val period = fst.getAs[Seq[Row]]("teachingPeriods").head.asInstanceOf[GenericRowWithSchema]
            period.getAs[String]("uuid") shouldBe "a5d4bd39-f5b3-4f6e-bd5f-f311a4cc12a4"
            period.getAs[String]("title") shouldBe "Period I"
            period.getAs[Long]("startDate") shouldBe 1704657600000L
            period.getAs[Long]("endDate") shouldBe 1709150400000L
            period.getAs[Boolean]("current") shouldBe false
            period.getAs[String]("createdBy") shouldBe "30f74d8c-d679-4f9a-b115-032020dab51a"
            period.getAs[String]("updatedBy") shouldBe "30f74d8c-d679-4f9a-b115-032020dab51a"
            period.getAs[Long]("createdOn") shouldBe 1710314413443L
            period.getAs[Long]("updatedOn") shouldBe 1710314413443L
          }
        }
      )
    }
  }

  test("handle AcademicCalendarUpdatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "uuid",
        "title",
        "default",
        "type",
        "academicYearId",
        "schoolId",
        "organization",
        "teachingPeriods",
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
              |		  "eventType": "AcademicCalendarUpdatedEvent",
              |		  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |	  },
              |	  "body": {
              |		  "occurredOn": 1710744411350,
              |		  "uuid": "6d3384d0-2233-4007-b744-2a0d3b84f3e9",
              |		  "title": "One more bulk calendar",
              |		  "default": false,
              |		  "type": "ORGANIZATION",
              |		  "academicYearId": "84409e2c-26fd-4afc-9eef-ab18e4f31e08",
              |		  "schoolId": null,
              |		  "organization": "HCZ",
              |		  "teachingPeriods": [
              |		 	  {
              |		 	  	"uuid": "4ea88e03-74e0-4a0e-8688-2b4cb8ce0822",
              |		 	  	"title": "Period I",
              |		 	  	"startDate": 1709409600000,
              |		 	  	"endDate": 1710360000000,
              |		 	  	"current": false,
              |		 	  	"createdBy": "30f74d8c-d679-4f9a-b115-032020dab51a",
              |		 	  	"updatedBy": "30f74d8c-d679-4f9a-b115-032020dab51a",
              |		 	  	"createdOn": 1710744411350,
              |		 	  	"updatedOn": 1710744411350
              |		 	  },
              |		 	  {
              |		 	  	"uuid": "e3030be3-7657-4c5c-afec-4b4e47897a83",
              |		 	  	"title": "Period II",
              |		 	  	"startDate": 1710619200000,
              |		 	  	"endDate": 1711828800000,
              |		 	  	"current": true,
              |		 	  	"createdBy": "30f74d8c-d679-4f9a-b115-032020dab51a",
              |		 	  	"updatedBy": "30f74d8c-d679-4f9a-b115-032020dab51a",
              |		 	  	"createdOn": 1710744411350,
              |		 	  	"updatedOn": 1710744411350
              |		 	  }
              |		  ],
              |		  "createdBy": "30f74d8c-d679-4f9a-b115-032020dab51a",
              |		  "updatedBy": "30f74d8c-d679-4f9a-b115-032020dab51a",
              |		  "createdOn": 1710492154000,
              |		  "updatedOn": 1710492154000
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
          val dfOpt = sinks.find(_.name == "academic-calendar-updated-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "AcademicCalendarUpdatedEvent"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-18 06:46:51.350"
            fst.getAs[String]("uuid") shouldBe "6d3384d0-2233-4007-b744-2a0d3b84f3e9"
            fst.getAs[String]("title") shouldBe "One more bulk calendar"
            fst.getAs[Boolean]("default") shouldBe false
            fst.getAs[String]("type") shouldBe "ORGANIZATION"
            fst.getAs[String]("academicYearId") shouldBe "84409e2c-26fd-4afc-9eef-ab18e4f31e08"
            fst.getAs[String]("schoolId") shouldBe null
            fst.getAs[String]("organization") shouldBe "HCZ"
            fst.getAs[String]("createdBy") shouldBe "30f74d8c-d679-4f9a-b115-032020dab51a"
            fst.getAs[String]("updatedBy") shouldBe "30f74d8c-d679-4f9a-b115-032020dab51a"
            fst.getAs[Long]("createdOn") shouldBe 1710492154000L
            fst.getAs[Long]("updatedOn") shouldBe 1710492154000L

            val period = fst.getAs[Seq[Row]]("teachingPeriods").head.asInstanceOf[GenericRowWithSchema]
            period.getAs[String]("uuid") shouldBe "4ea88e03-74e0-4a0e-8688-2b4cb8ce0822"
            period.getAs[String]("title") shouldBe "Period I"
            period.getAs[Long]("startDate") shouldBe 1709409600000L
            period.getAs[Long]("endDate") shouldBe 1710360000000L
            period.getAs[Boolean]("current") shouldBe false
            period.getAs[String]("createdBy") shouldBe "30f74d8c-d679-4f9a-b115-032020dab51a"
            period.getAs[String]("updatedBy") shouldBe "30f74d8c-d679-4f9a-b115-032020dab51a"
            period.getAs[Long]("createdOn") shouldBe 1710744411350L
            period.getAs[Long]("updatedOn") shouldBe 1710744411350L
          }
        }
      )
    }
  }

  test("handle AcademicCalendarDeletedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "uuid",
        "title",
        "default",
        "type",
        "academicYearId",
        "schoolId",
        "organization",
        "teachingPeriods",
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
              |		  "eventType": "AcademicCalendarDeletedEvent",
              |		  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |	  },
              |	  "body": {
              |		  "occurredOn": 1710744411350,
              |		  "uuid": "6d3384d0-2233-4007-b744-2a0d3b84f3e9",
              |		  "title": "One more bulk calendar",
              |		  "default": false,
              |		  "type": "ORGANIZATION",
              |		  "academicYearId": "84409e2c-26fd-4afc-9eef-ab18e4f31e08",
              |		  "schoolId": null,
              |		  "organization": "HCZ",
              |		  "teachingPeriods": [],
              |		  "createdBy": "30f74d8c-d679-4f9a-b115-032020dab51a",
              |		  "updatedBy": "30f74d8c-d679-4f9a-b115-032020dab51a",
              |		  "createdOn": 1710492154000,
              |		  "updatedOn": 1710492154000
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
          val dfOpt = sinks.find(_.name == "academic-calendar-deleted-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "AcademicCalendarDeletedEvent"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-18 06:46:51.350"
            fst.getAs[String]("uuid") shouldBe "6d3384d0-2233-4007-b744-2a0d3b84f3e9"
            fst.getAs[String]("title") shouldBe "One more bulk calendar"
            fst.getAs[Boolean]("default") shouldBe false
            fst.getAs[String]("type") shouldBe "ORGANIZATION"
            fst.getAs[String]("academicYearId") shouldBe "84409e2c-26fd-4afc-9eef-ab18e4f31e08"
            fst.getAs[String]("schoolId") shouldBe null
            fst.getAs[String]("organization") shouldBe "HCZ"
            fst.getAs[String]("createdBy") shouldBe "30f74d8c-d679-4f9a-b115-032020dab51a"
            fst.getAs[String]("updatedBy") shouldBe "30f74d8c-d679-4f9a-b115-032020dab51a"
            fst.getAs[Long]("createdOn") shouldBe 1710492154000L
            fst.getAs[Long]("updatedOn") shouldBe 1710492154000L
          }
        }
      )
    }
  }

}
