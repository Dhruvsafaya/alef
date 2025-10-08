package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers

class AllRawEventsCourseSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer = new AllRawEvents(spark)
  }

  test("handle ClassCreated events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "schoolId",
        "loadtime",
        "teachers",
        "academicYearId",
        "occurredOn",
        "status",
        "sectionId",
        "subjectCategory",
        "material",
        "title",
        "tenantId",
        "classId",
        "gradeId",
        "settings",
        "sourceId",
        "categoryId",
        "organisationGlobal",
        "academicCalendarId"
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
              |       "eventType":"ClassCreatedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |       "classId": "613768fa-0980-4a4e-9863-78be6607d7fb",
              |       "title": "Test",
              |       "schoolId": "school-id",
              |       "gradeId": "grade-id",
              |       "sectionId": null,
              |       "academicYearId": "acad-id",
              |       "subjectCategory": "Maths",
              |       "material": {
              |       "curriculum":563622,
              |       "grade":333938,
              |       "subject":742980,
              |        "year":2020,
              |        "instructionalPlanId": "f92c4dda-0ad9-464b-a005-67d581ada1ed"
              |        },
              |       "settings": {
              |         "tutorDhabiEnabled":false,
              |         "languageDirection":"RTL",
              |         "online":true,
              |         "practice":true,
              |         "studentProgressView": null
              |        },
              |        "teachers": ["tecaher1"],
              |        "status": "ACTIVE",
              |        "sourceId": null,
              |        "occurredOn": 1592122860957,
              |        "categoryId": "a92c4dda-0ad9-464b-a005-67d581ad01ed"
              |     }
              | },
              | "timestamp": "2020-02-20 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "class-modified-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "ClassCreatedEvent"
            fst.getAs[String]("classId") shouldBe "613768fa-0980-4a4e-9863-78be6607d7fb"
            fst.getAs[String]("title") shouldBe "Test"
            fst.getAs[String]("categoryId") shouldBe "a92c4dda-0ad9-464b-a005-67d581ad01ed"
            val materialDf = df.select(col("material.*"))
            val materialRow = materialDf.first()
            materialRow.getAs[String]("curriculum") shouldBe "563622"
            materialRow.getAs[String]("grade") shouldBe "333938"
            materialRow.getAs[String]("subject") shouldBe "742980"
            materialRow.getAs[Int]("year") shouldBe 2020
            materialRow.getAs[String]("instructionalPlanId") shouldBe "f92c4dda-0ad9-464b-a005-67d581ada1ed"

            val settingsDf = df.select(col("settings.*"))
            val settingsRow = settingsDf.first()
            settingsRow.getAs[Boolean]("tutorDhabiEnabled") shouldBe false
            settingsRow.getAs[String]("studentProgressView") shouldBe null
          }
        }
      )
    }
  }

  test("handle ClassUpdatedEvent") {
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
                    |      "eventType":"ClassUpdatedEvent",
                    |      "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |   },
                    |   "body": {
                    |  "title": "hhgg",
                    |  "schoolId": "0a1176f3-f949-4700-9a8c-9231265bb54c",
                    |  "gradeId": "b306bd2c-3a30-427e-a5de-2c3584261443",
                    |  "gradeLevel": 6,
                    |  "sectionId": "cbb041ba-76ef-4023-aa30-7b9643bc0bf5",
                    |  "academicYearId": "0fbb87c6-6c80-4c9a-a994-e9165e0dd808",
                    |  "subjectCategory": "Art",
                    |  "material": {
                    |    "curriculum": "392027",
                    |    "grade": "363684",
                    |    "subject": "658224",
                    |    "year": 2020,
                    |    "instructionalPlanId": "f92c4dda-0ad9-464b-a005-67d581ada1ed"
                    |  },
                    |  "settings": {
                    |    "tutorDhabiEnabled": true,
                    |    "languageDirection": "LTR",
                    |    "online": true,
                    |    "practice": true,
                    |    "studentProgressView": "GRADE_LEVEL"
                    |  },
                    |  "teachers": [
                    |    "70a24bfe-27c2-4600-b1f5-1f9db2a38e9f",
                    |    "753005c6-24d2-4b21-9335-60ce8e15fee0"
                    |  ],
                    |  "status": "ACTIVE",
                    |  "sourceId": "909",
                    |  "classId": "fbf6fc05-a8f2-4bcb-9b7e-518b9194daa8",
                    |  "occurredOn": 12412412512,
                    |  "categoryId": "a92c4dda-0ad9-464b-a005-67d581ad01ed"
                    |}
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
            .find(_.name == "class-modified-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("class-modified-sink is not found"))

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "ClassUpdatedEvent"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("title") shouldBe "hhgg"
          fst.getAs[String]("schoolId") shouldBe "0a1176f3-f949-4700-9a8c-9231265bb54c"
          fst.getAs[String]("gradeId") shouldBe "b306bd2c-3a30-427e-a5de-2c3584261443"
          fst.getAs[String]("sectionId") shouldBe "cbb041ba-76ef-4023-aa30-7b9643bc0bf5"
          fst.getAs[String]("academicYearId") shouldBe "0fbb87c6-6c80-4c9a-a994-e9165e0dd808"
          fst.getAs[String]("subjectCategory") shouldBe "Art"
          fst.getAs[String]("status") shouldBe "ACTIVE"
          fst.getAs[String]("sourceId") shouldBe "909"
          fst.getAs[String]("classId") shouldBe "fbf6fc05-a8f2-4bcb-9b7e-518b9194daa8"
          fst.getAs[String]("occurredOn") shouldBe "1970-05-24 15:53:32.512"
          fst.getAs[String]("categoryId") shouldBe "a92c4dda-0ad9-464b-a005-67d581ad01ed"

          val classMaterial = fst.getAs[GenericRowWithSchema]("material")
          classMaterial.getAs[String]("curriculum") shouldBe "392027"
          classMaterial.getAs[String]("grade") shouldBe "363684"
          classMaterial.getAs[String]("subject") shouldBe "658224"
          classMaterial.getAs[Int]("year") shouldBe 2020
          classMaterial.getAs[String]("instructionalPlanId") shouldBe "f92c4dda-0ad9-464b-a005-67d581ada1ed"

          val settings = fst.getAs[GenericRowWithSchema]("settings")
          settings.getAs[String]("languageDirection") shouldBe "LTR"
          settings.getAs[Boolean]("tutorDhabiEnabled") shouldBe true
          settings.getAs[Boolean]("online") shouldBe true
          settings.getAs[Boolean]("practice") shouldBe true
          settings.getAs[String]("studentProgressView") shouldBe "GRADE_LEVEL"
        }
      )
    }
  }

  val commonExpectedColumns = Set(
    "eventType",
    "eventDateDw",
    "loadtime",
    "occurredOn",
    "tenantId",
  )

  test("handle Student Enrollment events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "userId",
        "classId"
      ) ++ commonExpectedColumns

      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"StudentEnrolledInClassEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "classId": "613768fa-0980-4a4e-9863-78be6607d7fb",
                    |       "userId": "student1",
                    |        "occurredOn": 1592122860957
                    |     }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "student-class-association-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "StudentEnrolledInClassEvent"
            fst.getAs[String]("classId") shouldBe "613768fa-0980-4a4e-9863-78be6607d7fb"
            fst.getAs[String]("userId") shouldBe "student1"
          }
        }
      )
    }
  }

  val expectedClassCategoryColumn = Set(
    "classCategoryId",
    "name",
    "organisationGlobal"
  ) ++ commonExpectedColumns

  test("handle ClassCategoryCreatedEvent events") {
    new Setup {


      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"ClassCategoryCreatedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "classCategoryId": "613768fa-0980-4a4e-9863-78be6607d7fb",
                    |       "name": "Advance",
                    |       "occurredOn": 1592122860957
                    |     }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "class-category-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedClassCategoryColumn)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "ClassCategoryCreatedEvent"
            fst.getAs[String]("classCategoryId") shouldBe "613768fa-0980-4a4e-9863-78be6607d7fb"
            fst.getAs[String]("name") shouldBe "Advance"
            fst.getAs[String]("occurredOn") shouldBe "2020-06-14 08:21:00.957"
          }
        }
      )
    }
  }

  test("handle ClassCategoryUpdatedEvent events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"ClassCategoryUpdatedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "classCategoryId": "613768fa-0980-4a4e-9863-78be6607d7fb",
                    |       "name": "Advance",
                    |       "occurredOn": 1592122860957
                    |     }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "class-category-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedClassCategoryColumn)
          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "ClassCategoryUpdatedEvent"
            fst.getAs[String]("classCategoryId") shouldBe "613768fa-0980-4a4e-9863-78be6607d7fb"
            fst.getAs[String]("name") shouldBe "Advance"
            fst.getAs[String]("occurredOn") shouldBe "2020-06-14 08:21:00.957"
          }
        }
      )
    }
  }
}
