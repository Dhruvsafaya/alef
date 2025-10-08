package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.should.Matchers

class AllRawEventsLearningPathSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer = new AllRawEvents(spark)
  }

  test("handle LevelCreatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "createdOn",
        "occurredOn",
        "loadtime",
        "tenantId",
        "id",
        "name",
        "status",
        "languageTypeScript",
        "experientialLearning",
        "tutorDhabiEnabled",
        "default",
        "schoolId",
        "classId",
        "schoolSubjectId",
        "academicYearId",
        "grade",
        "academicYear",
        "academicYear",
        "curriculumId",
        "curriculumGradeId",
        "curriculumSubjectId"
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
                    |       "eventType":"LevelCreatedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "id": "level_id",
                    |       "name": "level-name",
                    |       "status": "DRAFT",
                    |       "languageTypeScript": "R-T-L",
                    |       "experientialLearning": true,
                    |       "tutorDhabiEnabled": true,
                    |       "default": false,
                    |       "schoolId": "school-UUID",
                    |       "classId": "class-UUID",
                    |       "schoolSubjectId": null,
                    |       "academicYearId": "ay-UUID",
                    |       "grade": 7,
                    |       "academicYear": 2020,
                    |       "academicYear": 7,
                    |       "curriculumId": "53245",
                    |       "curriculumGradeId": "11223",
                    |       "curriculumSubjectId": "43215",
                    |       "createdOn": 1582026526000
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
          val dfOpt = sinks.find(_.name == "admin-learning-path-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "LevelCreatedEvent"
            fst.getAs[String]("id") shouldBe "level_id"
            fst.getAs[String]("schoolSubjectId") shouldBe null
            fst.getAs[String]("classId") shouldBe "class-UUID"
            fst.getAs[String]("occurredOn") shouldBe "2020-02-18 11:48:46.000"
          }
        }
      )
    }
  }
}
