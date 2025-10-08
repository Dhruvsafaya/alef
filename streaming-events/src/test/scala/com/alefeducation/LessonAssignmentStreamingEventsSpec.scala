package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.should.Matchers

class LessonAssignmentStreamingEventsSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: LessonAssignmentStreamingEvents = new LessonAssignmentStreamingEvents(spark)
  }

  val commonColumns: Set[String] = Set(
    "eventType",
    "eventDateDw",
    "loadtime",
    "occurredOn",
    "tenantId",
    "classId",
    "contentId",
    "contentType",
    "assignedBy",
    "mloId"
  )

  val expectedStudentLessonColumns: Set[String] = commonColumns + "studentId" - "contentId"
  val expectedClassLessonColumns: Set[String] = commonColumns - "contentId"

  test("handle Student Lesson Assignment events") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = "lesson-assignment-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"LearnerLessonAssignedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "classId": "4a4e9863-0980-4a4e-9863-78be6607d7fb",
                    |       "mloId": "613768fa-0980-4a4e-9863-78be6607d7fb",
                    |       "contentType": "TEQ_1",
                    |       "assignedBy": "123768fa-0980-4a4e-9863-78be6607d7fb",
                    |       "studentId": "student1",
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
          val dfOpt = sinks.find(_.name == "student-lesson-assignment-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedStudentLessonColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "LearnerLessonAssignedEvent"
            fst.getAs[String]("classId") shouldBe "4a4e9863-0980-4a4e-9863-78be6607d7fb"
            fst.getAs[String]("contentType") shouldBe "TEQ_1"
            fst.getAs[String]("mloId") shouldBe "613768fa-0980-4a4e-9863-78be6607d7fb"
            fst.getAs[String]("studentId") shouldBe "student1"
          }
        }
      )
    }
  }

  test("handle Class Lesson Assignment events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "lesson-assignment-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"ClassLessonAssignedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "classId": "4a4e9863-0980-4a4e-9863-78be6607d7fb",
                    |       "mloId": "613768fa-0980-4a4e-9863-78be6607d7fb",
                    |       "contentType": "TEQ_1",
                    |       "assignedBy": "teacher1",
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
          val dfOpt = sinks.find(_.name == "class-lesson-assignment-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedClassLessonColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "ClassLessonAssignedEvent"
            fst.getAs[String]("classId") shouldBe "4a4e9863-0980-4a4e-9863-78be6607d7fb"
            fst.getAs[String]("contentType") shouldBe "TEQ_1"
            fst.getAs[String]("mloId") shouldBe "613768fa-0980-4a4e-9863-78be6607d7fb"
            fst.getAs[String]("assignedBy") shouldBe "teacher1"
          }
        }
      )
    }
  }
}
