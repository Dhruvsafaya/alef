package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.should.Matchers

class AllRawEventsAwardedStarsSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer = new AllRawEvents(spark)
  }

  test("handle AwardedStar event") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "tenantId",
        "id",
        "schoolId",
        "grade",
        "gradeId",
        "section",
        "trimesterId",
        "classId",
        "subjectId",
        "learnerId",
        "teacherId",
        "categoryCode",
        "categoryLabelEn",
        "categoryLabelAr",
        "comment",
        "createdOn",
        "academicYearId",
        "stars"
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
                    |       "eventType":"AwardResource",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |        "id":"awardResource-1",
                    |        "schoolId":"school-1",
                    |        "grade":5,
                    |        "gradeId":"grade-5",
                    |        "section":"section-5",
                    |        "trimesterId":"trimester-2",
                    |        "classId":"class-id-1",
                    |        "subjectId":"subject-id-1",
                    |        "learnerId":"student-id-1",
                    |        "teacherId":"teacher-id-1",
                    |        "categoryCode":"A",
                    |        "categoryLabelEn":"ABC",
                    |        "categoryLabelAr":"أَلِف",
                    |        "comment":"Good",
                    |        "createdOn":1592122860957,
                    |        "academicYearId":"ay-1",
                    |        "stars":3
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
          val dfOpt = sinks.find(_.name == "award-resource-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "AwardResource"
            fst.getAs[String]("id") shouldBe "awardResource-1"
            fst.getAs[String]("schoolId") shouldBe "school-1"
            fst.getAs[Int]("grade") shouldBe 5
            fst.getAs[String]("gradeId") shouldBe "grade-5"
            fst.getAs[String]("section") shouldBe "section-5"
            fst.getAs[String]("trimesterId") shouldBe "trimester-2"
            fst.getAs[String]("classId") shouldBe "class-id-1"
            fst.getAs[String]("subjectId") shouldBe "subject-id-1"
            fst.getAs[String]("learnerId") shouldBe "student-id-1"
            fst.getAs[String]("teacherId") shouldBe "teacher-id-1"
            fst.getAs[String]("categoryCode") shouldBe "A"
            fst.getAs[Int]("stars") shouldBe 3

          }
        }
      )
    }
  }

}
