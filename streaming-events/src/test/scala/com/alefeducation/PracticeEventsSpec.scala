package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers

class PracticeEventsSpec extends SparkSuite with Matchers {

  trait Setup {

    val session = spark

    import session.implicits._

    implicit val transformer = new PracticeEventsTransformer("practice-events", spark)

    def explodeArr(df: DataFrame, name: String): DataFrame = {
      df.select(explode(col(name))).select($"col.*")
    }

  }

  val expectedSkillsColumns: Set[String] = Set(
    "uuid",
    "code",
    "name"
  )

  test("handle PracticeCreatedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "learnerId",
        "subjectName",
        "schoolId",
        "loadtime",
        "items",
        "learningObjectiveTitle",
        "subjectId",
        "occurredOn",
        "id",
        "grade",
        "saScore",
        "sectionId",
        "skills",
        "tenantId",
        "learningObjectiveCode",
        "gradeId",
        "learningObjectiveId",
        "created",
        "academicYearId",
        "instructionalPlanId",
        "learningPathId",
        "classId",
        "materialId",
        "materialType"
      )

      val expectedItemColumns: Set[String] = Set(
        "learningObjectiveId",
        "learningObjectiveCode",
        "learningObjectiveTitle",
        "skills",
        "contents"
      )

      val expectedContentColumns: Set[String] = Set(
        "id",
        "title",
        "lessonType",
        "location"
      )
      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "PracticeCreatedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |    "occurredOn": "2019-07-09T12:36:05.765",
              |    "created": "2019-07-09T12:36:05.763",
              |    "id": "5080b3b1-7a4f-4ca4-b4a3-347fb9f1030c",
              |    "learnerId": "cec2121b-1f75-46eb-a09e-d45e866cc078",
              |    "subjectId": "1755bdc6-e660-4f80-8e3c-d4580a7da612",
              |    "subjectName": "Math",
              |    "schoolId": "fc604213-e624-4502-a48f-db3f1f1d7667",
              |    "gradeId": "6e70b79d-51f0-4c74-82cb-9ee9d01ab728",
              |    "grade": 6,
              |    "sectionId": "96fd3ccb-54e4-4d07-a609-48ad51b8a442",
              |    "learningObjectiveId": "02175a5e-6be7-4b00-9450-bb806d7eda38",
              |    "learningObjectiveCode": "MA6_MLO_154",
              |    "learningObjectiveTitle": "Polygons on The Coordinate Plane",
              |    "skills": [
              |      {
              |        "uuid": "924095a3-a0ce-4cf1-8e7e-604dbd901fb2",
              |        "code": "sk.m151",
              |        "name": "Find the area and perimeter of polygons on a coordinate plane"
              |      }
              |    ],
              |    "saScore": 20.0,
              |    "items": [
              |      {
              |        "learningObjectiveId": "d1b637b7-c285-43be-9065-c9b0546041d1",
              |        "learningObjectiveCode": "MA6_MLO_126",
              |        "learningObjectiveTitle": "Finding the Distance on a Coordinate Plane ",
              |        "skills": [
              |          {
              |            "uuid": "3086cf62-0f4e-4539-afa1-c01fc5eefce5",
              |            "code": "sk.m189",
              |            "name": "Find the distance on a coordinate plane"
              |          }
              |        ],
              |        "contents": [
              |          {
              |            "id": "ae8c12a6-9fa1-475e-8827-98e968e61dfe",
              |            "title": "Depth of Knowledge 1",
              |            "lessonType": "LESSON_1",
              |            "location": "/content/data/ccl/content/b9/1f/4f/4d/5227/index.html?digest=806508297d729b86c3e6619f5b9a589b6b1ba547"
              |          },
              |          {
              |            "id": "49b76fff-20f3-4805-a761-d57ce085b554",
              |            "title": "Second Look",
              |            "lessonType": "R",
              |            "location": "/content/data/ccl/content/b4/f1/ec/9f/5652/MA6_MLO_126_VD_002.mp4?digest=5fd0ebb0fa6a8c8bd69fb7fd8bb4d1a3994a3401"
              |          },
              |          {
              |            "id": "4f4b7a00-a08f-455a-a93d-b7ebca6e4165",
              |            "title": "Check My Understanding 1",
              |            "lessonType": "TEQ_1",
              |            "location": "/content/interactive-content?rules=5c346e0b9df15d00019ebad3;TEQ1;ANY;2"
              |          },
              |          {
              |            "id": "062f9abd-87ec-470c-98b6-b9915a4026d1",
              |            "title": "Check My Understanding 2",
              |            "lessonType": "TEQ_2",
              |            "location": "/content/interactive-content?rules=5c346e0b9df15d00019ebad3;TEQ2;ANY;2"
              |          },
              |          {
              |            "id": "f6c9f8c4-0ea5-4c57-ba88-c25012a64c53",
              |            "title": "Check My Understanding 3",
              |            "lessonType": "TEQ_3",
              |            "location": "/content/interactive-content?rules=5c346e0b9df15d00019ebad3;TEQ3;ANY;1"
              |          }
              |        ]
              |      }
              |    ],
              |    "academicYearId": "6a87dedd-d7f2-4a9a-b4a2-870ad56f013f",
              |    "classId": "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17",
              |    "materialId":"materialId",
              |    "materialType":"materialType"
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
          val df = sinks
            .find(_.name == "practice-created-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("practice-created-sink is not found"))

          val skillsDf = explodeArr(df, "skills")

          val itemsDf = explodeArr(df, "items")
          val itemsSkillsDf = explodeArr(itemsDf, "skills")
          val itemsContentsDf = explodeArr(itemsDf, "contents")

          df.columns.toSet shouldBe expectedColumns
          skillsDf.columns.toSet shouldBe expectedSkillsColumns
          itemsSkillsDf.columns.toSet shouldBe expectedSkillsColumns
          itemsDf.columns.toSet shouldBe expectedItemColumns
          itemsContentsDf.columns.toSet shouldBe expectedContentColumns

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "PracticeCreatedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190709"
          fst.getAs[String]("learnerId") shouldBe "cec2121b-1f75-46eb-a09e-d45e866cc078"
          fst.getAs[String]("subjectName") shouldBe "Math"
          fst.getAs[String]("schoolId") shouldBe "fc604213-e624-4502-a48f-db3f1f1d7667"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("learningObjectiveTitle") shouldBe "Polygons on The Coordinate Plane"
          fst.getAs[String]("subjectId") shouldBe "1755bdc6-e660-4f80-8e3c-d4580a7da612"
          fst.getAs[String]("occurredOn") shouldBe "2019-07-09 12:36:05.765"
          fst.getAs[String]("id") shouldBe "5080b3b1-7a4f-4ca4-b4a3-347fb9f1030c"
          fst.getAs[Int]("grade") shouldBe 6
          fst.getAs[Double]("saScore") shouldBe 20.0
          fst.getAs[String]("sectionId") shouldBe "96fd3ccb-54e4-4d07-a609-48ad51b8a442"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("learningObjectiveCode") shouldBe "MA6_MLO_154"
          fst.getAs[String]("gradeId") shouldBe "6e70b79d-51f0-4c74-82cb-9ee9d01ab728"
          fst.getAs[String]("learningObjectiveId") shouldBe "02175a5e-6be7-4b00-9450-bb806d7eda38"
          fst.getAs[String]("created") shouldBe "2019-07-09T12:36:05.763"
          fst.getAs[String]("academicYearId") shouldBe "6a87dedd-d7f2-4a9a-b4a2-870ad56f013f"
          fst.getAs[String]("classId") shouldBe "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17"
          fst.getAs[String]("materialId") shouldBe "materialId"
          fst.getAs[String]("materialType") shouldBe "materialType"

          val fstSkills = skillsDf.first()
          fstSkills.getAs[String]("uuid") shouldBe "924095a3-a0ce-4cf1-8e7e-604dbd901fb2"
          fstSkills.getAs[String]("code") shouldBe "sk.m151"
          fstSkills.getAs[String]("name") shouldBe "Find the area and perimeter of polygons on a coordinate plane"

          val fstItemsSkills = itemsSkillsDf.first()
          fstItemsSkills.getAs[String]("uuid") shouldBe "3086cf62-0f4e-4539-afa1-c01fc5eefce5"
          fstItemsSkills.getAs[String]("code") shouldBe "sk.m189"
          fstItemsSkills.getAs[String]("name") shouldBe "Find the distance on a coordinate plane"

          val fstItems = itemsDf.first()
          fstItems.getAs[String]("learningObjectiveId") shouldBe "d1b637b7-c285-43be-9065-c9b0546041d1"
          fstItems.getAs[String]("learningObjectiveCode") shouldBe "MA6_MLO_126"
          fstItems.getAs[String]("learningObjectiveTitle") shouldBe "Finding the Distance on a Coordinate Plane "

          val fstItemsContents = itemsContentsDf.first()
          fstItemsContents.getAs[String]("id") shouldBe "ae8c12a6-9fa1-475e-8827-98e968e61dfe"
          fstItemsContents.getAs[String]("title") shouldBe "Depth of Knowledge 1"
          fstItemsContents.getAs[String]("lessonType") shouldBe "LESSON_1"
          fstItemsContents
            .getAs[String]("location") shouldBe "/content/data/ccl/content/b9/1f/4f/4d/5227/index.html?digest=806508297d729b86c3e6619f5b9a589b6b1ba547"
        }
      )
    }
  }

  test("handle PracticeSessionStartedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "practiceId",
        "eventType",
        "eventDateDw",
        "learnerId",
        "subjectName",
        "schoolId",
        "loadtime",
        "learningObjectiveTitle",
        "subjectId",
        "occurredOn",
        "updated",
        "outsideOfSchool",
        "grade",
        "status",
        "saScore",
        "sectionId",
        "skills",
        "tenantId",
        "learningObjectiveCode",
        "gradeId",
        "learningObjectiveId",
        "created",
        "academicYearId",
        "instructionalPlanId",
        "learningPathId",
        "classId",
        "materialId",
        "materialType"
      )
      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |  "headers": {
              |    "eventType": "PracticeSessionStartedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |    "occurredOn": "2019-07-09T12:36:30.365",
              |    "practiceId": "67e828d4-116e-4aed-854c-a4d22d2a4718",
              |    "learnerId": "cec2121b-1f75-46eb-a09e-d45e866cc078",
              |    "subjectId": "1755bdc6-e660-4f80-8e3c-d4580a7da612",
              |    "subjectName": "Math",
              |    "schoolId": "fc604213-e624-4502-a48f-db3f1f1d7667",
              |    "gradeId": "6e70b79d-51f0-4c74-82cb-9ee9d01ab728",
              |    "grade": 6,
              |    "sectionId": "96fd3ccb-54e4-4d07-a609-48ad51b8a442",
              |    "learningObjectiveId": "a9e4d46a-7842-4de2-9452-98b89fed0438",
              |    "learningObjectiveCode": "MA6_MLO_153",
              |    "learningObjectiveTitle": "Changes in Dimensions",
              |    "skills": [
              |      {
              |        "uuid": "a1035a35-e266-4993-aba5-e6db75e51b97",
              |        "code": "sk.m152",
              |        "name": "Compare areas and perimeters"
              |      }
              |    ],
              |    "saScore": 20.0,
              |    "created": "2019-07-09T10:31:44.515",
              |    "updated": "2019-07-09T12:36:30.363",
              |    "status": "IN_PROGRESS",
              |    "outsideOfSchool": true,
              |    "academicYearId": "98cc9843-d9e9-48ab-91c9-574ff6995487",
              |    "classId": "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17",
              |    "materialId":"materialId",
              |    "materialType":"materialType"
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
          val df = sinks
            .find(_.name == "practice-session-started-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("practice-session-started-sink is not found"))

          val skillsDf = explodeArr(df, "skills")

          skillsDf.columns.toSet shouldBe expectedSkillsColumns
          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("practiceId") shouldBe "67e828d4-116e-4aed-854c-a4d22d2a4718"
          fst.getAs[String]("eventType") shouldBe "PracticeSessionStartedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190709"
          fst.getAs[String]("learnerId") shouldBe "cec2121b-1f75-46eb-a09e-d45e866cc078"
          fst.getAs[String]("subjectName") shouldBe "Math"
          fst.getAs[String]("schoolId") shouldBe "fc604213-e624-4502-a48f-db3f1f1d7667"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("status") shouldBe "IN_PROGRESS"
          fst.getAs[String]("learningObjectiveTitle") shouldBe "Changes in Dimensions"
          fst.getAs[String]("subjectId") shouldBe "1755bdc6-e660-4f80-8e3c-d4580a7da612"
          fst.getAs[String]("occurredOn") shouldBe "2019-07-09 12:36:30.365"
          fst.getAs[String]("updated") shouldBe "2019-07-09T12:36:30.363"
          fst.getAs[Boolean]("outsideOfSchool") shouldBe true
          fst.getAs[Int]("grade") shouldBe 6
          fst.getAs[String]("status") shouldBe "IN_PROGRESS"
          fst.getAs[Double]("saScore") shouldBe 20.0
          fst.getAs[String]("sectionId") shouldBe "96fd3ccb-54e4-4d07-a609-48ad51b8a442"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("learningObjectiveCode") shouldBe "MA6_MLO_153"
          fst.getAs[String]("gradeId") shouldBe "6e70b79d-51f0-4c74-82cb-9ee9d01ab728"
          fst.getAs[String]("learningObjectiveId") shouldBe "a9e4d46a-7842-4de2-9452-98b89fed0438"
          fst.getAs[String]("created") shouldBe "2019-07-09T10:31:44.515"
          fst.getAs[String]("academicYearId") shouldBe "98cc9843-d9e9-48ab-91c9-574ff6995487"
          fst.getAs[String]("classId") shouldBe "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17"
          fst.getAs[String]("materialId") shouldBe "materialId"
          fst.getAs[String]("materialType") shouldBe "materialType"

          val fstSkills = skillsDf.first()
          fstSkills.getAs[String]("uuid") shouldBe "a1035a35-e266-4993-aba5-e6db75e51b97"
          fstSkills.getAs[String]("code") shouldBe "sk.m152"
          fstSkills.getAs[String]("name") shouldBe "Compare areas and perimeters"
        }
      )
    }
  }

  test("handle PracticeItemSessionStartedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "practiceId",
        "practiceLearningObjectiveTitle",
        "eventType",
        "eventDateDw",
        "practiceSectionId",
        "loadtime",
        "practiceLearnerId",
        "practiceSkills",
        "learningObjectiveTitle",
        "practiceLearningObjectiveId",
        "practiceLearningObjectiveCode",
        "practiceSchoolId",
        "occurredOn",
        "outsideOfSchool",
        "practiceSubjectName",
        "practiceSubjectId",
        "status",
        "practiceGradeId",
        "skills",
        "practiceGrade",
        "tenantId",
        "learningObjectiveCode",
        "learningObjectiveId",
        "practiceSaScore",
        "practiceAcademicYearId",
        "instructionalPlanId",
        "learningPathId",
        "practiceClassId",
        "materialId",
        "materialType"
      )
      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |  "headers": {
              |    "eventType": "PracticeItemSessionStartedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |    "occurredOn": "2019-07-09T12:36:30.366",
              |    "practiceId": "67e828d4-116e-4aed-854c-a4d22d2a4718",
              |    "learningObjectiveId": "44a9760d-e4da-40b0-8059-fe0c8c65a832",
              |    "learningObjectiveCode": "MA6_MLO_150",
              |    "learningObjectiveTitle": "Area of Triangles",
              |    "skills": [
              |      {
              |        "uuid": "8d5da36c-1bd9-4ef4-8fa2-8c794383949d",
              |        "code": "sk.m141",
              |        "name": "Find the area of triangle"
              |      }
              |    ],
              |    "status": "IN_PROGRESS",
              |    "practiceLearnerId": "cec2121b-1f75-46eb-a09e-d45e866cc078",
              |    "practiceSubjectId": "1755bdc6-e660-4f80-8e3c-d4580a7da612",
              |    "practiceSubjectName": "Math",
              |    "practiceSchoolId": "fc604213-e624-4502-a48f-db3f1f1d7667",
              |    "practiceGradeId": "6e70b79d-51f0-4c74-82cb-9ee9d01ab728",
              |    "practiceGrade": 6,
              |    "practiceSectionId": "96fd3ccb-54e4-4d07-a609-48ad51b8a442",
              |    "practiceLearningObjectiveId": "a9e4d46a-7842-4de2-9452-98b89fed0438",
              |    "practiceLearningObjectiveCode": "MA6_MLO_153",
              |    "practiceLearningObjectiveTitle": "Changes in Dimensions",
              |    "practiceSkills": [
              |      {
              |        "uuid": "a1035a35-e266-4993-aba5-e6db75e51b97",
              |        "code": "sk.m152",
              |        "name": "Compare areas and perimeters"
              |      }
              |    ],
              |    "practiceSaScore": 20.0,
              |    "outsideOfSchool": true,
              |    "practiceAcademicYearId": "fadc2a74-e825-489b-bb75-ada674aa2f82",
              |    "practiceClassId": "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17",
              |    "materialId":"materialId",
              |    "materialType":"materialType"
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
          val df = sinks
            .find(_.name == "practice-item-session-started-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("practice-item-session-started-sink is not found"))

          val skillsDf = explodeArr(df, "skills")

          val practiceSkillsDf = explodeArr(df, "practiceSkills")

          df.columns.toSet shouldBe expectedColumns
          skillsDf.columns.toSet shouldBe expectedSkillsColumns
          practiceSkillsDf.columns.toSet shouldBe expectedSkillsColumns

          val fst = df.first()
          fst.getAs[String]("practiceId") shouldBe "67e828d4-116e-4aed-854c-a4d22d2a4718"
          fst.getAs[String]("practiceLearningObjectiveTitle") shouldBe "Changes in Dimensions"
          fst.getAs[String]("eventType") shouldBe "PracticeItemSessionStartedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190709"
          fst.getAs[String]("practiceSectionId") shouldBe "96fd3ccb-54e4-4d07-a609-48ad51b8a442"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("practiceLearnerId") shouldBe "cec2121b-1f75-46eb-a09e-d45e866cc078"
          fst.getAs[String]("learningObjectiveTitle") shouldBe "Area of Triangles"
          fst.getAs[String]("practiceLearningObjectiveId") shouldBe "a9e4d46a-7842-4de2-9452-98b89fed0438"
          fst.getAs[String]("practiceLearningObjectiveCode") shouldBe "MA6_MLO_153"
          fst.getAs[String]("practiceSchoolId") shouldBe "fc604213-e624-4502-a48f-db3f1f1d7667"
          fst.getAs[String]("occurredOn") shouldBe "2019-07-09 12:36:30.366"
          fst.getAs[Boolean]("outsideOfSchool") shouldBe true
          fst.getAs[String]("practiceSubjectName") shouldBe "Math"
          fst.getAs[String]("practiceSubjectId") shouldBe "1755bdc6-e660-4f80-8e3c-d4580a7da612"
          fst.getAs[String]("status") shouldBe "IN_PROGRESS"
          fst.getAs[String]("practiceGradeId") shouldBe "6e70b79d-51f0-4c74-82cb-9ee9d01ab728"
          fst.getAs[Int]("practiceGrade") shouldBe 6
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("learningObjectiveCode") shouldBe "MA6_MLO_150"
          fst.getAs[String]("learningObjectiveId") shouldBe "44a9760d-e4da-40b0-8059-fe0c8c65a832"
          fst.getAs[Double]("practiceSaScore") shouldBe 20.0
          fst.getAs[String]("practiceAcademicYearId") shouldBe "fadc2a74-e825-489b-bb75-ada674aa2f82"
          fst.getAs[String]("practiceClassId") shouldBe "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17"
          fst.getAs[String]("materialId") shouldBe "materialId"
          fst.getAs[String]("materialType") shouldBe "materialType"
          val fstSkills = skillsDf.first()
          fstSkills.getAs[String]("uuid") shouldBe "8d5da36c-1bd9-4ef4-8fa2-8c794383949d"
          fstSkills.getAs[String]("code") shouldBe "sk.m141"
          fstSkills.getAs[String]("name") shouldBe "Find the area of triangle"

          val fstPracticeSkills = practiceSkillsDf.first()
          fstPracticeSkills.getAs[String]("uuid") shouldBe "a1035a35-e266-4993-aba5-e6db75e51b97"
          fstPracticeSkills.getAs[String]("code") shouldBe "sk.m152"
          fstPracticeSkills.getAs[String]("name") shouldBe "Compare areas and perimeters"
        }
      )
    }
  }

  test("handle PracticeItemContentSessionStartedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "practiceId",
        "practiceLearningObjectiveTitle",
        "eventType",
        "eventDateDw",
        "location",
        "practiceSectionId",
        "loadtime",
        "practiceItemLearningObjectiveCode",
        "practiceLearnerId",
        "practiceSkills",
        "practiceItemLearningObjectiveTitle",
        "practiceLearningObjectiveId",
        "practiceLearningObjectiveCode",
        "practiceSchoolId",
        "lessonType",
        "occurredOn",
        "id",
        "outsideOfSchool",
        "practiceItemLearningObjectiveId",
        "practiceSubjectName",
        "practiceItemSkills",
        "practiceSubjectId",
        "status",
        "practiceGradeId",
        "practiceGrade",
        "title",
        "tenantId",
        "practiceSaScore",
        "practiceAcademicYearId",
        "instructionalPlanId",
        "learningPathId",
        "practiceClassId",
        "materialId",
        "materialType"
      )
      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |  "headers": {
              |    "eventType": "PracticeItemContentSessionStartedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |    "occurredOn": "2019-07-09T12:36:30.366",
              |    "practiceId": "67e828d4-116e-4aed-854c-a4d22d2a4718",
              |    "id": "a62f0aa7-d45d-43f1-b107-f2a9037664e0",
              |    "title": "Check My Understanding 1",
              |    "lessonType": "TEQ_1",
              |    "location": "/content/alef-assessment?rules=5c88f6fcc242820001f90be8;TEQ1;ANY;1",
              |    "status": "IN_PROGRESS",
              |    "practiceLearnerId": "cec2121b-1f75-46eb-a09e-d45e866cc078",
              |    "practiceSubjectId": "1755bdc6-e660-4f80-8e3c-d4580a7da612",
              |    "practiceSubjectName": "Math",
              |    "practiceSchoolId": "fc604213-e624-4502-a48f-db3f1f1d7667",
              |    "practiceGradeId": "6e70b79d-51f0-4c74-82cb-9ee9d01ab728",
              |    "practiceGrade": 6,
              |    "practiceSectionId": "96fd3ccb-54e4-4d07-a609-48ad51b8a442",
              |    "practiceLearningObjectiveId": "a9e4d46a-7842-4de2-9452-98b89fed0438",
              |    "practiceLearningObjectiveCode": "MA6_MLO_153",
              |    "practiceLearningObjectiveTitle": "Changes in Dimensions",
              |    "practiceSkills": [
              |      {
              |        "uuid": "a1035a35-e266-4993-aba5-e6db75e51b97",
              |        "code": "sk.m152",
              |        "name": "Compare areas and perimeters"
              |      }
              |    ],
              |    "practiceItemLearningObjectiveId": "44a9760d-e4da-40b0-8059-fe0c8c65a832",
              |    "practiceItemLearningObjectiveCode": "MA6_MLO_150",
              |    "practiceItemLearningObjectiveTitle": "Area of Triangles",
              |    "practiceItemSkills": [
              |      {
              |        "uuid": "8d5da36c-1bd9-4ef4-8fa2-8c794383949d",
              |        "code": "sk.m141",
              |        "name": "Find the area of triangle"
              |      }
              |    ],
              |    "practiceSaScore": 20.0,
              |    "outsideOfSchool": true,
              |    "practiceAcademicYearId" : "fef234fe-06dd-4884-9f4b-4a2b8cab3a28",
              |    "practiceClassId": "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17",
              |    "materialId":"materialId",
              |    "materialType":"materialType"
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
          val df = sinks
            .find(_.name == "practice-item-content-session-started-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("practice-item-content-session-started-sink is not found"))

          val practiceSkillsDf = explodeArr(df, "practiceSkills")

          val practiceItemSkillsDf = explodeArr(df, "practiceItemSkills")

          df.columns.toSet shouldBe expectedColumns
          practiceSkillsDf.columns.toSet shouldBe expectedSkillsColumns
          practiceItemSkillsDf.columns.toSet shouldBe expectedSkillsColumns

          val fst = df.first()
          fst.getAs[String]("practiceId") shouldBe "67e828d4-116e-4aed-854c-a4d22d2a4718"
          fst.getAs[String]("practiceLearningObjectiveTitle") shouldBe "Changes in Dimensions"
          fst.getAs[String]("eventType") shouldBe "PracticeItemContentSessionStartedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190709"
          fst.getAs[String]("practiceSectionId") shouldBe "96fd3ccb-54e4-4d07-a609-48ad51b8a442"
          fst.getAs[String]("location") shouldBe "/content/alef-assessment?rules=5c88f6fcc242820001f90be8;TEQ1;ANY;1"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("practiceItemLearningObjectiveCode") shouldBe "MA6_MLO_150"
          fst.getAs[String]("practiceLearnerId") shouldBe "cec2121b-1f75-46eb-a09e-d45e866cc078"
          fst.getAs[String]("practiceItemLearningObjectiveTitle") shouldBe "Area of Triangles"
          fst.getAs[String]("practiceLearningObjectiveId") shouldBe "a9e4d46a-7842-4de2-9452-98b89fed0438"
          fst.getAs[String]("practiceLearningObjectiveCode") shouldBe "MA6_MLO_153"
          fst.getAs[String]("practiceSchoolId") shouldBe "fc604213-e624-4502-a48f-db3f1f1d7667"
          fst.getAs[String]("lessonType") shouldBe "TEQ_1"
          fst.getAs[String]("occurredOn") shouldBe "2019-07-09 12:36:30.366"
          fst.getAs[String]("id") shouldBe "a62f0aa7-d45d-43f1-b107-f2a9037664e0"
          fst.getAs[Boolean]("outsideOfSchool") shouldBe true
          fst.getAs[String]("practiceItemLearningObjectiveId") shouldBe "44a9760d-e4da-40b0-8059-fe0c8c65a832"
          fst.getAs[String]("practiceSubjectName") shouldBe "Math"
          fst.getAs[String]("practiceSubjectId") shouldBe "1755bdc6-e660-4f80-8e3c-d4580a7da612"
          fst.getAs[String]("status") shouldBe "IN_PROGRESS"
          fst.getAs[String]("practiceGradeId") shouldBe "6e70b79d-51f0-4c74-82cb-9ee9d01ab728"
          fst.getAs[Int]("practiceGrade") shouldBe 6
          fst.getAs[String]("title") shouldBe "Check My Understanding 1"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[Double]("practiceSaScore") shouldBe 20.0
          fst.getAs[String]("practiceAcademicYearId") shouldBe "fef234fe-06dd-4884-9f4b-4a2b8cab3a28"
          fst.getAs[String]("practiceClassId") shouldBe "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17"
          fst.getAs[String]("materialId") shouldBe "materialId"
          fst.getAs[String]("materialType") shouldBe "materialType"
          val fstPriceSkills = practiceSkillsDf.first()
          fstPriceSkills.getAs[String]("uuid") shouldBe "a1035a35-e266-4993-aba5-e6db75e51b97"
          fstPriceSkills.getAs[String]("code") shouldBe "sk.m152"
          fstPriceSkills.getAs[String]("name") shouldBe "Compare areas and perimeters"

          val fstPracticeItemSkills = practiceItemSkillsDf.first()
          fstPracticeItemSkills.getAs[String]("uuid") shouldBe "8d5da36c-1bd9-4ef4-8fa2-8c794383949d"
          fstPracticeItemSkills.getAs[String]("code") shouldBe "sk.m141"
          fstPracticeItemSkills.getAs[String]("name") shouldBe "Find the area of triangle"
        }
      )
    }
  }

  test("handle PracticeItemContentSessionFinishedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "practiceId",
        "practiceLearningObjectiveTitle",
        "eventType",
        "eventDateDw",
        "location",
        "practiceSectionId",
        "loadtime",
        "practiceItemLearningObjectiveCode",
        "score",
        "practiceLearnerId",
        "practiceSkills",
        "practiceItemLearningObjectiveTitle",
        "scoreBreakdown",
        "practiceLearningObjectiveId",
        "practiceLearningObjectiveCode",
        "practiceSchoolId",
        "lessonType",
        "occurredOn",
        "id",
        "outsideOfSchool",
        "practiceItemLearningObjectiveId",
        "practiceSubjectName",
        "practiceItemSkills",
        "practiceSubjectId",
        "status",
        "practiceGradeId",
        "practiceGrade",
        "title",
        "tenantId",
        "practiceSaScore",
        "practiceAcademicYearId",
        "instructionalPlanId",
        "learningPathId",
        "practiceClassId",
        "materialId",
        "materialType"
      )
      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |  "headers": {
              |    "eventType": "PracticeItemContentSessionFinishedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |    "occurredOn": "2019-07-09T12:36:43.052",
              |    "practiceId": "67e828d4-116e-4aed-854c-a4d22d2a4718",
              |    "id": "a62f0aa7-d45d-43f1-b107-f2a9037664e0",
              |    "title": "Check My Understanding 1",
              |    "lessonType": "TEQ_1",
              |    "location": "/content/alef-assessment?rules=5c88f6fcc242820001f90be8;TEQ1;ANY;1",
              |    "score": 0.0,
              |    "scoreBreakDown": [],
              |    "status": "COMPLETED",
              |    "practiceLearnerId": "cec2121b-1f75-46eb-a09e-d45e866cc078",
              |    "practiceSubjectId": "1755bdc6-e660-4f80-8e3c-d4580a7da612",
              |    "practiceSubjectName": "Math",
              |    "practiceSchoolId": "fc604213-e624-4502-a48f-db3f1f1d7667",
              |    "practiceGradeId": "6e70b79d-51f0-4c74-82cb-9ee9d01ab728",
              |    "practiceGrade": 6,
              |    "practiceSectionId": "96fd3ccb-54e4-4d07-a609-48ad51b8a442",
              |    "practiceLearningObjectiveId": "a9e4d46a-7842-4de2-9452-98b89fed0438",
              |    "practiceLearningObjectiveCode": "MA6_MLO_153",
              |    "practiceLearningObjectiveTitle": "Changes in Dimensions",
              |    "practiceSkills": [
              |      {
              |        "uuid": "a1035a35-e266-4993-aba5-e6db75e51b97",
              |        "code": "sk.m152",
              |        "name": "Compare areas and perimeters"
              |      }
              |    ],
              |    "practiceSaScore": 20.0,
              |    "practiceItemLearningObjectiveId": "44a9760d-e4da-40b0-8059-fe0c8c65a832",
              |    "practiceItemLearningObjectiveCode": "MA6_MLO_150",
              |    "practiceItemLearningObjectiveTitle": "Area of Triangles",
              |    "practiceItemSkills": [
              |      {
              |        "uuid": "8d5da36c-1bd9-4ef4-8fa2-8c794383949d",
              |        "code": "sk.m141",
              |        "name": "Find the area of triangle"
              |      }
              |    ],
              |    "outsideOfSchool": true,
              |    "practiceAcademicYearId" : "f239fb58-056a-4e2c-809d-545fd62fb93d",
              |    "practiceClassId": "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17",
              |    "materialId":"materialId",
              |    "materialType":"materialType"
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
          val df = sinks
            .find(_.name == "practice-item-content-session-finished-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("practice-item-content-session-finished-sink is not found"))

          val practiceSkillsDf = explodeArr(df, "practiceSkills")

          val practiceItemSkillsDf = explodeArr(df, "practiceItemSkills")

          df.columns.toSet shouldBe expectedColumns
          practiceSkillsDf.columns.toSet shouldBe expectedSkillsColumns
          practiceItemSkillsDf.columns.toSet shouldBe expectedSkillsColumns

          val fst = df.first()
          fst.getAs[String]("practiceId") shouldBe "67e828d4-116e-4aed-854c-a4d22d2a4718"
          fst.getAs[String]("practiceLearningObjectiveTitle") shouldBe "Changes in Dimensions"
          fst.getAs[String]("eventType") shouldBe "PracticeItemContentSessionFinishedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190709"
          fst.getAs[String]("practiceSectionId") shouldBe "96fd3ccb-54e4-4d07-a609-48ad51b8a442"
          fst.getAs[String]("location") shouldBe "/content/alef-assessment?rules=5c88f6fcc242820001f90be8;TEQ1;ANY;1"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[String]("practiceItemLearningObjectiveCode") shouldBe "MA6_MLO_150"
          fst.getAs[Int]("score") shouldBe 0
          fst.getAs[String]("practiceLearnerId") shouldBe "cec2121b-1f75-46eb-a09e-d45e866cc078"
          fst.getAs[String]("practiceItemLearningObjectiveTitle") shouldBe "Area of Triangles"
          fst.getAs[List[String]]("scoreBreakdown") shouldBe null
          fst.getAs[String]("practiceLearningObjectiveId") shouldBe "a9e4d46a-7842-4de2-9452-98b89fed0438"
          fst.getAs[String]("practiceLearningObjectiveCode") shouldBe "MA6_MLO_153"
          fst.getAs[String]("practiceSchoolId") shouldBe "fc604213-e624-4502-a48f-db3f1f1d7667"
          fst.getAs[String]("lessonType") shouldBe "TEQ_1"
          fst.getAs[String]("occurredOn") shouldBe "2019-07-09 12:36:43.052"
          fst.getAs[String]("id") shouldBe "a62f0aa7-d45d-43f1-b107-f2a9037664e0"
          fst.getAs[Boolean]("outsideOfSchool") shouldBe true
          fst.getAs[String]("practiceItemLearningObjectiveId") shouldBe "44a9760d-e4da-40b0-8059-fe0c8c65a832"
          fst.getAs[String]("practiceSubjectName") shouldBe "Math"
          fst.getAs[String]("practiceSubjectId") shouldBe "1755bdc6-e660-4f80-8e3c-d4580a7da612"
          fst.getAs[String]("status") shouldBe "COMPLETED"
          fst.getAs[String]("practiceGradeId") shouldBe "6e70b79d-51f0-4c74-82cb-9ee9d01ab728"
          fst.getAs[Int]("practiceGrade") shouldBe 6
          fst.getAs[String]("title") shouldBe "Check My Understanding 1"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[Double]("practiceSaScore") shouldBe 20.0
          fst.getAs[String]("practiceAcademicYearId") shouldBe "f239fb58-056a-4e2c-809d-545fd62fb93d"
          fst.getAs[String]("practiceClassId") shouldBe "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17"
          fst.getAs[String]("materialId") shouldBe "materialId"
          fst.getAs[String]("materialType") shouldBe "materialType"
          val fstPriceSkills = practiceSkillsDf.first()
          fstPriceSkills.getAs[String]("uuid") shouldBe "a1035a35-e266-4993-aba5-e6db75e51b97"
          fstPriceSkills.getAs[String]("code") shouldBe "sk.m152"
          fstPriceSkills.getAs[String]("name") shouldBe "Compare areas and perimeters"

          val fstPracticeItemSkills = practiceItemSkillsDf.first()
          fstPracticeItemSkills.getAs[String]("uuid") shouldBe "8d5da36c-1bd9-4ef4-8fa2-8c794383949d"
          fstPracticeItemSkills.getAs[String]("code") shouldBe "sk.m141"
          fstPracticeItemSkills.getAs[String]("name") shouldBe "Find the area of triangle"
        }
      )
    }
  }

  test("handle PracticeItemSessionFinishedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "practiceId",
        "practiceLearningObjectiveTitle",
        "eventType",
        "eventDateDw",
        "practiceSectionId",
        "loadtime",
        "score",
        "practiceLearnerId",
        "practiceSkills",
        "learningObjectiveTitle",
        "practiceLearningObjectiveId",
        "practiceLearningObjectiveCode",
        "practiceSchoolId",
        "occurredOn",
        "outsideOfSchool",
        "practiceSubjectName",
        "practiceSubjectId",
        "practiceGradeId",
        "skills",
        "practiceGrade",
        "tenantId",
        "learningObjectiveCode",
        "learningObjectiveId",
        "practiceSaScore",
        "practiceAcademicYearId",
        "instructionalPlanId",
        "learningPathId",
        "practiceClassId",
        "materialId",
        "materialType"
      )
      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |  "headers": {
              |    "eventType": "PracticeItemSessionFinishedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |    "occurredOn": "2019-07-09T12:38:02.988",
              |    "practiceId": "67e828d4-116e-4aed-854c-a4d22d2a4718",
              |    "learningObjectiveId": "31a45b2f-8dfa-4a96-88ce-98c80060b720",
              |    "learningObjectiveCode": "MA6_MLO_151",
              |    "learningObjectiveTitle": " Area of Parallelograms ",
              |    "skills": [
              |      {
              |        "uuid": "bd5711f5-9a58-45bf-9504-a6a8eb514d48",
              |        "code": "sk.m146",
              |        "name": "Find the area of parallelogram"
              |      }
              |    ],
              |    "status": "COMPLETED",
              |    "score": 67.0,
              |    "practiceLearnerId": "cec2121b-1f75-46eb-a09e-d45e866cc078",
              |    "practiceSubjectId": "1755bdc6-e660-4f80-8e3c-d4580a7da612",
              |    "practiceSubjectName": "Math",
              |    "practiceSchoolId": "fc604213-e624-4502-a48f-db3f1f1d7667",
              |    "practiceGradeId": "6e70b79d-51f0-4c74-82cb-9ee9d01ab728",
              |    "practiceGrade": 6,
              |    "practiceSectionId": "96fd3ccb-54e4-4d07-a609-48ad51b8a442",
              |    "practiceLearningObjectiveId": "a9e4d46a-7842-4de2-9452-98b89fed0438",
              |    "practiceLearningObjectiveCode": "MA6_MLO_153",
              |    "practiceLearningObjectiveTitle": "Changes in Dimensions",
              |    "practiceSkills": [
              |      {
              |        "uuid": "a1035a35-e266-4993-aba5-e6db75e51b97",
              |        "code": "sk.m152",
              |        "name": "Compare areas and perimeters"
              |      }
              |    ],
              |    "practiceSaScore": 20.0,
              |    "outsideOfSchool": true,
              |    "practiceAcademicYearId" : "69784e23-47a7-4896-8548-4b3c7b95e77c",
              |    "practiceClassId": "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17",
              |    "materialId":"materialId",
              |    "materialType":"materialType"
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
          val df = sinks
            .find(_.name == "practice-item-session-finished-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("practice-item-session-finished-sink is not found"))

          val skillsDf = explodeArr(df, "skills")

          val practiceSkillsDf = explodeArr(df, "practiceSkills")

          df.columns.toSet shouldBe expectedColumns
          skillsDf.columns.toSet shouldBe expectedSkillsColumns
          practiceSkillsDf.columns.toSet shouldBe expectedSkillsColumns

          val fst = df.first()
          fst.getAs[String]("practiceId") shouldBe "67e828d4-116e-4aed-854c-a4d22d2a4718"
          fst.getAs[String]("practiceLearningObjectiveTitle") shouldBe "Changes in Dimensions"
          fst.getAs[String]("eventType") shouldBe "PracticeItemSessionFinishedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190709"
          fst.getAs[String]("practiceSectionId") shouldBe "96fd3ccb-54e4-4d07-a609-48ad51b8a442"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[Double]("score") shouldBe 67.0
          fst.getAs[String]("practiceLearnerId") shouldBe "cec2121b-1f75-46eb-a09e-d45e866cc078"
          fst.getAs[String]("learningObjectiveTitle") shouldBe " Area of Parallelograms "
          fst.getAs[String]("practiceLearningObjectiveId") shouldBe "a9e4d46a-7842-4de2-9452-98b89fed0438"
          fst.getAs[String]("practiceLearningObjectiveCode") shouldBe "MA6_MLO_153"
          fst.getAs[String]("practiceSchoolId") shouldBe "fc604213-e624-4502-a48f-db3f1f1d7667"
          fst.getAs[String]("occurredOn") shouldBe "2019-07-09 12:38:02.988"
          fst.getAs[Boolean]("outsideOfSchool") shouldBe true
          fst.getAs[String]("practiceSubjectName") shouldBe "Math"
          fst.getAs[String]("practiceSubjectId") shouldBe "1755bdc6-e660-4f80-8e3c-d4580a7da612"
          fst.getAs[String]("practiceGradeId") shouldBe "6e70b79d-51f0-4c74-82cb-9ee9d01ab728"
          fst.getAs[Int]("practiceGrade") shouldBe 6
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("learningObjectiveCode") shouldBe "MA6_MLO_151"
          fst.getAs[String]("learningObjectiveId") shouldBe "31a45b2f-8dfa-4a96-88ce-98c80060b720"
          fst.getAs[Double]("practiceSaScore") shouldBe 20.0
          fst.getAs[String]("practiceAcademicYearId") shouldBe "69784e23-47a7-4896-8548-4b3c7b95e77c"
          fst.getAs[String]("practiceClassId") shouldBe "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17"
          fst.getAs[String]("materialId") shouldBe "materialId"
          fst.getAs[String]("materialType") shouldBe "materialType"
          val fstSkills = skillsDf.first()
          fstSkills.getAs[String]("uuid") shouldBe "bd5711f5-9a58-45bf-9504-a6a8eb514d48"
          fstSkills.getAs[String]("code") shouldBe "sk.m146"
          fstSkills.getAs[String]("name") shouldBe "Find the area of parallelogram"

          val fstPracticeSkills = practiceSkillsDf.first()
          fstPracticeSkills.getAs[String]("uuid") shouldBe "a1035a35-e266-4993-aba5-e6db75e51b97"
          fstPracticeSkills.getAs[String]("code") shouldBe "sk.m152"
          fstPracticeSkills.getAs[String]("name") shouldBe "Compare areas and perimeters"
        }
      )
    }
  }

  test("handle PracticeSessionFinishedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "practiceId",
        "eventType",
        "eventDateDw",
        "stars",
        "learnerId",
        "subjectName",
        "schoolId",
        "loadtime",
        "score",
        "learningObjectiveTitle",
        "subjectId",
        "occurredOn",
        "updated",
        "outsideOfSchool",
        "grade",
        "status",
        "saScore",
        "sectionId",
        "skills",
        "tenantId",
        "learningObjectiveCode",
        "gradeId",
        "learningObjectiveId",
        "created",
        "academicYearId",
        "instructionalPlanId",
        "learningPathId",
        "classId",
        "materialId",
        "materialType"
      )
      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value": {
              |  "headers": {
              |    "eventType": "PracticeSessionFinishedEvent",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |    "occurredOn": "2019-07-09T12:38:02.988",
              |    "practiceId": "67e828d4-116e-4aed-854c-a4d22d2a4718",
              |    "learnerId": "cec2121b-1f75-46eb-a09e-d45e866cc078",
              |    "subjectId": "1755bdc6-e660-4f80-8e3c-d4580a7da612",
              |    "subjectName": "Math",
              |    "schoolId": "fc604213-e624-4502-a48f-db3f1f1d7667",
              |    "gradeId": "6e70b79d-51f0-4c74-82cb-9ee9d01ab728",
              |    "grade": 6,
              |    "sectionId": "96fd3ccb-54e4-4d07-a609-48ad51b8a442",
              |    "learningObjectiveId": "a9e4d46a-7842-4de2-9452-98b89fed0438",
              |    "learningObjectiveCode": "MA6_MLO_153",
              |    "learningObjectiveTitle": "Changes in Dimensions",
              |    "skills": [
              |      {
              |        "uuid": "a1035a35-e266-4993-aba5-e6db75e51b97",
              |        "code": "sk.m152",
              |        "name": "Compare areas and perimeters"
              |      }
              |    ],
              |    "saScore": 20.0,
              |    "created": "2019-07-09T10:31:44.515",
              |    "updated": "2019-07-09T12:38:02.986",
              |    "status": "COMPLETED",
              |    "score": 22.0,
              |    "outsideOfSchool": true,
              |    "stars": 0,
              |    "academicYearId": "c9d21420-4e94-43b0-a4fc-bf4810575941",
              |    "classId": "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17",
              |    "materialId":"materialId",
              |    "materialType":"materialType"
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
          val df = sinks
            .find(_.name == "practice-session-finished-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("practice-session-finished-sink is not found"))

          val skillsDf = explodeArr(df, "skills")

          skillsDf.columns.toSet shouldBe expectedSkillsColumns
          df.columns.toSet shouldBe expectedColumns

          val fst = df.first()
          fst.getAs[String]("practiceId") shouldBe "67e828d4-116e-4aed-854c-a4d22d2a4718"
          fst.getAs[String]("eventType") shouldBe "PracticeSessionFinishedEvent"
          fst.getAs[String]("eventDateDw") shouldBe "20190709"
          fst.getAs[Int]("stars") shouldBe 0
          fst.getAs[String]("learnerId") shouldBe "cec2121b-1f75-46eb-a09e-d45e866cc078"
          fst.getAs[String]("subjectName") shouldBe "Math"
          fst.getAs[String]("schoolId") shouldBe "fc604213-e624-4502-a48f-db3f1f1d7667"
          fst.getAs[String]("loadtime") shouldBe "2019-04-20 16:24:37.501"
          fst.getAs[Double]("score") shouldBe 22.0
          fst.getAs[String]("learningObjectiveTitle") shouldBe "Changes in Dimensions"
          fst.getAs[String]("subjectId") shouldBe "1755bdc6-e660-4f80-8e3c-d4580a7da612"
          fst.getAs[String]("occurredOn") shouldBe "2019-07-09 12:38:02.988"
          fst.getAs[String]("updated") shouldBe "2019-07-09T12:38:02.986"
          fst.getAs[Boolean]("outsideOfSchool") shouldBe true
          fst.getAs[Int]("grade") shouldBe 6
          fst.getAs[String]("status") shouldBe "COMPLETED"
          fst.getAs[Double]("saScore") shouldBe 20.0
          fst.getAs[String]("sectionId") shouldBe "96fd3ccb-54e4-4d07-a609-48ad51b8a442"
          fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
          fst.getAs[String]("learningObjectiveCode") shouldBe "MA6_MLO_153"
          fst.getAs[String]("gradeId") shouldBe "6e70b79d-51f0-4c74-82cb-9ee9d01ab728"
          fst.getAs[String]("learningObjectiveId") shouldBe "a9e4d46a-7842-4de2-9452-98b89fed0438"
          fst.getAs[String]("created") shouldBe "2019-07-09T10:31:44.515"
          fst.getAs[String]("academicYearId") shouldBe "c9d21420-4e94-43b0-a4fc-bf4810575941"
          fst.getAs[String]("classId") shouldBe "a4f4b3b8-6c19-43bf-bd2a-bffbccae9b17"
          fst.getAs[String]("materialId") shouldBe "materialId"
          fst.getAs[String]("materialType") shouldBe "materialType"
          val fstSkills = skillsDf.first()
          fstSkills.getAs[String]("uuid") shouldBe "a1035a35-e266-4993-aba5-e6db75e51b97"
          fstSkills.getAs[String]("code") shouldBe "sk.m152"
          fstSkills.getAs[String]("name") shouldBe "Compare areas and perimeters"
        }
      )
    }
  }

  test("handle TeacherAnnouncementSentEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value =
            """
              |[
              |   {
              |      "key":"key1",
              |      "value":{
              |         "headers":{
              |            "eventType":"TeacherAnnouncementSentEvent",
              |            "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |         },
              |         "body":{
              |            "announcementId":"0ad38f1b-c259-421e-81a1-a0ea43f55fb0",
              |            "teacherId":"9574742b-4300-4340-ad12-27f946c13906",
              |            "classIds":[
              |               "ddab74e9-f739-41db-9c1c-d8fe7a761e9f",
              |               "8280884d-661e-47d0-91ae-591861525564",
              |               "5d7a67f3-bbf0-4022-a990-55e0c7400bf5",
              |               "f35b0603-a1f9-4f4f-ba2b-0471a633d555",
              |               "bbcb33e5-3afa-4334-adfa-a77ad73cc96a"
              |            ],
              |            "hasAttachment":false,
              |            "announcementType": "GUARDIANS",
              |            "occurredOn":1667825397901
              |         }
              |      },
              |      "timestamp":"2022-09-27 16:23:46.609"
              |   }
              |]
              |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "teacher-announcement-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("teacher-announcement-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "tenantId", "eventDateDw", "loadtime", "announcementId", "teacherId", "classIds", "studentIds",
            "hasAttachment", "announcementType", "occurredOn"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("handle PrincipalAnnouncementSentEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value =
            """
              |[
              |   {
              |      "key":"key1",
              |      "value":{
              |         "headers":{
              |            "eventType":"PrincipalAnnouncementSentEvent",
              |            "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |         },
              |         "body":{
              |            "announcementId":"bab95747-ecad-4a04-a3dd-fd5446c85a59",
              |            "principalId":"ca6781fa-cf59-4335-8fb4-69b5512be480",
              |            "schoolId":null,
              |            "gradeIds":[
              |               "7f8afa37-6049-448d-b5fa-ed2f5403aaa9"
              |            ],
              |            "classIds":[
              |               "ddab74e9-f739-41db-9c1c-d8fe7a761e9f",
              |               "8280884d-661e-47d0-91ae-591861525564",
              |               "5d7a67f3-bbf0-4022-a990-55e0c7400bf5",
              |               "f35b0603-a1f9-4f4f-ba2b-0471a633d555",
              |               "bbcb33e5-3afa-4334-adfa-a77ad73cc96a"
              |            ],
              |            "studentIds": null,
              |            "hasAttachment":false,
              |            "announcementType": "STUDENTS",
              |            "occurredOn":1668668898107
              |         }
              |      },
              |      "timestamp":"2022-09-27 16:23:46.609"
              |   }
              |]
              |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "principal-announcement-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("principal-announcement-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "tenantId", "eventDateDw", "loadtime", "announcementId", "principalId", "schoolId", "gradeIds", "classIds", "studentIds",
            "hasAttachment", "announcementType", "occurredOn"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("handle SuperintendentAnnouncementSentEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value =
            """
              |[
              |   {
              |      "key":"key1",
              |      "value":{
              |         "headers":{
              |            "eventType":"SuperintendentAnnouncementSentEvent",
              |            "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |         },
              |         "body":{
              |            "announcementId":"2c4bf2db-9d17-4abd-a695-2be853255d17",
              |            "superintendentId":"69cbf584-f1ff-4679-841e-cf491e273013",
              |            "schoolIds":[
              |               "f8d4bc9a-0c75-4345-9a59-7a63f775ba4c"
              |            ],
              |            "gradeIds":null,
              |            "hasAttachment":false,
              |            "announcementType": "GUARDIANS_AND_STUDENTS",
              |            "occurredOn":1667826843790
              |         }
              |      },
              |      "timestamp":"2022-09-27 16:23:46.609"
              |   }
              |]
              |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "superintendent-announcement-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("superintendent-announcement-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "tenantId", "eventDateDw", "loadtime", "announcementId", "superintendentId", "schoolIds",
            "gradeIds", "hasAttachment", "announcementType", "occurredOn"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("handle AnnouncementDeletedEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value =
            """
              |[
              |   {
              |      "key":"key1",
              |      "value":{
              |         "headers":{
              |            "eventType":"AnnouncementDeletedEvent",
              |            "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |         },
              |         "body":{
              |            "announcementId":"0ad38f1b-c259-421e-81a1-a0ea43f55fb0",
              |            "occurredOn":1667825451775
              |         }
              |      },
              |      "timestamp":"2022-09-27 16:23:46.609"
              |   }
              |]
              |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "announcement-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("announcement-deleted-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "tenantId", "eventDateDw", "loadtime", "announcementId", "occurredOn"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("handle GuardianJointActivityPendingEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "tenantId",
        "guardianIds",
        "pathwayId",
        "pathwayLevelId",
        "classId",
        "studentId",
        "schoolId",
        "grade",
      )

      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"GuardianJointActivityPendingEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |     "guardianIds": ["830a4d57-6e58-4554-ad71-f82e2cf9c6b3"],
                    |     "pathwayId": "546daba6-677a-4110-8843-54aaacb549b9",
                    |     "pathwayLevelId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
                    |     "classId": "b4c3b6a3-5d59-4dc7-8fef-78df1fc21f87",
                    |     "studentId": "a3bd5c12-98ea-4d00-80a1-5c3abbc4ad1f",
                    |     "schoolId": "b2a5dc84-88ac-409f-84cc-5c89eca5c66d",
                    |     "grade": 6,
                    |     "occurredOn": 100500092323
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
          val dfOpt = sinks.find(_.name == "guardian-joint-activity-pending-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "GuardianJointActivityPendingEvent"
            fst.getAs[List[String]]("guardianIds") shouldBe List("830a4d57-6e58-4554-ad71-f82e2cf9c6b3")
            fst.getAs[String]("pathwayId") shouldBe "546daba6-677a-4110-8843-54aaacb549b9"
            fst.getAs[String]("pathwayLevelId") shouldBe "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f"
            fst.getAs[String]("classId") shouldBe "b4c3b6a3-5d59-4dc7-8fef-78df1fc21f87"
            fst.getAs[String]("studentId") shouldBe "a3bd5c12-98ea-4d00-80a1-5c3abbc4ad1f"
            fst.getAs[String]("schoolId") shouldBe "b2a5dc84-88ac-409f-84cc-5c89eca5c66d"
            fst.getAs[Int]("grade") shouldBe 6
          }
        }
      )
    }
  }

  test("handle GuardianJointActivityStartedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "assignedOn",
        "tenantId",
        "pathwayId",
        "pathwayLevelId",
        "classId",
        "studentId",
        "startedByGuardianId",
        "schoolId",
        "grade",
        "attempt",
        "occurredOn"
      )
      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"GuardianJointActivityStartedEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |     "pathwayId": "546daba6-677a-4110-8843-54aaacb549b9",
                    |     "pathwayLevelId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
                    |     "classId": "b4c3b6a3-5d59-4dc7-8fef-78df1fc21f87",
                    |     "studentId": "a3bd5c12-98ea-4d00-80a1-5c3abbc4ad1f",
                    |     "schoolId": "b2a5dc84-88ac-409f-84cc-5c89eca5c66d",
                    |     "startedByGuardianId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
                    |     "grade": 6,
                    |     "attempt": 1,
                    |     "occurredOn": 100500092323
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
          val dfOpt = sinks.find(_.name == "guardian-joint-activity-started-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "GuardianJointActivityStartedEvent"
            fst.getAs[String]("pathwayId") shouldBe "546daba6-677a-4110-8843-54aaacb549b9"
            fst.getAs[String]("pathwayLevelId") shouldBe "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f"
            fst.getAs[String]("classId") shouldBe "b4c3b6a3-5d59-4dc7-8fef-78df1fc21f87"
            fst.getAs[String]("studentId") shouldBe "a3bd5c12-98ea-4d00-80a1-5c3abbc4ad1f"
            fst.getAs[String]("schoolId") shouldBe "b2a5dc84-88ac-409f-84cc-5c89eca5c66d"
            fst.getAs[String]("startedByGuardianId") shouldBe "830a4d57-6e58-4554-ad71-f82e2cf9c6b3"
            fst.getAs[Int]("grade") shouldBe 6
            fst.getAs[Int]("attempt") shouldBe 1
          }
        }
      )
    }
  }

  test("handle GuardianJointActivityCompletedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "tenantId",
        "pathwayId",
        "pathwayLevelId",
        "classId",
        "studentId",
        "schoolId",
        "grade",
        "completedByGuardianId",
        "attempt",
        "assignedOn",
        "startedOn"
      )
      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"GuardianJointActivityCompletedEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |     "pathwayId": "546daba6-677a-4110-8843-54aaacb549b9",
                    |     "pathwayLevelId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
                    |     "classId": "b4c3b6a3-5d59-4dc7-8fef-78df1fc21f87",
                    |     "studentId": "a3bd5c12-98ea-4d00-80a1-5c3abbc4ad1f",
                    |     "schoolId": "b2a5dc84-88ac-409f-84cc-5c89eca5c66d",
                    |     "completedByGuardianId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
                    |     "grade": 6,
                    |     "attempt": 1,
                    |     "assignedOn": 1624426404921,
                    |     "startedOn": 1624426404921,
                    |     "occurredOn": 100500092323
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
          val dfOpt = sinks.find(_.name == "guardian-joint-activity-completed-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "GuardianJointActivityCompletedEvent"
            fst.getAs[String]("pathwayId") shouldBe "546daba6-677a-4110-8843-54aaacb549b9"
            fst.getAs[String]("pathwayLevelId") shouldBe "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f"
            fst.getAs[String]("classId") shouldBe "b4c3b6a3-5d59-4dc7-8fef-78df1fc21f87"
            fst.getAs[String]("studentId") shouldBe "a3bd5c12-98ea-4d00-80a1-5c3abbc4ad1f"
            fst.getAs[String]("schoolId") shouldBe "b2a5dc84-88ac-409f-84cc-5c89eca5c66d"
            fst.getAs[String]("completedByGuardianId") shouldBe "830a4d57-6e58-4554-ad71-f82e2cf9c6b3"
            fst.getAs[Long]("assignedOn") shouldBe 1624426404921L
            fst.getAs[Long]("startedOn") shouldBe 1624426404921L
            fst.getAs[Int]("grade") shouldBe 6
            fst.getAs[Int]("attempt") shouldBe 1
          }
        }
      )
    }
  }

  test("handle GuardianJointActivityRatedEvent") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "tenantId",
        "pathwayId",
        "pathwayLevelId",
        "classId",
        "studentId",
        "guardianId",
        "schoolId",
        "grade",
        "attempt",
        "rating",
      )
      val fixtures = List(
        SparkFixture(
          key = "practice-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"GuardianJointActivityRatedEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |     "pathwayId": "546daba6-677a-4110-8843-54aaacb549b9",
                    |     "pathwayLevelId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
                    |     "classId": "b4c3b6a3-5d59-4dc7-8fef-78df1fc21f87",
                    |     "studentId": "a3bd5c12-98ea-4d00-80a1-5c3abbc4ad1f",
                    |     "schoolId": "b2a5dc84-88ac-409f-84cc-5c89eca5c66d",
                    |     "guardianId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
                    |     "grade": 6,
                    |     "attempt": 1,
                    |     "rating": 9,
                    |     "occurredOn": 100500092323
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
          val dfOpt = sinks.find(_.name == "guardian-joint-activity-rated-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "GuardianJointActivityRatedEvent"
            fst.getAs[String]("pathwayId") shouldBe "546daba6-677a-4110-8843-54aaacb549b9"
            fst.getAs[String]("pathwayLevelId") shouldBe "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f"
            fst.getAs[String]("classId") shouldBe "b4c3b6a3-5d59-4dc7-8fef-78df1fc21f87"
            fst.getAs[String]("studentId") shouldBe "a3bd5c12-98ea-4d00-80a1-5c3abbc4ad1f"
            fst.getAs[String]("schoolId") shouldBe "b2a5dc84-88ac-409f-84cc-5c89eca5c66d"
            fst.getAs[String]("guardianId") shouldBe "830a4d57-6e58-4554-ad71-f82e2cf9c6b3"
            fst.getAs[Int]("grade") shouldBe 6
            fst.getAs[Int]("attempt") shouldBe 1
            fst.getAs[Int]("rating") shouldBe 9
          }
        }
      )
    }
  }
}
