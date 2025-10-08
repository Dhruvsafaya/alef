package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.explode
import org.scalatest.matchers.should.Matchers

class LearningProgressRawEventsSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer = new LearningProgressRawEventsTransformer(LearningProgressRawEvents.name, spark)
  }

  val occurredOn = "2022-09-01 10:43:52.898"
  val tenantId = "2746328b-4109-4434-8517-940d636ffa09"
  val loadtime = "2022-09-01 16:24:37.501"
  val eventDateDw = "20220901"
  val learningSessionId = "32b1de3a-9dce-4e19-ac06-33002856d49d"
  val learningObjectiveId = "f9f7dbcd-2e20-41d6-8adc-b29b6e16d641"
  val classId = "a98408e7-74bd-42b6-be5d-ab0d75399621"
  val studentId = "763e55ab-2ae0-4aea-a3e4-887237bc026f"

  val expCommonMapCols = Map(
    "tenantId" -> tenantId,
    "occurredOn" -> occurredOn,
    "loadtime" -> loadtime,
    "eventDateDw" -> eventDateDw,
    "learningSessionId" -> learningSessionId,
    "learningObjectiveId" -> learningObjectiveId,
    "classId" -> classId,
    "studentId" -> studentId
  )

  val expCommonCols: Set[String] = Set(
    "eventType",
  ) ++ expCommonMapCols.keys.toSet

  val expColsForLearningSessionStartEvents: Set[String] = expCommonCols ++ Set(
    "learningObjectiveType",
    "curriculumGradeId",
    "curriculumSubjectName",
    "studentGradeId",
    "subjectName",
    "learningPathId",
    "schoolId",
    "subjectCode",
    "redo",
    "studentGrade",
    "academicYearId",
    "learningObjectiveTitle",
    "studentSection",
    "subjectId",
    "instructionalPlanId",
    "trimesterId",
    "contentAcademicYear",
    "curriculumName",
    "trimesterOrder",
    "curriculumSubjectId",
    "attempt",
    "learningObjectiveCode",
    "curriculumId",
    "lessonCategory",
    "activityTemplateId",
    "activityType",
    "activityComponentResources",
    "materialId",
    "materialType",
    "openPathEnabled",
    "source",
    "teachingPeriodId",
    "academicYear",
    "replayed"
  )

  val expColsForLearningSessionEndEvents: Set[String] = expCommonCols ++ expColsForLearningSessionStartEvents ++ Set(
      "startTime",
      "stars",
      "score",
      "timeSpent",
      "outsideOfSchool",
      "totalScore",
      "isAdditionalResource",
      "bonusStarsScheme",
      "bonusStars"
    )

  val expColsForLearningExperienceEvents: Set[String] = expCommonCols ++ Set(
    "contentPackageItem",
    "studentGradeId",
    "experienceId",
    "learningPathId",
    "schoolId",
    "subjectCode",
    "redo",
    "studentGrade",
    "academicYearId",
    "contentPackageId",
    "studentSection",
    "subjectId",
    "lessonType",
    "contentAcademicYear",
    "learningSessionId",
    "flag",
    "contentId",
    "contentTitle",
    "attempt",
    "academicYearId",
    "instructionalPlanId",
    "contentAcademicYear",
    "lessonCategory",
    "abbreviation",
    "activityType",
    "exitTicket",
    "mainComponent",
    "completionNode",
    "activityComponentType",
    "materialId",
    "materialType",
    "openPathEnabled",
    "source",
    "replayed",
    "isInClassGameExperience",
    "inClassGameOutcomeId"
  )

  val expColsForLearningExperienceStartEvents: Set[String] = expCommonCols ++ expColsForLearningExperienceEvents ++ Set("retry", "teachingPeriodId", "academicYear")

  val expColsForLearningExperienceEndEvents: Set[String] = expCommonCols ++ expColsForLearningExperienceEvents ++ Set(
    "curriculumGradeId",
    "startTime",
    "curriculumSubjectName",
    "stars",
    "activityComponentResources",
    "score",
    "timeSpent",
    "suid",
    "curriculumName",
    "outsideOfSchool",
    "totalScore",
    "totalStars",
    "trimesterOrder",
    "testId",
    "curriculumSubjectId",
    "activityCompleted",
    "scoreBreakDown",
    "scorePresent",
    "curriculumId",
    "level",
    "teachingPeriodId",
    "academicYear",
    "isAdditionalResource",
    "bonusStarsScheme",
    "bonusStars"
  )

  val expColsForLearningExperienceDiscardedEvents: Set[String] = expCommonCols ++ expColsForLearningExperienceEvents ++ Set(
    "curriculumGradeId",
    "startTime",
    "curriculumSubjectName",
    "stars",
    "activityComponentResources",
    "score",
    "timeSpent",
    "suid",
    "curriculumName",
    "outsideOfSchool",
    "totalScore",
    "trimesterOrder",
    "testId",
    "curriculumSubjectId",
    "activityCompleted",
    "scoreBreakDown",
    "scorePresent",
    "curriculumId",
    "level"
  ) -- Set("source")

  val expectedContentPackageItemColumns: Set[String] = Set(
    "uuid",
    "contentUuid",
    "title",
    "lessonType",
    "contentType",
    "abbreviation"
  )

  val expectedScoreBreakdownColumns: Set[String] = Set(
    "id",
    "code",
    "timeSpent",
    "hintsUsed",
    "type",
    "version",
    "maxScore",
    "score",
    "isAttended",
    "lessonIds",
    "attempts",
    "lessonIds",
    "learningOutcomes"
  )

  val expColsForExperienceSubmittedEvents: Set[String] = expCommonCols ++ Set(
    "materialId",
    "activityComponentType",
    "studentK12Grade",
    "startTime",
    "exitTicket",
    "studentGradeId",
    "subjectName",
    "experienceId",
    "learningPathId",
    "mainComponent",
    "schoolId",
    "uuid",
    "lessonCategory",
    "subjectCode",
    "redo",
    "academicYearId",
    "contentPackageId",
    "completionNode",
    "materialType",
    "studentSection",
    "suid",
    "instructionalPlanId",
    "subjectId",
    "lessonType",
    "activityType",
    "contentAcademicYear",
    "contentType",
    "outsideOfSchool",
    "learningSessionId",
    "trimesterOrder",
    "contentId",
    "contentTitle",
    "attempt",
    "abbreviation",
    "openPathEnabled",
    "teachingPeriodId",
    "academicYear",
    "replayed"
  )

  val expColsForContentStartedEvents: Set[String] = expCommonCols ++ Set(
    "uuid",
    "studentK12Grade",
    "studentGradeId",
    "studentSection",
    "subjectCode",
    "subjectName",
    "schoolId",
    "learningPathId",
    "eventDateDw",
    "outsideOfSchool",
    "contentId",
    "academicYearId",
    "contentAcademicYear",
    "instructionalPlanId"
  )

  val expColsForContentEndEvents: Set[String] = expCommonCols ++ expColsForContentStartedEvents ++ Set("vendorData")

  val expColsForTotalScoreUpdatedEvents: Set[String] = expCommonCols ++ Set(
    "learningObjectiveType",
    "studentK12Grade",
    "startTime",
    "curriculumSubjectName",
    "stars",
    "studentGradeId",
    "practiceEnabled",
    "subjectName",
    "learningPathId",
    "studentName",
    "schoolId",
    "uuid",
    "activityTemplateId",
    "lessonCategory",
    "score",
    "subjectCode",
    "redo",
    "studentGrade",
    "academicYearId",
    "learningObjectiveTitle",
    "timeSpent",
    "studentSection",
    "instructionalPlanId",
    "subjectId",
    "activityType",
    "trimesterId",
    "contentAcademicYear",
    "curriculumName",
    "outsideOfSchool",
    "totalScore",
    "learningSessionId",
    "trimesterOrder",
    "curriculumSubjectId",
    "eventSemanticId",
    "activityCompleted",
    "attempt",
    "learningObjectiveCode",
    "eventRoute",
    "curriculumId",
    "activityComponents",
    "curriculumGradeId",
    "materialId",
    "materialType",
    "openPathEnabled",
    "source",
    "teachingPeriodId",
    "academicYear",
    "replayed"
  )

  test("handle LearningSessionStartedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = LearningProgressRawEvents.source,
          value = s"""
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"LearningSessionStarted",
                    |      "tenantId":"$tenantId"
                    |   },
                    |   "body":{
                    |      "occurredOn":"$occurredOn",
                    |      "attempt":1,
                    |      "redo":false,
                    |      "learningSessionId":"$learningSessionId",
                    |      "learningObjectiveId":"$learningObjectiveId",
                    |      "studentId":"$studentId",
                    |      "studentGrade":6,
                    |      "studentGradeId":"d893f582-c61e-4458-a633-cc5d3d349b40",
                    |      "studentSection":"e47308e7-74bd-42b6-be5d-ab0d75399621",
                    |      "classId":"$classId",
                    |      "subjectId":"8fb9c76e-750a-4f19-9676-18e1398bbe52",
                    |      "subjectCode":"MATH",
                    |      "subjectName":"Math",
                    |      "learningObjectiveCode":"HCZ_MA6_M05_009a",
                    |      "learningObjectiveTitle":"Comparing Volumes of Rectangular Prisms",
                    |      "learningObjectiveType":"FF4",
                    |      "trimesterOrder":3,
                    |      "schoolId":"62e57b85-e854-4719-967a-307ee9ba5822",
                    |      "curriculumId":"563622",
                    |      "curriculumName":"NYDOE",
                    |      "curriculumSubjectId":"571671",
                    |      "curriculumSubjectName":"Math",
                    |      "curriculumGradeId":"322135",
                    |      "learningPathId":"daf3bc12-0b8d-43e7-a922-399c13df2452",
                    |      "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
                    |      "contentAcademicYear": "2019",
                    |      "instructionalPlanId": "instructional-plan-id",
                    |      "lessonCategory":"INSTRUCTIONAL_LESSON",
                    |      "materialType":"INSTRUCTIONAL_PLAN",
                    |      "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
                    |      "source": "WEB",
                    |      "teachingPeriodId": "71d76a2c-8f62-4e91-b342-46150de25218",
                    |      "academicYear" : "2024",
                    |      "replayed": true
                    |   }
                    |},
                    | "timestamp": "$loadtime"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "learning-session-started-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("learning-session is not found"))

          df.columns.toSet shouldBe expColsForLearningSessionStartEvents

          val fst = df.first()

          assertCommonCols(fst, "LearningSessionStarted")
          fst.getAs[Int]("attempt") shouldBe 1
          fst.getAs[Boolean]("redo") shouldBe false
          fst.getAs[String]("studentGrade") shouldBe "6"
          fst.getAs[String]("studentGradeId") shouldBe "d893f582-c61e-4458-a633-cc5d3d349b40"
          fst.getAs[String]("studentSection") shouldBe "e47308e7-74bd-42b6-be5d-ab0d75399621"
          fst.getAs[String]("subjectId") shouldBe "8fb9c76e-750a-4f19-9676-18e1398bbe52"
          fst.getAs[String]("subjectCode") shouldBe "MATH"
          fst.getAs[String]("subjectName") shouldBe "Math"
          fst.getAs[String]("learningObjectiveCode") shouldBe "HCZ_MA6_M05_009a"
          fst.getAs[String]("learningObjectiveTitle") shouldBe "Comparing Volumes of Rectangular Prisms"
          fst.getAs[String]("learningObjectiveType") shouldBe "FF4"
          fst.getAs[String]("trimesterId") shouldBe null
          fst.getAs[Int]("trimesterOrder") shouldBe 3
          fst.getAs[String]("schoolId") shouldBe "62e57b85-e854-4719-967a-307ee9ba5822"
          fst.getAs[String]("curriculumId") shouldBe "563622"
          fst.getAs[String]("curriculumName") shouldBe "NYDOE"
          fst.getAs[String]("curriculumSubjectId") shouldBe "571671"
          fst.getAs[String]("curriculumSubjectName") shouldBe "Math"
          fst.getAs[String]("curriculumGradeId") shouldBe "322135"
          fst.getAs[String]("instructionalPlanId") shouldBe "instructional-plan-id"
          fst.getAs[String]("learningPathId") shouldBe "daf3bc12-0b8d-43e7-a922-399c13df2452"
          fst.getAs[String]("academicYearId") shouldBe "e1eff0a1-9469-4581-a4c3-12dbe777c984"
          fst.getAs[String]("contentAcademicYear") shouldBe "2019"
          fst.getAs[String]("lessonCategory") shouldBe "INSTRUCTIONAL_LESSON"
          fst.getAs[String]("materialType") shouldBe "INSTRUCTIONAL_PLAN"
          fst.getAs[String]("materialId") shouldBe "71d76a2c-8f62-4e91-b342-46150de25218"
          fst.getAs[String]("source") shouldBe "WEB"
          fst.getAs[String]("teachingPeriodId") shouldBe "71d76a2c-8f62-4e91-b342-46150de25218"
          fst.getAs[String]("academicYear") shouldBe "2024"
        }
      )
    }
  }

  test("handle LearningSessionFinishedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = LearningProgressRawEvents.source,
          value = s"""
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"LearningSessionFinished",
                    |      "tenantId":"$tenantId"
                    |   },
                    |   "body":{
                    |      "occurredOn":"$occurredOn",
                    |      "attempt":1,
                    |      "redo":false,
                    |      "learningSessionId":"$learningSessionId",
                    |      "learningObjectiveId":"$learningObjectiveId",
                    |      "studentId":"$studentId",
                    |      "studentGrade":6,
                    |      "studentGradeId":"6e70b79d-51f0-4c74-82cb-9ee9d01ab728",
                    |      "studentSection":"96fd3ccb-54e4-4d07-a609-48ad51b8a442",
                    |      "classId":"$classId",
                    |      "subjectId":"1755bdc6-e660-4f80-8e3c-d4580a7da612",
                    |      "subjectCode":"MATH",
                    |      "subjectName":"Math",
                    |      "learningObjectiveCode":"MA6_MLO_164",
                    |      "learningObjectiveTitle":"Mean Absolute Deviation ",
                    |      "learningObjectiveType":"FF4",
                    |      "trimesterId":"7bc796c1-2cf4-4a63-9ab0-b9c04478bd59",
                    |      "trimesterOrder":3,
                    |      "schoolId":"fc604213-e624-4502-a48f-db3f1f1d7667",
                    |      "curriculumId":"392027",
                    |      "curriculumName":"UAE MOE",
                    |      "curriculumSubjectId":"571671",
                    |      "curriculumSubjectName":"Math",
                    |      "curriculumGradeId":"322135",
                    |      "learningPathId":"ba0ba2c9-2482-47e9-9bf0-62db008e3fdb",
                    |      "stars":6,
                    |      "startTime":"2019-07-23T10:52:45.035",
                    |      "score":0.0,
                    |      "outsideOfSchool":true,
                    |      "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
                    |      "contentAcademicYear": "2019",
                    |      "timeSpent": 10,
                    |      "instructionalPlanId": "instructional-plan-id",
                    |      "lessonCategory":"INSTRUCTIONAL_LESSON",
                    |      "totalScore": 20.0,
                    |      "materialType":"INSTRUCTIONAL_PLAN",
                    |      "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
                    |      "isAdditionalResource":true,
                    |      "bonusStars":3,
                    |      "bonusStarsScheme":"BONUS_2X"
                    |   }
                    |},
                    | "timestamp": "$loadtime"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "learning-session-finished-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("learning-session is not found"))

          df.columns.toSet shouldBe expColsForLearningSessionEndEvents

          val fst = df.first()

          assertCommonCols(fst, "LearningSessionFinished")
          fst.getAs[Int]("attempt") shouldBe 1
          fst.getAs[Boolean]("redo") shouldBe false
          fst.getAs[String]("studentGrade") shouldBe "6"
          fst.getAs[String]("studentGradeId") shouldBe "6e70b79d-51f0-4c74-82cb-9ee9d01ab728"
          fst.getAs[String]("studentSection") shouldBe "96fd3ccb-54e4-4d07-a609-48ad51b8a442"
          fst.getAs[String]("subjectId") shouldBe "1755bdc6-e660-4f80-8e3c-d4580a7da612"
          fst.getAs[String]("subjectCode") shouldBe "MATH"
          fst.getAs[String]("subjectName") shouldBe "Math"
          fst.getAs[String]("learningObjectiveCode") shouldBe "MA6_MLO_164"
          fst.getAs[String]("learningObjectiveTitle") shouldBe "Mean Absolute Deviation "
          fst.getAs[String]("learningObjectiveType") shouldBe "FF4"
          fst.getAs[String]("trimesterId") shouldBe "7bc796c1-2cf4-4a63-9ab0-b9c04478bd59"
          fst.getAs[Int]("trimesterOrder") shouldBe 3
          fst.getAs[String]("schoolId") shouldBe "fc604213-e624-4502-a48f-db3f1f1d7667"
          fst.getAs[String]("curriculumId") shouldBe "392027"
          fst.getAs[String]("curriculumName") shouldBe "UAE MOE"
          fst.getAs[String]("curriculumSubjectId") shouldBe "571671"
          fst.getAs[String]("curriculumSubjectName") shouldBe "Math"
          fst.getAs[String]("curriculumGradeId") shouldBe "322135"
          fst.getAs[String]("learningPathId") shouldBe "ba0ba2c9-2482-47e9-9bf0-62db008e3fdb"
          fst.getAs[Boolean]("outsideOfSchool") shouldBe true
          fst.getAs[String]("startTime") shouldBe "2019-07-23T10:52:45.035"
          fst.getAs[Double]("score") shouldBe 0.0
          fst.getAs[Int]("stars") shouldBe 6
          fst.getAs[String]("academicYearId") shouldBe "e1eff0a1-9469-4581-a4c3-12dbe777c984"
          fst.getAs[String]("contentAcademicYear") shouldBe "2019"
          fst.getAs[Int]("timeSpent") shouldBe 10
          fst.getAs[String]("instructionalPlanId") shouldBe "instructional-plan-id"
          fst.getAs[String]("lessonCategory") shouldBe "INSTRUCTIONAL_LESSON"
          fst.getAs[Double]("totalScore") shouldBe 20.0
          fst.getAs[String]("materialType") shouldBe "INSTRUCTIONAL_PLAN"
          fst.getAs[String]("materialId") shouldBe "71d76a2c-8f62-4e91-b342-46150de25218"
          fst.getAs[Int]("bonusStars") shouldBe 3
          fst.getAs[String]("bonusStarsScheme") shouldBe "BONUS_2X"
        }
      )
    }
  }

  test("handle LearningExperienceStartedEvent") {
    new Setup {
      val session = spark

      import session.implicits._

      val fixtures = List(
        SparkFixture(
          key = LearningProgressRawEvents.source,
          value = s"""
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"ExperienceStarted",
                    |      "tenantId":"$tenantId"
                    |   },
                    |   "body":{
                    |      "occurredOn":"$occurredOn",
                    |      "attempt":1,
                    |      "redo":false,
                    |      "learningSessionId":"$learningSessionId",
                    |      "experienceId":"2b7d849c-5ef9-47bf-a849-40b14fb75bc0",
                    |      "learningObjectiveId":"$learningObjectiveId",
                    |      "studentId":"$studentId",
                    |      "schoolId":"62e57b85-e854-4719-967a-307ee9ba5822",
                    |      "studentGrade":0,
                    |      "studentGradeId":"d893f582-c61e-4458-a633-cc5d3d349b40",
                    |      "studentSection":"e47308e7-74bd-42b6-be5d-ab0d75399621",
                    |      "classId":"$classId",
                    |      "subjectId":"8fb9c76e-750a-4f19-9676-18e1398bbe52",
                    |      "subjectCode":"MATH",
                    |      "contentPackageId":"97b19338-7d11-4639-a926-b928b7110799",
                    |      "contentId":"97b19338-7d11-4639-a926-b928b7110799",
                    |      "contentTitle":"Key Terms",
                    |      "lessonType":"KT",
                    |      "flag":"NONE",
                    |      "contentPackageItem":{
                    |         "uuid":"0aa84023-dfe9-4253-928e-a91b17e3f4bf",
                    |         "contentUuid":"97b19338-7d11-4639-a926-b928b7110799",
                    |         "title":"Key Terms",
                    |         "lessonType":"KT"
                    |      },
                    |      "learningPathId":"daf3bc12-0b8d-43e7-a922-399c13df2452",
                    |      "retry":false,
                    |      "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
                    |      "contentAcademicYear": "2019",
                    |      "instructionalPlanId": "instructional-plan-id",
                    |      "lessonCategory":"INSTRUCTIONAL_LESSON",
                    |      "materialType":"INSTRUCTIONAL_PLAN",
                    |      "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
                    |      "replayed": true
                    |   }
                    |},
                    | "timestamp": "$loadtime"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "learning-experience-started-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("learning-session is not found"))

          val contentPackageItemDf = df.select($"contentPackageItem.*")

          df.columns.toSet shouldBe expColsForLearningExperienceStartEvents
          contentPackageItemDf.columns.toSet shouldBe expectedContentPackageItemColumns

          val fst = df.first()

          assertCommonCols(fst, eventType = "ExperienceStarted")
          fst.getAs[Int]("attempt") shouldBe 1
          fst.getAs[Boolean]("redo") shouldBe false
          fst.getAs[Boolean]("retry") shouldBe false
          fst.getAs[String]("flag") shouldBe "NONE"
          fst.getAs[String]("studentGrade") shouldBe "0"
          fst.getAs[String]("studentGradeId") shouldBe "d893f582-c61e-4458-a633-cc5d3d349b40"
          fst.getAs[String]("studentSection") shouldBe "e47308e7-74bd-42b6-be5d-ab0d75399621"
          fst.getAs[String]("subjectId") shouldBe "8fb9c76e-750a-4f19-9676-18e1398bbe52"
          fst.getAs[String]("subjectCode") shouldBe "MATH"
          fst.getAs[String]("schoolId") shouldBe "62e57b85-e854-4719-967a-307ee9ba5822"
          fst.getAs[String]("learningPathId") shouldBe "daf3bc12-0b8d-43e7-a922-399c13df2452"
          fst.getAs[String]("academicYearId") shouldBe "e1eff0a1-9469-4581-a4c3-12dbe777c984"
          fst.getAs[String]("contentAcademicYear") shouldBe "2019"
          fst.getAs[String]("instructionalPlanId") shouldBe "instructional-plan-id"
          fst.getAs[String]("lessonCategory") shouldBe "INSTRUCTIONAL_LESSON"
          fst.getAs[String]("materialType") shouldBe "INSTRUCTIONAL_PLAN"
          fst.getAs[String]("materialId") shouldBe "71d76a2c-8f62-4e91-b342-46150de25218"

          val fstCpI = contentPackageItemDf.first()
          fstCpI.getAs[String]("uuid") shouldBe "0aa84023-dfe9-4253-928e-a91b17e3f4bf"
          fstCpI.getAs[String]("contentUuid") shouldBe "97b19338-7d11-4639-a926-b928b7110799"
          fstCpI.getAs[String]("title") shouldBe "Key Terms"
          fstCpI.getAs[String]("lessonType") shouldBe "KT"
        }
      )
    }
  }

  test("handle LearningExperienceFinishedEvent") {
    new Setup {
      val session = spark
      import session.implicits._

      val fixtures = List(
        SparkFixture(
          key = LearningProgressRawEvents.source,
          value = s"""
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"ExperienceFinished",
                    |      "tenantId":"$tenantId"
                    |   },
                    |   "body":{
                    |      "occurredOn":"$occurredOn",
                    |      "attempt":1,
                    |      "redo":false,
                    |      "learningSessionId":"$learningSessionId",
                    |      "experienceId":"2b7d849c-5ef9-47bf-a849-40b14fb75bc0",
                    |      "learningObjectiveId":"$learningObjectiveId",
                    |      "studentId":"$studentId",
                    |      "schoolId":"62e57b85-e854-4719-967a-307ee9ba5822",
                    |      "studentGrade":0,
                    |      "studentGradeId":"d893f582-c61e-4458-a633-cc5d3d349b40",
                    |      "studentSection":"e47308e7-74bd-42b6-be5d-ab0d75399621",
                    |      "classId":"$classId",
                    |      "subjectId":"8fb9c76e-750a-4f19-9676-18e1398bbe52",
                    |      "subjectCode":"MATH",
                    |      "contentPackageId":"97b19338-7d11-4639-a926-b928b7110799",
                    |      "contentId":"97b19338-7d11-4639-a926-b928b7110799",
                    |      "contentTitle":"Key Terms",
                    |      "lessonType":"KT",
                    |      "flag":"NONE",
                    |      "contentPackageItem":{
                    |         "uuid":"0aa84023-dfe9-4253-928e-a91b17e3f4bf",
                    |         "contentUuid":"97b19338-7d11-4639-a926-b928b7110799",
                    |         "title":"Key Terms",
                    |         "lessonType":"KT"
                    |      },
                    |      "learningPathId":"daf3bc12-0b8d-43e7-a922-399c13df2452",
                    |      "score":0.0,
                    |      "scoreBreakDown":[{
                    |		        "id": 1234,
                    |		        "code": "MA8_MLO_024_Q_36_FIN_IMPORT",
                    |		        "timeSpent": 20,
                    |		        "hintsUsed": true,
                    |		        "type": "MULTIPLE_CHOICE",
                    |		        "version": "3",
                    |		        "maxScore": 1.0,
                    |		        "score": 1.0,
                    |		        "isAttended": true,
                    |		        "attempts": [{
                    |		        	"score": 0,
                    |		        	"hintsUsed": false,
                    |		        	"isAttended": null,
                    |		        	"maxScore": null,
                    |		        	"suid": null,
                    |		        	"timestamp": "2020-05-31T04:57:01.669",
                    |		        	"timeSpent": "2.969"
                    |		        }],
                    |               "lessonIds" : [ "0041779d-a341-4e10-a0f1-000000021837", "670f47a8-8f08-4e3e-b784-000000021836" ],
                    |               "learningOutcomes" : [{
                    |                   "id" : "800489",
                    |                   "type" : "sub_standard",
                    |                   "__typename" : "AssessmentLearningOutcome"
                    |               }]
                    |      }],
                    |      "scorePresent":false,
                    |      "outsideOfSchool":false,
                    |      "testId":"0",
                    |      "suid":null,
                    |      "startTime":"2019-07-23T10:43:52.9",
                    |      "stars":null,
                    |      "curriculumId":"563622",
                    |      "curriculumName":"NYDOE",
                    |      "curriculumSubjectId":"571671",
                    |      "curriculumSubjectName":"Math",
                    |      "curriculumGradeId":"322135",
                    |      "trimesterOrder":3,
                    |      "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
                    |      "contentAcademicYear": "2019",
                    |      "timeSpent": 15,
                    |      "instructionalPlanId": "instructional-plan-id",
                    |      "lessonCategory":"INSTRUCTIONAL_LESSON",
                    |      "level":"B2",
                    |      "totalScore": 20.0,
                    |      "totalStars": 6,
                    |      "activityCompleted": true,
                    |      "materialType":"INSTRUCTIONAL_PLAN",
                    |      "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
                    |      "replayed": true,
                    |      "isAdditionalResource":true,
                    |      "bonusStars": 3,
                    |      "bonusStarsScheme": "BONUS_2X"
                    |   }
                    |},
                    | "timestamp": "$loadtime"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "learning-experience-finished-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("learning-session is not found"))

          val contentPackageItemDf = df.select($"contentPackageItem.*")
          val scoreBreakdownDf = df
            .select($"scoreBreakdown"(0).as("firstScoreBreakdown"))
            .select($"firstScoreBreakdown.*")

          val lessonIdsDf = scoreBreakdownDf.select("lessonIds")
          val learningOutcomesDf = scoreBreakdownDf.select("learningOutcomes")
            .withColumn("learningOutcomes", explode($"learningOutcomes"))
            .select($"learningOutcomes.*")

          df.columns.toSet shouldBe expColsForLearningExperienceEndEvents
          contentPackageItemDf.columns.toSet shouldBe expectedContentPackageItemColumns
          scoreBreakdownDf.columns.toSet shouldBe expectedScoreBreakdownColumns

          val fst = df.first()

          assertCommonCols(fst, eventType = "ExperienceFinished")
          fst.getAs[Int]("attempt") shouldBe 1
          fst.getAs[Boolean]("redo") shouldBe false
          fst.getAs[String]("flag") shouldBe "NONE"
          fst.getAs[String]("stars") shouldBe null
          fst.getAs[String]("studentGrade") shouldBe "0"
          fst.getAs[String]("studentGradeId") shouldBe "d893f582-c61e-4458-a633-cc5d3d349b40"
          fst.getAs[String]("studentSection") shouldBe "e47308e7-74bd-42b6-be5d-ab0d75399621"
          fst.getAs[String]("subjectId") shouldBe "8fb9c76e-750a-4f19-9676-18e1398bbe52"
          fst.getAs[String]("subjectCode") shouldBe "MATH"
          fst.getAs[String]("schoolId") shouldBe "62e57b85-e854-4719-967a-307ee9ba5822"
          fst.getAs[String]("learningPathId") shouldBe "daf3bc12-0b8d-43e7-a922-399c13df2452"
          fst.getAs[String]("curriculumId") shouldBe "563622"
          fst.getAs[String]("curriculumGradeId") shouldBe "322135"
          fst.getAs[String]("startTime") shouldBe "2019-07-23T10:43:52.9"
          fst.getAs[String]("curriculumSubjectName") shouldBe "Math"
          fst.getAs[String]("experienceId") shouldBe "2b7d849c-5ef9-47bf-a849-40b14fb75bc0"
          fst.getAs[Double]("score") shouldBe 0.0
          fst.getAs[String]("contentPackageId") shouldBe "97b19338-7d11-4639-a926-b928b7110799"
          fst.getAs[String]("suid") shouldBe null
          fst.getAs[String]("lessonType") shouldBe "KT"
          fst.getAs[String]("curriculumName") shouldBe "NYDOE"
          fst.getAs[Boolean]("outsideOfSchool") shouldBe false
          fst.getAs[Int]("trimesterOrder") shouldBe 3
          fst.getAs[String]("testId") shouldBe "0"
          fst.getAs[String]("curriculumSubjectId") shouldBe "571671"
          fst.getAs[String]("contentId") shouldBe "97b19338-7d11-4639-a926-b928b7110799"
          fst.getAs[String]("contentTitle") shouldBe "Key Terms"
          fst.getAs[Boolean]("scorePresent") shouldBe false
          fst.getAs[String]("academicYearId") shouldBe "e1eff0a1-9469-4581-a4c3-12dbe777c984"
          fst.getAs[String]("contentAcademicYear") shouldBe "2019"
          fst.getAs[Int]("timeSpent") shouldBe 15
          fst.getAs[String]("instructionalPlanId") shouldBe "instructional-plan-id"
          fst.getAs[String]("lessonCategory") shouldBe "INSTRUCTIONAL_LESSON"
          fst.getAs[String]("level") shouldBe "B2"
          fst.getAs[Double]("totalScore") shouldBe 20.0
          fst.getAs[Boolean]("activityCompleted") shouldBe true
          fst.getAs[String]("materialType") shouldBe "INSTRUCTIONAL_PLAN"
          fst.getAs[String]("materialId") shouldBe "71d76a2c-8f62-4e91-b342-46150de25218"
          fst.getAs[Int]("totalStars") shouldBe 6
          fst.getAs[Int]("bonusStars") shouldBe 3
          fst.getAs[String]("bonusStarsScheme") shouldBe "BONUS_2X"

          val fstCpI = contentPackageItemDf.first()
          fstCpI.getAs[String]("uuid") shouldBe "0aa84023-dfe9-4253-928e-a91b17e3f4bf"
          fstCpI.getAs[String]("contentUuid") shouldBe "97b19338-7d11-4639-a926-b928b7110799"
          fstCpI.getAs[String]("title") shouldBe "Key Terms"
          fstCpI.getAs[String]("lessonType") shouldBe "KT"

          val lessonIds = lessonIdsDf.first()
          lessonIds.getAs[Array[String]]("lessonIds") shouldBe Array("0041779d-a341-4e10-a0f1-000000021837", "670f47a8-8f08-4e3e-b784-000000021836")

          val learningOutcome = learningOutcomesDf.first()
          learningOutcome.getAs[String]("id") shouldBe "800489"
          learningOutcome.getAs[String]("type") shouldBe "sub_standard"
        }
      )
    }
  }

  test("handle ExperienceSubmitted") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = LearningProgressRawEvents.source,
          value = s"""
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"ExperienceSubmitted",
                    |      "tenantId":"$tenantId"
                    |   },
                    |   "body":{
                    |      "uuid":"a1cea7e7-fb3a-45ef-bee4-acff4518a6eb",
                    |      "occurredOn":"$occurredOn",
                    |      "attempt":1,
                    |      "redo":false,
                    |      "learningSessionId":"$learningSessionId",
                    |      "experienceId":"2b7d849c-5ef9-47bf-a849-40b14fb75bc0",
                    |      "learningObjectiveId":"$learningObjectiveId",
                    |      "studentId":"$studentId",
                    |      "schoolId":"62e57b85-e854-4719-967a-307ee9ba5822",
                    |      "studentGradeId":"d893f582-c61e-4458-a633-cc5d3d349b40",
                    |      "studentSection":"e47308e7-74bd-42b6-be5d-ab0d75399621",
                    |      "classId":"$classId",
                    |      "subjectId":"8fb9c76e-750a-4f19-9676-18e1398bbe52",
                    |      "subjectCode":"MATH",
                    |      "contentPackageId":"97b19338-7d11-4639-a926-b928b7110799",
                    |      "contentId":"97b19338-7d11-4639-a926-b928b7110799",
                    |      "contentTitle":"Key Terms",
                    |      "lessonType":"KT",
                    |      "learningPathId":"daf3bc12-0b8d-43e7-a922-399c13df2452",
                    |      "outsideOfSchool":false,
                    |      "suid":null,
                    |      "startTime":"2019-07-23T10:43:52.9",
                    |      "trimesterOrder":3,
                    |      "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
                    |      "contentAcademicYear": "2019",
                    |      "instructionalPlanId": "instructional-plan-id",
                    |      "lessonCategory":"INSTRUCTIONAL_LESSON",
                    |      "studentK12Grade":6,
                    |      "materialType":"INSTRUCTIONAL_PLAN",
                    |      "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
                    |      "replayed": true
                    |   }
                    |},
                    | "timestamp": "$loadtime"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "experience-submitted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("experience-submitted-sink is not found"))

          df.columns.toSet shouldBe expColsForExperienceSubmittedEvents

          val fst = df.first()

          assertCommonCols(fst, eventType = "ExperienceSubmitted")
          fst.getAs[String]("uuid") shouldBe "a1cea7e7-fb3a-45ef-bee4-acff4518a6eb"
          fst.getAs[Int]("attempt") shouldBe 1
          fst.getAs[Boolean]("redo") shouldBe false
          fst.getAs[String]("studentGradeId") shouldBe "d893f582-c61e-4458-a633-cc5d3d349b40"
          fst.getAs[String]("studentSection") shouldBe "e47308e7-74bd-42b6-be5d-ab0d75399621"
          fst.getAs[String]("subjectId") shouldBe "8fb9c76e-750a-4f19-9676-18e1398bbe52"
          fst.getAs[String]("subjectCode") shouldBe "MATH"
          fst.getAs[String]("schoolId") shouldBe "62e57b85-e854-4719-967a-307ee9ba5822"
          fst.getAs[String]("learningPathId") shouldBe "daf3bc12-0b8d-43e7-a922-399c13df2452"
          fst.getAs[String]("startTime") shouldBe "2019-07-23T10:43:52.9"
          fst.getAs[String]("experienceId") shouldBe "2b7d849c-5ef9-47bf-a849-40b14fb75bc0"
          fst.getAs[String]("contentPackageId") shouldBe "97b19338-7d11-4639-a926-b928b7110799"
          fst.getAs[String]("suid") shouldBe null
          fst.getAs[String]("lessonType") shouldBe "KT"
          fst.getAs[Boolean]("outsideOfSchool") shouldBe false
          fst.getAs[Int]("trimesterOrder") shouldBe 3
          fst.getAs[String]("contentId") shouldBe "97b19338-7d11-4639-a926-b928b7110799"
          fst.getAs[String]("contentTitle") shouldBe "Key Terms"
          fst.getAs[String]("academicYearId") shouldBe "e1eff0a1-9469-4581-a4c3-12dbe777c984"
          fst.getAs[String]("contentAcademicYear") shouldBe "2019"
          fst.getAs[String]("instructionalPlanId") shouldBe "instructional-plan-id"
          fst.getAs[String]("lessonCategory") shouldBe "INSTRUCTIONAL_LESSON"
          fst.getAs[Int]("studentK12Grade") shouldBe 6
          fst.getAs[String]("materialType") shouldBe "INSTRUCTIONAL_PLAN"
          fst.getAs[String]("materialId") shouldBe "71d76a2c-8f62-4e91-b342-46150de25218"
        }
      )
    }
  }

  test("handle ContentStartedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = LearningProgressRawEvents.source,
          value = s"""
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"ContentStartedEvent",
                    |      "tenantId":"$tenantId"
                    |   },
                    |   "body":{
                    |   "uuid":"3a04e4d7-8868-445b-93b2-09f6530f597e",
                    |   "learningSessionId":"$learningSessionId",
                    |   "learningObjectiveId":"$learningObjectiveId",
                    |   "studentId":"$studentId",
                    |   "studentK12Grade":6,
                    |   "studentGradeId":"c362ac41-c90f-4ac3-8ead-74e199603aba",
                    |   "studentSection":"7a91a97b-4643-4a26-95f6-3eaa481a0032",
                    |   "learningPathId":"d5d10524-6b73-4fe7-8a9b-27b8209f2725",
                    |   "instructionalPlanId":"e1ea5911-6e31-4839-be17-59f248810cda",
                    |   "subjectCode":"SOCIAL_STUDIES",
                    |   "contentId":"08cec393-3158-4776-b527-37f5900d46f2",
                    |   "subjectName":"Gen Subject",
                    |   "schoolId":"9e10cd3c-077b-44b7-ab89-127e893954bc",
                    |   "academicYearId":"44b110a3-5839-4036-874e-f16995bcdc03",
                    |   "contentAcademicYear":"2019",
                    |   "classId":"$classId",
                    |   "outsideOfSchool":false,
                    |   "occurredOn":"$occurredOn"
                    |   }
                    |   },
                    | "timestamp": "$loadtime"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "learning-content-started-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("learning-content-started-sink is not found"))

          df.columns.toSet shouldBe expColsForContentStartedEvents

          val fst = df.first()

          assertCommonCols(fst, eventType = "ContentStartedEvent")
          fst.getAs[String]("uuid") shouldBe "3a04e4d7-8868-445b-93b2-09f6530f597e"
          fst.getAs[Int]("studentK12Grade") shouldBe 6
          fst.getAs[String]("studentGradeId") shouldBe "c362ac41-c90f-4ac3-8ead-74e199603aba"
          fst.getAs[String]("studentSection") shouldBe "7a91a97b-4643-4a26-95f6-3eaa481a0032"
          fst.getAs[String]("subjectCode") shouldBe "SOCIAL_STUDIES"
          fst.getAs[String]("schoolId") shouldBe "9e10cd3c-077b-44b7-ab89-127e893954bc"
          fst.getAs[String]("learningPathId") shouldBe "d5d10524-6b73-4fe7-8a9b-27b8209f2725"
          fst.getAs[Boolean]("outsideOfSchool") shouldBe false
          fst.getAs[String]("contentId") shouldBe "08cec393-3158-4776-b527-37f5900d46f2"
          fst.getAs[String]("academicYearId") shouldBe "44b110a3-5839-4036-874e-f16995bcdc03"
          fst.getAs[String]("contentAcademicYear") shouldBe "2019"
          fst.getAs[String]("instructionalPlanId") shouldBe "e1ea5911-6e31-4839-be17-59f248810cda"
        }
      )
    }
  }

  test("handle ContentFinishedEvent") {
    new Setup {
      val session = spark

      import session.implicits._

      val fixtures = List(
        SparkFixture(
          key = LearningProgressRawEvents.source,
          value = s"""
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"ContentFinishedEvent",
                    |      "tenantId":"$tenantId"
                    |   },
                    |   "body":{
                    |       "uuid":"3a04e4d7-8868-445b-93b2-09f6530f597e",
                    |       "learningSessionId":"$learningSessionId",
                    |       "learningObjectiveId":"$learningObjectiveId",
                    |       "studentId":"$studentId",
                    |       "studentK12Grade":6,
                    |       "studentGradeId":"c362ac41-c90f-4ac3-8ead-74e199603aba",
                    |       "studentSection":"7a91a97b-4643-4a26-95f6-3eaa481a0032",
                    |       "learningPathId":"d5d10524-6b73-4fe7-8a9b-27b8209f2725",
                    |       "instructionalPlanId":"e1ea5911-6e31-4839-be17-59f248810cda",
                    |       "subjectCode":"SOCIAL_STUDIES",
                    |       "contentId":"08cec393-3158-4776-b527-37f5900d46f2",
                    |       "subjectName":"Gen Subject",
                    |       "schoolId":"9e10cd3c-077b-44b7-ab89-127e893954bc",
                    |       "academicYearId":"44b110a3-5839-4036-874e-f16995bcdc03",
                    |       "contentAcademicYear":"2019",
                    |       "classId":"$classId",
                    |       "outsideOfSchool":false,
                    |       "occurredOn":"$occurredOn",
                    |       "vendorData" : {
                    |           "score" : 100.00,
                    |           "timeSpent": 120
                    |        }
                    |     }
                    |  },
                    | "timestamp": "$loadtime"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "learning-content-finished-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("learning-content-finished-sink is not found"))

          val vendorData = df.select($"vendorData.*")

          df.columns.toSet shouldBe expColsForContentEndEvents

          val fst = df.first()

          assertCommonCols(fst, eventType = "ContentFinishedEvent")
          fst.getAs[String]("uuid") shouldBe "3a04e4d7-8868-445b-93b2-09f6530f597e"
          fst.getAs[Int]("studentK12Grade") shouldBe 6
          fst.getAs[String]("studentGradeId") shouldBe "c362ac41-c90f-4ac3-8ead-74e199603aba"
          fst.getAs[String]("studentSection") shouldBe "7a91a97b-4643-4a26-95f6-3eaa481a0032"
          fst.getAs[String]("subjectCode") shouldBe "SOCIAL_STUDIES"
          fst.getAs[String]("schoolId") shouldBe "9e10cd3c-077b-44b7-ab89-127e893954bc"
          fst.getAs[String]("learningPathId") shouldBe "d5d10524-6b73-4fe7-8a9b-27b8209f2725"
          fst.getAs[Boolean]("outsideOfSchool") shouldBe false
          fst.getAs[String]("contentId") shouldBe "08cec393-3158-4776-b527-37f5900d46f2"
          fst.getAs[String]("academicYearId") shouldBe "44b110a3-5839-4036-874e-f16995bcdc03"
          fst.getAs[String]("contentAcademicYear") shouldBe "2019"
          fst.getAs[String]("instructionalPlanId") shouldBe "e1ea5911-6e31-4839-be17-59f248810cda"

          val fstCpI = vendorData.first()
          fstCpI.getAs[Double]("score") shouldBe 100.00
          fstCpI.getAs[Int]("timeSpent") shouldBe 120
        }
      )
    }
  }

  test("handle TotalScoreUpdatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = LearningProgressRawEvents.source,
          value = s"""
                    |[
                    |{
                    | "key": "key1",
                    | "value": {
                    |   "headers":{
                    |      "eventType":"TotalScoreUpdatedEvent",
                    |      "tenantId":"$tenantId"
                    |   },
                    |   "body":{
                    |       "type":"TotalScoreUpdatedEvent",
                    |       "uuid":"bfba9302-da96-408f-a0b4-dfe80504a1fc",
                    |       "occurredOn":"$occurredOn",
                    |       "attempt":1,
                    |       "learningSessionId":"$learningSessionId",
                    |       "learningObjectiveId":"$learningObjectiveId",
                    |       "studentId":"$studentId",
                    |       "studentName":"St60 Test",
                    |       "studentK12Grade":7,
                    |       "studentGradeId":"f87b5a01-5327-40cf-ad96-2dddb5abdff5",
                    |       "studentSection":"960998a9-584d-4add-9557-cdeead3395ee",
                    |       "learningPathId":"4c172cc6-0c43-4cc4-9648-3e7756261482",
                    |       "instructionalPlanId":"f92c4dda-0ad9-464b-a005-67d581ada1ed",
                    |       "subjectId":null,
                    |       "subjectCode":"ENGLISH_MOE",
                    |       "subjectName":"English",
                    |       "learningObjectiveCode":"EN7_MLO_204_ASGN",
                    |       "learningObjectiveTitle":"Using Percentages and Fractions_A",
                    |       "learningObjectiveType":"FF4",
                    |       "schoolId":"fa465fed-41ac-4165-9849-e20426ba688d",
                    |       "trimesterId":"bd94be12-6c63-41d0-855c-80dbb2e01fd8",
                    |       "trimesterOrder":3,
                    |       "curriculumId":"392027",
                    |       "curriculumName":"UAE MOE",
                    |       "curriculumSubjectId":"423412",
                    |       "curriculumSubjectName":"English_MOE",
                    |       "curriculumGradeId":"596550",
                    |       "academicYearId":"78ef63bc-3156-4b31-b8e4-b48f03c0b6e1",
                    |       "contentAcademicYear":"2021",
                    |       "classId":"$classId",
                    |       "startTime":"2021-09-13T09:12:19.253",
                    |       "outsideOfSchool":false,
                    |       "practiceEnabled":false,
                    |       "lessonCategory":"INSTRUCTIONAL_LESSON",
                    |       "activityTemplateId":"FF4",
                    |       "activityType":"INSTRUCTIONAL_LESSON",
                    |       "activityComponents":[
                    |          {
                    |             "activityComponentType":"CONTENT",
                    |             "exitTicket":false,
                    |             "activityComponentId":"9e258568-9551-491b-9e77-09e0575da0e0",
                    |             "mainComponent":true,
                    |             "alwaysEnabled":true,
                    |             "abbreviation":"AC"
                    |          },
                    |          {
                    |             "activityComponentType":"CONTENT",
                    |             "exitTicket":false,
                    |             "activityComponentId":"68a60a05-d9d6-4dab-bb1e-0bafa1ba6a4c",
                    |             "mainComponent":false,
                    |             "alwaysEnabled":false,
                    |             "abbreviation":"MP"
                    |          },
                    |          {
                    |             "activityComponentType":"KEY_TERM",
                    |             "exitTicket":false,
                    |             "activityComponentId":"e8c056db-f5bd-49a3-a59d-f28aee712541",
                    |             "mainComponent":false,
                    |             "alwaysEnabled":false,
                    |             "abbreviation":"KT"
                    |          },
                    |          {
                    |             "activityComponentType":"CONTENT",
                    |             "exitTicket":false,
                    |             "activityComponentId":"b04817fd-7633-41f7-ab9c-eff891e982fa",
                    |             "mainComponent":true,
                    |             "alwaysEnabled":true,
                    |             "abbreviation":"LESSON_1"
                    |          },
                    |          {
                    |             "activityComponentType":"ASSESSMENT",
                    |             "exitTicket":false,
                    |             "activityComponentId":"c81c247d-266f-48e4-937d-c24c5d6582a3",
                    |             "mainComponent":true,
                    |             "alwaysEnabled":true,
                    |             "abbreviation":"TEQ_1"
                    |          },
                    |          {
                    |             "activityComponentType":"CONTENT",
                    |             "exitTicket":false,
                    |             "activityComponentId":"85f42ee6-a4a0-4dd2-979c-6e5c5b955f31",
                    |             "mainComponent":false,
                    |             "alwaysEnabled":false,
                    |             "abbreviation":"R"
                    |          },
                    |          {
                    |             "activityComponentType":"CONTENT",
                    |             "exitTicket":false,
                    |             "activityComponentId":"f0c8afec-f250-4a44-b2be-c98fee13b2f0",
                    |             "mainComponent":true,
                    |             "alwaysEnabled":true,
                    |             "abbreviation":"LESSON_2"
                    |          },
                    |          {
                    |             "activityComponentType":"ASSESSMENT",
                    |             "exitTicket":false,
                    |             "activityComponentId":"963fb97e-73ee-4929-a113-b374339ece13",
                    |             "mainComponent":true,
                    |             "alwaysEnabled":true,
                    |             "abbreviation":"TEQ_2"
                    |          },
                    |          {
                    |             "activityComponentType":"ASSESSMENT",
                    |             "exitTicket":true,
                    |             "activityComponentId":"e586a18c-7008-4c27-8846-6282e05e7f31",
                    |             "mainComponent":true,
                    |             "alwaysEnabled":true,
                    |             "abbreviation":"SA"
                    |          },
                    |          {
                    |             "activityComponentType":"ASSIGNMENT",
                    |             "exitTicket":false,
                    |             "activityComponentId":"0e03e0d8-7204-40bf-90bf-0455b7fbe0de",
                    |             "mainComponent":true,
                    |             "alwaysEnabled":true,
                    |             "abbreviation":"ASGN"
                    |          },
                    |          {
                    |             "activityComponentType":"CONTENT",
                    |             "exitTicket":false,
                    |             "activityComponentId":"974ab4f0-6ae4-48f4-93f8-84823640c670",
                    |             "mainComponent":false,
                    |             "alwaysEnabled":false,
                    |             "abbreviation":"R_2"
                    |          }
                    |       ],
                    |       "stars":1,
                    |       "score":0,
                    |       "timeSpent":79,
                    |       "totalScore":37.5,
                    |       "activityCompleted":false,
                    |       "eventRoute":"learning.score.updated",
                    |       "eventSemanticId":"d105ac31-7731-435f-a935-3239baa9b152",
                    |       "studentGrade":"7",
                    |       "redo":false,
                    |       "materialType":"INSTRUCTIONAL_PLAN",
                    |       "materialId":"8ca0d670-5d7a-47dc-b578-ff8458a74a7e",
                    |       "replayed": true
                    |    }
                    |  },
                    | "timestamp": "$loadtime"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "learning-total-score-updated-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("learning-total-score-updated-sink is not found"))

          df.columns.toSet shouldBe expColsForTotalScoreUpdatedEvents

          val fst = df.first()

          assertCommonCols(fst, eventType = "TotalScoreUpdatedEvent")
          fst.getAs[String]("uuid") shouldBe "bfba9302-da96-408f-a0b4-dfe80504a1fc"
          fst.getAs[Int]("studentK12Grade") shouldBe 7
          fst.getAs[String]("studentGradeId") shouldBe "f87b5a01-5327-40cf-ad96-2dddb5abdff5"
          fst.getAs[String]("studentSection") shouldBe "960998a9-584d-4add-9557-cdeead3395ee"
          fst.getAs[String]("subjectCode") shouldBe "ENGLISH_MOE"
          fst.getAs[String]("subjectName") shouldBe "English"
          fst.getAs[String]("schoolId") shouldBe "fa465fed-41ac-4165-9849-e20426ba688d"
          fst.getAs[String]("learningPathId") shouldBe "4c172cc6-0c43-4cc4-9648-3e7756261482"
          fst.getAs[Boolean]("outsideOfSchool") shouldBe false
          fst.getAs[Boolean]("practiceEnabled") shouldBe false
          fst.getAs[String]("lessonCategory") shouldBe "INSTRUCTIONAL_LESSON"
          fst.getAs[String]("academicYearId") shouldBe "78ef63bc-3156-4b31-b8e4-b48f03c0b6e1"
          fst.getAs[String]("contentAcademicYear") shouldBe "2021"
          fst.getAs[String]("instructionalPlanId") shouldBe "f92c4dda-0ad9-464b-a005-67d581ada1ed"
          fst.getAs[Int]("stars") shouldBe 1
          fst.getAs[Int]("score") shouldBe 0
          fst.getAs[Int]("timeSpent") shouldBe 79
          fst.getAs[Double]("totalScore") shouldBe 37.5
          fst.getAs[Boolean]("activityCompleted") shouldBe false
          fst.getAs[String]("eventRoute") shouldBe "learning.score.updated"
          fst.getAs[String]("eventSemanticId") shouldBe "d105ac31-7731-435f-a935-3239baa9b152"
          fst.getAs[String]("studentGrade") shouldBe "7"
          fst.getAs[Boolean]("redo") shouldBe false
          fst.getAs[String]("activityType") shouldBe "INSTRUCTIONAL_LESSON"
          fst.getAs[String]("activityTemplateId") shouldBe "FF4"
          fst.getAs[String]("startTime") shouldBe "2021-09-13T09:12:19.253"
          fst.getAs[String]("curriculumGradeId") shouldBe "596550"
          fst.getAs[String]("curriculumSubjectName") shouldBe "English_MOE"
          fst.getAs[String]("curriculumSubjectId") shouldBe "423412"
          fst.getAs[String]("curriculumName") shouldBe "UAE MOE"
          fst.getAs[String]("curriculumId") shouldBe "392027"
          fst.getAs[Int]("trimesterOrder") shouldBe 3
          fst.getAs[String]("trimesterId") shouldBe "bd94be12-6c63-41d0-855c-80dbb2e01fd8"
          fst.getAs[String]("schoolId") shouldBe "fa465fed-41ac-4165-9849-e20426ba688d"
          fst.getAs[String]("learningObjectiveType") shouldBe "FF4"
          fst.getAs[String]("learningObjectiveTitle") shouldBe "Using Percentages and Fractions_A"
          fst.getAs[String]("learningObjectiveCode") shouldBe "EN7_MLO_204_ASGN"
          fst.getAs[String]("subjectId") shouldBe null
          fst.getAs[Int]("attempt") shouldBe 1
          fst.getAs[String]("studentName") shouldBe "St60 Test"
          fst.getAs[String]("materialType") shouldBe "INSTRUCTIONAL_PLAN"
          fst.getAs[String]("materialId") shouldBe "8ca0d670-5d7a-47dc-b578-ff8458a74a7e"
        }
      )
    }
  }

  test("consume ExperienceDiscardedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = LearningProgressRawEvents.source,
          value = s"""
                     |[
                     |{
                     | "key": "key1",
                     | "value": {
                     |   "headers":{
                     |      "eventType":"ExperienceDiscarded",
                     |      "tenantId":"$tenantId"
                     |   },
                     |   "body":{
                     |      "occurredOn":"$occurredOn",
                     |      "attempt":1,
                     |      "redo":false,
                     |      "learningSessionId":"$learningSessionId",
                     |      "experienceId":"2b7d849c-5ef9-47bf-a849-40b14fb75bc0",
                     |      "learningObjectiveId":"$learningObjectiveId",
                     |      "studentId":"$studentId",
                     |      "schoolId":"62e57b85-e854-4719-967a-307ee9ba5822",
                     |      "studentGrade":0,
                     |      "studentGradeId":"d893f582-c61e-4458-a633-cc5d3d349b40",
                     |      "studentSection":"e47308e7-74bd-42b6-be5d-ab0d75399621",
                     |      "classId":"$classId",
                     |      "subjectId":"8fb9c76e-750a-4f19-9676-18e1398bbe52",
                     |      "subjectCode":"MATH",
                     |      "contentPackageId":"97b19338-7d11-4639-a926-b928b7110799",
                     |      "contentId":"97b19338-7d11-4639-a926-b928b7110799",
                     |      "contentTitle":"Key Terms",
                     |      "lessonType":"KT",
                     |      "flag":"NONE",
                     |      "contentPackageItem":{
                     |         "uuid":"0aa84023-dfe9-4253-928e-a91b17e3f4bf",
                     |         "contentUuid":"97b19338-7d11-4639-a926-b928b7110799",
                     |         "title":"Key Terms",
                     |         "lessonType":"KT"
                     |      },
                     |      "learningPathId":"daf3bc12-0b8d-43e7-a922-399c13df2452",
                     |      "score":0.0,
                     |      "scoreBreakDown":[{
                     |		        "id": 1234,
                     |		        "code": "MA8_MLO_024_Q_36_FIN_IMPORT",
                     |		        "timeSpent": 20,
                     |		        "hintsUsed": true,
                     |		        "type": "MULTIPLE_CHOICE",
                     |		        "version": "3",
                     |		        "maxScore": 1.0,
                     |		        "score": 1.0,
                     |		        "isAttended": true,
                     |		        "attempts": [{
                     |		        	"score": 0,
                     |		        	"hintsUsed": false,
                     |		        	"isAttended": null,
                     |		        	"maxScore": null,
                     |		        	"suid": null,
                     |		        	"timestamp": "2020-05-31T04:57:01.669",
                     |		        	"timeSpent": "2.969"
                     |		        }],
                     |               "lessonIds" : [ "0041779d-a341-4e10-a0f1-000000021837", "670f47a8-8f08-4e3e-b784-000000021836" ],
                     |               "learningOutcomes" : [{
                     |                   "id" : "800489",
                     |                   "type" : "sub_standard",
                     |                   "__typename" : "AssessmentLearningOutcome"
                     |               }]
                     |      }],
                     |      "scorePresent":false,
                     |      "outsideOfSchool":false,
                     |      "testId":"0",
                     |      "suid":null,
                     |      "startTime":"2019-07-23T10:43:52.9",
                     |      "stars":null,
                     |      "curriculumId":"563622",
                     |      "curriculumName":"NYDOE",
                     |      "curriculumSubjectId":"571671",
                     |      "curriculumSubjectName":"Math",
                     |      "curriculumGradeId":"322135",
                     |      "trimesterOrder":3,
                     |      "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
                     |      "contentAcademicYear": "2019",
                     |      "timeSpent": 15,
                     |      "instructionalPlanId": "instructional-plan-id",
                     |      "lessonCategory":"INSTRUCTIONAL_LESSON",
                     |      "level":"B2",
                     |      "totalScore": 20.0,
                     |      "activityCompleted": true,
                     |      "materialType":"INSTRUCTIONAL_PLAN",
                     |      "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
                     |      "replayed": true
                     |   }
                     |},
                     | "timestamp": "$loadtime"
                     |}
                     |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "learning-experience-discarded-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("learning-experience-discarded-sink is not found"))

          df.columns.toSet shouldBe expColsForLearningExperienceDiscardedEvents

          val fst = df.first()
          assertCommonCols(fst, "ExperienceDiscarded")
          fst.getAs[Int]("attempt") shouldBe 1
          fst.getAs[Boolean]("redo") shouldBe false
          fst.getAs[String]("flag") shouldBe "NONE"
          fst.getAs[String]("stars") shouldBe null
          fst.getAs[String]("studentGrade") shouldBe "0"
          fst.getAs[String]("studentGradeId") shouldBe "d893f582-c61e-4458-a633-cc5d3d349b40"
          fst.getAs[String]("studentSection") shouldBe "e47308e7-74bd-42b6-be5d-ab0d75399621"
          fst.getAs[String]("subjectId") shouldBe "8fb9c76e-750a-4f19-9676-18e1398bbe52"
          fst.getAs[String]("subjectCode") shouldBe "MATH"
          fst.getAs[String]("schoolId") shouldBe "62e57b85-e854-4719-967a-307ee9ba5822"
          fst.getAs[String]("learningPathId") shouldBe "daf3bc12-0b8d-43e7-a922-399c13df2452"
          fst.getAs[String]("curriculumId") shouldBe "563622"
          fst.getAs[String]("curriculumGradeId") shouldBe "322135"
          fst.getAs[String]("startTime") shouldBe "2019-07-23T10:43:52.9"
          fst.getAs[String]("curriculumSubjectName") shouldBe "Math"
          fst.getAs[String]("experienceId") shouldBe "2b7d849c-5ef9-47bf-a849-40b14fb75bc0"
          fst.getAs[Double]("score") shouldBe 0.0
          fst.getAs[String]("contentPackageId") shouldBe "97b19338-7d11-4639-a926-b928b7110799"
          fst.getAs[String]("suid") shouldBe null
          fst.getAs[String]("lessonType") shouldBe "KT"
          fst.getAs[String]("curriculumName") shouldBe "NYDOE"
          fst.getAs[Boolean]("outsideOfSchool") shouldBe false
          fst.getAs[Int]("trimesterOrder") shouldBe 3
          fst.getAs[String]("testId") shouldBe "0"
          fst.getAs[String]("curriculumSubjectId") shouldBe "571671"
          fst.getAs[String]("contentId") shouldBe "97b19338-7d11-4639-a926-b928b7110799"
          fst.getAs[String]("contentTitle") shouldBe "Key Terms"
          fst.getAs[Boolean]("scorePresent") shouldBe false
          fst.getAs[String]("academicYearId") shouldBe "e1eff0a1-9469-4581-a4c3-12dbe777c984"
          fst.getAs[String]("contentAcademicYear") shouldBe "2019"
          fst.getAs[Int]("timeSpent") shouldBe 15
          fst.getAs[String]("instructionalPlanId") shouldBe "instructional-plan-id"
          fst.getAs[String]("lessonCategory") shouldBe "INSTRUCTIONAL_LESSON"
          fst.getAs[String]("level") shouldBe "B2"
          fst.getAs[Double]("totalScore") shouldBe 20.0
          fst.getAs[Boolean]("activityCompleted") shouldBe true
          fst.getAs[String]("materialType") shouldBe "INSTRUCTIONAL_PLAN"
          fst.getAs[String]("materialId") shouldBe "71d76a2c-8f62-4e91-b342-46150de25218"
        }
      )
    }
  }

  test("consume LearningSessionDeletedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = LearningProgressRawEvents.source,
          value = s"""
                     |[
                     |{
                     | "key": "key1",
                     | "value": {
                     |   "headers":{
                     |      "eventType":"ExperienceDiscarded",
                     |      "tenantId":"$tenantId"
                     |   },
                     |   "body": {
                     |	"type": "LearningSessionDeletedEvent",
                     |	"uuid": "e554c6e4-ae78-4b6a-bed2-20df8ff02cb8",
                     |	"occurredOn": "2023-12-13T08:26:39.306",
                     |	"learningSessionId": "0c3b4438-5ca4-4a75-97af-531c9ee298d4",
                     |	"learningObjectiveId": "07e0c841-c813-46c9-a8e2-000000028363",
                     |	"studentId": "5417a4da-8062-4677-8a93-2768a4d7a239",
                     |	"studentName": "St1119 St1119",
                     |	"schoolId": "3c4d2d1a-9e66-4aa8-bdbc-16a520fd3450",
                     |	"studentK12Grade": 6,
                     |	"studentGradeId": "9d9d79a2-32dc-48f8-8f0d-05cd7b5dfe43",
                     |	"studentSection": "b630e406-7785-43cc-9ebf-366d01d49ba9",
                     |	"learningPathId": null,
                     |	"instructionalPlanId": null,
                     |	"subjectId": null,
                     |	"subjectCode": "Math",
                     |	"subjectName": "Math",
                     |	"academicYearId": "a9756e3c-db1c-44a1-b8c1-c9dd3f64ae3c",
                     |	"contentAcademicYear": "2023",
                     |	"classId": "2b82055e-3b1a-4e2d-8da1-302b1266d7cf",
                     |	"outsideOfSchool": false,
                     |	"lessonCategory": "INSTRUCTIONAL_LESSON",
                     |	"activityType": "INSTRUCTIONAL_LESSON",
                     |	"materialType": "PATHWAY",
                     |	"materialId": "6ddb0a44-4115-430c-8252-bc4a5d65ad88",
                     |	"attempt": 1,
                     |	"learningObjectiveCode": "pathway-3",
                     |	"learningObjectiveTitle": "pathway-3",
                     |	"learningObjectiveType": "0a74a826-bfa8-4fa7-a911-420d9b178dcd",
                     |	"trimesterId": 1,
                     |	"trimesterOrder": 1,
                     |	"curriculumId": "392027",
                     |	"curriculumName": "UAE MOE",
                     |	"curriculumSubjectId": "571671",
                     |	"curriculumSubjectName": "Math",
                     |	"curriculumGradeId": "322135",
                     |	"startTime": "2023-10-18T17:45:17.746",
                     |	"practiceEnabled": false,
                     |	"activityTemplateId": "0a74a826-bfa8-4fa7-a911-420d9b178dcd",
                     |	"activityComponents": [
                     |		{
                     |			"activityComponentType": "CONTENT",
                     |			"exitTicket": false,
                     |			"activityComponentId": "ae51136b-5f7a-4289-a26c-c7ff67c435a8",
                     |			"mainComponent": true,
                     |			"alwaysEnabled": true,
                     |			"abbreviation": "CNT"
                     |		},
                     |		{
                     |			"activityComponentType": "CONTENT",
                     |			"exitTicket": false,
                     |			"activityComponentId": "24db6d8f-005a-4333-8fe9-72b7fe7e5717",
                     |			"mainComponent": true,
                     |			"alwaysEnabled": false,
                     |			"abbreviation": "SL"
                     |		},
                     |		{
                     |			"activityComponentType": "CONTENT",
                     |			"exitTicket": false,
                     |			"activityComponentId": "16427628-8861-4481-91fa-fd53214d02d0",
                     |			"mainComponent": true,
                     |			"alwaysEnabled": false,
                     |			"abbreviation": "LL"
                     |		},
                     |		{
                     |			"activityComponentType": "ASSESSMENT",
                     |			"exitTicket": false,
                     |			"activityComponentId": "af4e9862-6324-4b65-b992-d94a63be03a8",
                     |			"mainComponent": true,
                     |			"alwaysEnabled": false,
                     |			"abbreviation": "CMU"
                     |		},
                     |		{
                     |			"activityComponentType": "ASSESSMENT",
                     |			"exitTicket": false,
                     |			"activityComponentId": "0fc33e98-147c-4afd-bd4d-95de84d69389",
                     |			"mainComponent": true,
                     |			"alwaysEnabled": false,
                     |			"abbreviation": "ASM"
                     |		},
                     |		{
                     |			"activityComponentType": "ASSESSMENT",
                     |			"exitTicket": true,
                     |			"activityComponentId": "34f74f7d-c130-4865-8e66-b2f2a23a29e9",
                     |			"mainComponent": true,
                     |			"alwaysEnabled": false,
                     |			"abbreviation": "ET"
                     |		}
                     |	],
                     |	"openPathEnabled": false,
                     |	"studentGrade": "6"
                     |}
                     |},
                     | "timestamp": "$loadtime"
                     |}
                     |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks
            .find(_.name == "learning-session-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("learning-session-deleted-sink is not found"))

          df.columns.toSet shouldBe (expColsForLearningSessionEndEvents -- Set("timeSpent",
            "totalScore",
            "stars",
            "activityComponentResources",
            "score",
            "redo", "source", "isAdditionalResource",  "bonusStarsScheme",
            "bonusStars"))
        }
      )
    }
  }

  test("handle ebook progress events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "id",
        "action",
        "studentId",
        "lessonId",
        "contentId",
        "contentHash",
        "sessionId",
        "experienceId",
        "schoolId",
        "gradeId",
        "classId",
        "materialType",
        "academicYearTag",
        "ebookMetaData",
        "progressData",
        "bookmark",
        "highlight",
        "occurredOn",
        "eventType",
        "eventDateDw",
        "loadtime",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = LearningProgressRawEvents.source,
          value =
            """
              |[
              |{
              | "key": "a12ffa9d-18bb-430c-b0b8-ad35c964f590",
              | "value":{
              |    "headers":{
              |       "eventType":"EbookProgressEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              | "body":{
              |    "id": "cc1af4ca-1acc-49b3-bb97-b62a56efde03",
              |    "action": "PROGRESS",
              |    "studentId": "c16d3b48-9fb4-4468-8d13-ba940067bc4b",
              |    "lessonId": "2acd5b0a-1dc9-49e9-962c-000000028475",
              |    "contentId": "abcd2bcc-b7e9-4d1a-86b4-94c844c5bc2b",
              |    "contentHash": "abc123def456",
              |    "sessionId": "acec2bcc-b7e9-4d1a-86b4-94c844c5bc2b",
              |    "experienceId": "acec2bcc-b7e9-4d1a-86b4-94c844c5bc2b",
              |    "schoolId": "acec2bcc-b7e9-4d1a-86b4-94c844c5bc2b",
              |    "gradeId": "acec2bcc-b7e9-4d1a-86b4-94c844c5bc2b",
              |    "classId": "acec2bcc-b7e9-4d1a-86b4-94c844c5bc2b",
              |    "materialType": "CORE",
              |    "academicYearTag": "2024",
              |    "ebookMetaData": {
              |       "title": "Test book",
              |       "totalPages": 100,
              |       "hasAudio": false
              |    },
              |    "progressData": {
              |       "location": "epubcfi(/6/22!/4/2/12/3:244)",
              |       "isLastPage": false,
              |       "status": "IN_PROGRESS",
              |       "timeSpent": 10.2
              |    },
              |    "bookmark": null,
              |    "highlight": null,
              |    "occurredOn": 1708929191264
              |}
              | },
              | "timestamp": "2023-10-26 16:23:46.609"
              |}
              |]
              """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "learning-ebook-progress-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)
          dfOpt.map(_.collect().length) shouldBe Some(1)

          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("eventType") shouldBe "EbookProgressEvent"
            fst.getAs[String]("loadtime") shouldBe "2023-10-26 16:23:46.609"
            fst.getAs[String]("occurredOn") shouldBe "2024-02-26 06:33:11.264"
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("lessonId") shouldBe "2acd5b0a-1dc9-49e9-962c-000000028475"
            fst.getAs[String]("action") shouldBe "PROGRESS"
            fst.getAs[String]("studentId") shouldBe "c16d3b48-9fb4-4468-8d13-ba940067bc4b"
            fst.getAs[String]("contentId") shouldBe "abcd2bcc-b7e9-4d1a-86b4-94c844c5bc2b"
            fst.getAs[String]("contentHash") shouldBe "abc123def456"
            fst.getAs[String]("sessionId") shouldBe "acec2bcc-b7e9-4d1a-86b4-94c844c5bc2b"
            fst.getAs[String]("experienceId") shouldBe "acec2bcc-b7e9-4d1a-86b4-94c844c5bc2b"
            fst.getAs[String]("schoolId") shouldBe "acec2bcc-b7e9-4d1a-86b4-94c844c5bc2b"
            fst.getAs[String]("gradeId") shouldBe "acec2bcc-b7e9-4d1a-86b4-94c844c5bc2b"
            fst.getAs[String]("classId") shouldBe "acec2bcc-b7e9-4d1a-86b4-94c844c5bc2b"
            fst.getAs[String]("materialType") shouldBe "CORE"
            fst.getAs[String]("bookmark") shouldBe null
            fst.getAs[String]("highlight") shouldBe null
            fst.getAs[String]("academicYearTag") shouldBe "2024"
            val ebookMetaData = fst.getAs[Row]("ebookMetaData")
            ebookMetaData.getAs[String]("title") shouldBe "Test book"
            ebookMetaData.getAs[Int]("totalPages") shouldBe 100
            ebookMetaData.getAs[Boolean]("hasAudio") shouldBe false
            val progressData = fst.getAs[Row]("progressData")
            progressData.getAs[String]("location") shouldBe "epubcfi(/6/22!/4/2/12/3:244)"
            progressData.getAs[Boolean]("isLastPage") shouldBe false
            progressData.getAs[String]("status") shouldBe "IN_PROGRESS"
            progressData.getAs[Float]("timeSpent") shouldBe 10.2F
          }
        }
      )
    }
  }

  test("handle AdditionalResourcesAssigned event") {
    val sink = "learning-core-additional-resources-assigned-sink"
    val fixtures = List(
      SparkFixture(
        key = LearningProgressRawEvents.source,
        value = s"""
                   |{
                   |"key" : "key1",
                   |"timestamp": "2023-08-16 12:18:59.931",
                   |"value": {
                   |  "body": {
                   |
                   |	"id": "37b93a60-6f60-41c1-9c5c-35ed91356e03",
                   |	"learnerId": "85a523be-080c-40ae-97a4-3134b0ae532b",
                   |	"courseId": "dc634dc2-cca6-4947-b91b-b8e7ee4a0388",
                   |	"classId": "ab634dc2-cca6-4947-b91b-b8e7ee4a03dc",
                   |	"courseType": "CORE",
                   |	"academicYear": "2023-2024",
                   |	"activities": [
                   |		"3235ffc3-ca0f-4907-963e-000000104084",
                   |		"dab868a3-001d-467e-8347-000000104073"
                   |	],
                   |	"dueDate": {
                   |		"startDate": "2024-09-23",
                   |		"endDate": null
                   |	},
                   |	"resourceInfo": [
                   |        {
                   |          "activityId": "3235ffc3-ca0f-4907-963e-000000104084",
                   |          "activityType": "ACTIVITY",
                   |          "progressStatus": "COMPLETED"
                   |        }
                   |  ],
                   |	"assignedBy": "51dd752f-c008-459f-a097-bac731a877f5",
                   |	"assignedOn": "2024-09-23T11:40:56.877871251",
                   |	"uuid": "dc254c51-b594-4f8d-9a2e-2a78af119920",
                   |  "occurredOn": "2024-07-25T13:09:26.727"
                   |  },
                   |  "headers": {
                   |    "eventType": "CoreAdditionalResourceAssignedEvent",
                   |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                   |  }
                   |}
                   |}
                   |""".stripMargin
      )
    )

    new Setup {
      withSparkFixtures(
        fixtures,
        sinks => {
          val df = sinks
            .find(_.name == sink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$sink is not found"))

          val fst = df.first()
          fst.getAs[String]("learnerId") shouldBe "85a523be-080c-40ae-97a4-3134b0ae532b"
          fst.getAs[String]("courseId") shouldBe "dc634dc2-cca6-4947-b91b-b8e7ee4a0388"
          fst.getAs[String]("uuid") shouldBe "dc254c51-b594-4f8d-9a2e-2a78af119920"
        }
      )
    }
  }

  def assertCommonCols(row: Row, eventType: String): Unit = {
    (expCommonMapCols + ("eventType" -> eventType)).foreach {
      case (name, value) => row.getAs[String](name) shouldBe value
    }
  }
}
