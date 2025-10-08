package com.alefeducation.facts.learning_experience

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.learning_experience.LearningExperienceUtils.expExperienceCols
import com.alefeducation.facts.learning_experience.transform.LearningExperienceAggregation
import com.alefeducation.facts.learning_experience.transform.LearningExperienceAggregation.{LearningExperienceDiscardedSource, LearningExperienceFinishedSource}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper
import org.scalatestplus.mockito.MockitoSugar.mock

class LearningExperienceAggregationSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  test("transform experience events successfully") {
      val LearningExperienceFinishedEvent = """
          |{
          |  "tenantId": "tenant-id",
          |  "eventType": "ExperienceFinished",
          |  "occurredOn": "2018-09-08 02:40:00.0",
          |  "startTime": "2018-09-08 02:39:00.0",
          |  "attempt": 1,
          |  "activityComponentType": "ASSESSMENT",
          |  "exitTicket": true,
          |  "mainComponent": true,
          |  "completionNode": true,
          |  "activityType": "INSTRUCTIONAL_LESSON",
          |  "abbreviation": "TEQ_1",
          |  "activityComponentResources": [
          |  {
          |  "activityComponentType": "ASSESSMENT",
          |  "exitTicket": true
          |  }
          |  ],
          |  "schoolId": "school-id",
          |  "experienceId": "experience-id",
          |  "learningSessionId": "learning-session-id",
          |  "studentId": "student-id",
          |  "studentGrade": "8",
          |  "studentGradeId": "school-grade-id",
          |  "studentSection": "class-id",
          |  "classId": "424950eb-e692-476d-a9d1-e4db11dd853b",
          |  "subjectId": "school-subject-id",
          |  "subjectCode": "SCIENCE",
          |  "learningObjectiveId": "learning-objective-id",
          |  "contentPackageId": "content-package-id",
          |  "contentId": "content-id",
          |  "contentTitle": "My Exit Ticket",
          |  "lessonType": "SA",
          |  "flag": "TURQUOISE",
          |  "redo": false,
          |  "contentPackageItem": {
          |    "uuid": "content-package-id",
          |    "contentUuid": "content-id",
          |    "title": "My Exit Ticket",
          |    "lessonType": "SA"
          |  },
          |  "learningPathId" : "learning-path-id",
          |  "trimesterOrder" : "1",
          |  "instructionalPlanId": "instructional-plan-id",
          |  "curriculumId" : "curr1",
          |  "curriculumGradeId" : "curr_grade1",
          |  "curriculumSubjectId" : "curr_subject1",
          |  "score": 33,
          |  "scoreBreakDown": [{
          |		"id": "28298c68-ed6a-418a-9441-45c90911c45e",
          |		"code": "MA8_MLO_024_Q_36_FIN_IMPORT",
          |		"timeSpent": "4.944",
          |		"hintsUsed": "true",
          |		"type": "MULTIPLE_CHOICE",
          |		"version": "3",
          |		"maxScore": "1",
          |		"score": "0",
          |		"isAttended": "true",
          |		"attempts": [{
          |			"score": "0",
          |			"hintsUsed": "false",
          |			"isAttended": null,
          |			"maxScore": null,
          |			"suid": null,
          |			"timestamp": "2020-05-31T04:57:01.669",
          |			"timeSpent": "2.969"
          |		}, {
          |			"score": "1",
          |			"hintsUsed": "true",
          |			"isAttended": null,
          |			"maxScore": null,
          |			"suid": null,
          |			"timestamp": "2020-05-31T04:57:04.107",
          |			"timeSpent": "1.975"
          |		}]
          |	  },
          |	  {
          |		"id": "b5b8f4c2-e8b1-4435-aca5-50fb5b69198d",
          |		"code": "MA8_MLO_024_Q_11_IMPORT",
          |		"timeSpent": "4.66",
          |		"hintsUsed": "true",
          |		"type": "MULTIPLE_CHOICE",
          |		"version": "3",
          |		"maxScore": "1",
          |		"score": "0",
          |		"isAttended": "true",
          |		"attempts": [{
          |			"score": "0",
          |			"hintsUsed": "false",
          |			"isAttended": null,
          |			"maxScore": null,
          |			"suid": null,
          |			"timestamp": "2020-05-31T04:57:08.059",
          |			"timeSpent": "2.714"
          |		  }, {
          |			"score": "1",
          |			"hintsUsed": "true",
          |			"isAttended": null,
          |			"maxScore": null,
          |			"suid": null,
          |			"timestamp": "2020-05-31T04:57:10.265",
          |			"timeSpent": "1.946"
          |		}]
          |	  }
          |  ],
          |  "scorePresent": true,
          |  "outsideOfSchool": false,
          |  "testId": "111178",
          |  "suid": "7823766",
          |  "eventDateDw": "20181029",
          |  "stars": null,
          |  "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
          |  "contentAcademicYear": "2019",
          |  "timeSpent": 10,
          |  "lessonCategory":"INSTRUCTIONAL_LESSON",
          |  "level":"B2",
          |  "totalScore":20.1,
          |  "totalStars":5,
          |  "activityCompleted": true,
          |  "materialType":"INSTRUCTIONAL_PLAN",
          |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
          |  "source": "WEB",
          |  "academicYear": "2024",
          |  "teachingPeriodId": null,
          |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68",
          |  "isInClassGameExperience": false,
          |  "isAdditionalResource":true,
          |  "bonusStars": 3,
          |  "bonusStarsScheme": "BONUS_2X"
          |}
      """.stripMargin

      val ExperienceDiscardedEvent = """
          |{
          |  "tenantId": "tenant-id",
          |  "eventType": "ExperienceDiscarded",
          |  "occurredOn": "2018-09-08 02:40:00.0",
          |  "startTime": "2018-09-08 02:39:00.0",
          |  "attempt": 1,
          |  "activityComponentType": "ASSESSMENT",
          |  "exitTicket": true,
          |  "mainComponent": true,
          |  "completionNode": true,
          |  "activityType": "INSTRUCTIONAL_LESSON",
          |  "abbreviation": "TEQ_1",
          |  "activityComponentResources": [
          |  {
          |  "activityComponentType": "ASSESSMENT",
          |  "exitTicket": true
          |  }
          |  ],
          |  "schoolId": "school-id",
          |  "experienceId": "experience-id",
          |  "learningSessionId": "learning-session-id",
          |  "studentId": "student-id",
          |  "studentGrade": "8",
          |  "studentGradeId": "school-grade-id",
          |  "studentSection": "class-id",
          |  "classId": "424950eb-e692-476d-a9d1-e4db11dd853b",
          |  "subjectId": "school-subject-id",
          |  "subjectCode": "SCIENCE",
          |  "learningObjectiveId": "learning-objective-id",
          |  "contentPackageId": "content-package-id",
          |  "contentId": "content-id",
          |  "contentTitle": "My Exit Ticket",
          |  "lessonType": "SA",
          |  "flag": "TURQUOISE",
          |  "redo": false,
          |  "contentPackageItem": {
          |    "uuid": "content-package-id",
          |    "contentUuid": "content-id",
          |    "title": "My Exit Ticket",
          |    "lessonType": "SA"
          |  },
          |  "learningPathId" : "learning-path-id",
          |  "trimesterOrder" : "1",
          |  "instructionalPlanId": "instructional-plan-id",
          |  "curriculumId" : "curr1",
          |  "curriculumGradeId" : "curr_grade1",
          |  "curriculumSubjectId" : "curr_subject1",
          |  "score": 33,
          |  "scoreBreakDown": [{
          |		"id": "28298c68-ed6a-418a-9441-45c90911c45e",
          |		"code": "MA8_MLO_024_Q_36_FIN_IMPORT",
          |		"timeSpent": "4.944",
          |		"hintsUsed": "true",
          |		"type": "MULTIPLE_CHOICE",
          |		"version": "3",
          |		"maxScore": "1",
          |		"score": "0",
          |		"isAttended": "true",
          |		"attempts": [{
          |			"score": "0",
          |			"hintsUsed": "false",
          |			"isAttended": null,
          |			"maxScore": null,
          |			"suid": null,
          |			"timestamp": "2020-05-31T04:57:01.669",
          |			"timeSpent": "2.969"
          |		}, {
          |			"score": "1",
          |			"hintsUsed": "true",
          |			"isAttended": null,
          |			"maxScore": null,
          |			"suid": null,
          |			"timestamp": "2020-05-31T04:57:04.107",
          |			"timeSpent": "1.975"
          |		}]
          |	  },
          |	  {
          |		"id": "b5b8f4c2-e8b1-4435-aca5-50fb5b69198d",
          |		"code": "MA8_MLO_024_Q_11_IMPORT",
          |		"timeSpent": "4.66",
          |		"hintsUsed": "true",
          |		"type": "MULTIPLE_CHOICE",
          |		"version": "3",
          |		"maxScore": "1",
          |		"score": "0",
          |		"isAttended": "true",
          |		"attempts": [{
          |			"score": "0",
          |			"hintsUsed": "false",
          |			"isAttended": null,
          |			"maxScore": null,
          |			"suid": null,
          |			"timestamp": "2020-05-31T04:57:08.059",
          |			"timeSpent": "2.714"
          |		  }, {
          |			"score": "1",
          |			"hintsUsed": "true",
          |			"isAttended": null,
          |			"maxScore": null,
          |			"suid": null,
          |			"timestamp": "2020-05-31T04:57:10.265",
          |			"timeSpent": "1.946"
          |		}]
          |	  }
          |  ],
          |  "scorePresent": true,
          |  "outsideOfSchool": false,
          |  "testId": "111178",
          |  "suid": "7823766",
          |  "eventDateDw": "20181029",
          |  "stars": null,
          |  "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
          |  "contentAcademicYear": "2019",
          |  "timeSpent": null,
          |  "lessonCategory":"INSTRUCTIONAL_LESSON",
          |  "level":"B2",
          |  "totalScore":20.1,
          |  "activityCompleted": null,
          |  "materialType":"INSTRUCTIONAL_PLAN",
          |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
          |  "academicYear": "2024",
          |  "teachingPeriodId": "228",
          |  "isInClassGameExperience": false,
          |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
          |}
      """.stripMargin


    val sprk = spark
    import sprk.implicits._

    val ExperienceFinishedTransformer = new LearningExperienceAggregation(sprk, service)

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())
    val ExperienceDiscardedDF = spark.read.json(Seq(ExperienceDiscardedEvent).toDS())

    when(service.readOptional(LearningExperienceFinishedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ExperienceFinishedDF))
    when(service.readOptional(LearningExperienceDiscardedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ExperienceDiscardedDF))

    val transformedExperienceFinishedDF = ExperienceFinishedTransformer.transform().get.output

    assert(transformedExperienceFinishedDF.columns.toSet === expExperienceCols)
    assert[String](transformedExperienceFinishedDF, "fle_academic_period_order", "1")
    assert[String](transformedExperienceFinishedDF, "curr_uuid", "curr1")
    assert[String](transformedExperienceFinishedDF, "curr_subject_uuid", "curr_subject1")
    assert[String](transformedExperienceFinishedDF, "curr_grade_uuid", "curr_grade1")
    assert[Boolean](transformedExperienceFinishedDF, "fle_is_retry", false)
    assert[String](transformedExperienceFinishedDF, "academic_year_uuid", "e1eff0a1-9469-4581-a4c3-12dbe777c984")
    assert[String](transformedExperienceFinishedDF, "fle_content_academic_year", "2019")
    assert[Int](transformedExperienceFinishedDF, "fle_time_spent_app", 10)
    assert[String](transformedExperienceFinishedDF, "fle_instructional_plan_id", "instructional-plan-id")
    assert[String](transformedExperienceFinishedDF, "fle_lesson_category", "INSTRUCTIONAL_LESSON")
    assert[String](transformedExperienceFinishedDF, "fle_adt_level", "B2")
    assert[String](transformedExperienceFinishedDF, "fle_date_dw_id", "20180908")
    assert[Double](transformedExperienceFinishedDF, "fle_total_score", 20.10)
    assert[Int](transformedExperienceFinishedDF, "fle_total_stars", 5)
    assert[Boolean](transformedExperienceFinishedDF, "fle_is_activity_completed", true)
    assert[String](transformedExperienceFinishedDF, "fle_material_type", "INSTRUCTIONAL_PLAN")
    assert[String](transformedExperienceFinishedDF, "fle_material_id", "71d76a2c-8f62-4e91-b342-46150de25218")
    assert[String](transformedExperienceFinishedDF, "fle_source", "WEB")
    assert[String](transformedExperienceFinishedDF, "fle_academic_year", "2024")
    assert[Int](transformedExperienceFinishedDF, "fle_bonus_stars", 3)
    assert[String](transformedExperienceFinishedDF, "fle_bonus_stars_scheme", "BONUS_2X")
    assert[String](transformedExperienceFinishedDF, "fle_teaching_period_id", null)
    assert[String](transformedExperienceFinishedDF, "fle_assessment_id", "7823766")
    assert[Boolean](transformedExperienceFinishedDF, "fle_is_gamified", false)
  }

  test("transform experience events when score and stars are null") {

      val LearningExperienceFinishedEvent = """
          |{
          |  "tenantId": "tenant-id",
          |  "eventType": "ExperienceFinished",
          |  "occurredOn": "2018-09-08 02:40:00.0",
          |  "startTime": "2018-09-08 02:39:00.0",
          |  "attempt": 2,
          |  "activityComponentType": "ASSESSMENT",
          |  "exitTicket": true,
          |  "mainComponent": true,
          |  "completionNode": true,
          |  "activityType": "INSTRUCTIONAL_LESSON",
          |  "abbreviation": "TEQ_1",
          |  "activityComponentResources": [
          |  {
          |  "activityComponentType": "ASSESSMENT",
          |  "exitTicket": true
          |  }
          |  ],
          |  "schoolId": "school-id",
          |  "experienceId": "experience-id",
          |  "learningSessionId": "learning-session-id",
          |  "studentId": "student-id",
          |  "studentGrade": "8",
          |  "studentGradeId": "school-grade-id",
          |  "studentSection": "class-id",
          |  "classId": "424950eb-e692-476d-a9d1-e4db11dd853b",
          |  "subjectId": "school-subject-id",
          |  "subjectCode": "SCIENCE",
          |  "learningObjectiveId": "learning-objective-id",
          |  "contentPackageId": "content-package-id",
          |  "contentId": "content-id",
          |  "contentTitle": "My Exit Ticket",
          |  "lessonType": "SA",
          |  "flag": "TURQUOISE",
          |  "redo": false,
          |  "contentPackageItem": {
          |    "uuid": "content-package-id",
          |    "contentUuid": "content-id",
          |    "title": "My Exit Ticket",
          |    "lessonType": "SA"
          |  },
          |  "learningPathId" : "learning-path-id",
          |  "trimesterOrder" : "1",
          |  "curriculumId" : "curr1",
          |  "curriculumGradeId" : "curr_grade1",
          |  "curriculumSubjectId" : "curr_subject1",
          |  "score": null,
          |  "scoreBreakDown": [{
          |		"id": "28298c68-ed6a-418a-9441-45c90911c45e",
          |		"code": "MA8_MLO_024_Q_36_FIN_IMPORT",
          |		"timeSpent": "4.944",
          |		"hintsUsed": "true",
          |		"type": "MULTIPLE_CHOICE",
          |		"version": "3",
          |		"maxScore": "1",
          |		"score": "0",
          |		"isAttended": "true",
          |		"attempts": [{
          |			"score": "0",
          |			"hintsUsed": "false",
          |			"isAttended": null,
          |			"maxScore": null,
          |			"suid": null,
          |			"timestamp": "2020-05-31T04:57:01.669",
          |			"timeSpent": "2.969"
          |		}, {
          |			"score": "1",
          |			"hintsUsed": "true",
          |			"isAttended": null,
          |			"maxScore": null,
          |			"suid": null,
          |			"timestamp": "2020-05-31T04:57:04.107",
          |			"timeSpent": "1.975"
          |		}]
          |	},
          |	{
          |		"id": "b5b8f4c2-e8b1-4435-aca5-50fb5b69198d",
          |		"code": "MA8_MLO_024_Q_11_IMPORT",
          |		"timeSpent": "4.66",
          |		"hintsUsed": "true",
          |		"type": "MULTIPLE_CHOICE",
          |		"version": "3",
          |		"maxScore": "1",
          |		"score": "0",
          |		"isAttended": "true",
          |		"attempts": [{
          |			"score": "0",
          |			"hintsUsed": "false",
          |			"isAttended": null,
          |			"maxScore": null,
          |			"suid": null,
          |			"timestamp": "2020-05-31T04:57:08.059",
          |			"timeSpent": "2.714"
          |		}, {
          |			"score": "1",
          |			"hintsUsed": "true",
          |			"isAttended": null,
          |			"maxScore": null,
          |			"suid": null,
          |			"timestamp": "2020-05-31T04:57:10.265",
          |			"timeSpent": "1.946"
          |		}]
          |	}
          |],
          |  "scorePresent": true,
          |  "outsideOfSchool": false,
          |  "instructionalPlanId": "instructional-plan-id",
          |  "testId": "111178",
          |  "suid": "7823766",
          |  "eventDateDw": "20181029",
          |  "stars": null,
          |  "academicYearId": null,
          |  "contentAcademicYear": null,
          |  "timeSpent": 15,
          |  "lessonCategory": null,
          |  "level": null,
          |  "totalScore":20.10,
          |  "totalStars":5,
          |  "activityCompleted": true,
          |  "materialType":"INSTRUCTIONAL_PLAN",
          |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
          |  "source": null,
          |  "academicYear": "2024",
          |  "teachingPeriodId": "period_id1",
          |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68",
          |  "isInClassGameExperience": false,
          |  "isAdditionalResource":true
          |}
        """.stripMargin

    val ExperienceDiscardedEvent =
      """
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ExperienceDiscarded",
        |  "occurredOn": "2018-09-08 02:40:00.0",
        |  "startTime": "2018-09-08 02:39:00.0",
        |  "attempt": 1,
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true,
        |  "mainComponent": true,
        |  "completionNode": true,
        |  "activityType": "INSTRUCTIONAL_LESSON",
        |  "abbreviation": "TEQ_1",
        |  "activityComponentResources": [
        |  {
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true
        |  }
        |  ],
        |  "schoolId": "school-id",
        |  "experienceId": "experience-id",
        |  "learningSessionId": "learning-session-id",
        |  "studentId": "student-id",
        |  "studentGrade": "8",
        |  "studentGradeId": "school-grade-id",
        |  "studentSection": "class-id",
        |  "classId": "424950eb-e692-476d-a9d1-e4db11dd853b",
        |  "subjectId": "school-subject-id",
        |  "subjectCode": "SCIENCE",
        |  "learningObjectiveId": "learning-objective-id",
        |  "contentPackageId": "content-package-id",
        |  "contentId": "content-id",
        |  "contentTitle": "My Exit Ticket",
        |  "lessonType": "SA",
        |  "flag": "TURQUOISE",
        |  "redo": false,
        |  "contentPackageItem": {
        |    "uuid": "content-package-id",
        |    "contentUuid": "content-id",
        |    "title": "My Exit Ticket",
        |    "lessonType": "SA"
        |  },
        |  "learningPathId" : "learning-path-id",
        |  "trimesterOrder" : "1",
        |  "instructionalPlanId": "instructional-plan-id",
        |  "curriculumId" : "curr1",
        |  "curriculumGradeId" : "curr_grade1",
        |  "curriculumSubjectId" : "curr_subject1",
        |  "score": 33,
        |  "scoreBreakDown": [{
        |		"id": "28298c68-ed6a-418a-9441-45c90911c45e",
        |		"code": "MA8_MLO_024_Q_36_FIN_IMPORT",
        |		"timeSpent": "4.944",
        |		"hintsUsed": "true",
        |		"type": "MULTIPLE_CHOICE",
        |		"version": "3",
        |		"maxScore": "1",
        |		"score": "0",
        |		"isAttended": "true",
        |		"attempts": [{
        |			"score": "0",
        |			"hintsUsed": "false",
        |			"isAttended": null,
        |			"maxScore": null,
        |			"suid": null,
        |			"timestamp": "2020-05-31T04:57:01.669",
        |			"timeSpent": "2.969"
        |		}, {
        |			"score": "1",
        |			"hintsUsed": "true",
        |			"isAttended": null,
        |			"maxScore": null,
        |			"suid": null,
        |			"timestamp": "2020-05-31T04:57:04.107",
        |			"timeSpent": "1.975"
        |		}]
        |	  },
        |	  {
        |		"id": "b5b8f4c2-e8b1-4435-aca5-50fb5b69198d",
        |		"code": "MA8_MLO_024_Q_11_IMPORT",
        |		"timeSpent": "4.66",
        |		"hintsUsed": "true",
        |		"type": "MULTIPLE_CHOICE",
        |		"version": "3",
        |		"maxScore": "1",
        |		"score": "0",
        |		"isAttended": "true",
        |		"attempts": [{
        |			"score": "0",
        |			"hintsUsed": "false",
        |			"isAttended": null,
        |			"maxScore": null,
        |			"suid": null,
        |			"timestamp": "2020-05-31T04:57:08.059",
        |			"timeSpent": "2.714"
        |		  }, {
        |			"score": "1",
        |			"hintsUsed": "true",
        |			"isAttended": null,
        |			"maxScore": null,
        |			"suid": null,
        |			"timestamp": "2020-05-31T04:57:10.265",
        |			"timeSpent": "1.946"
        |		}]
        |	  }
        |  ],
        |  "scorePresent": true,
        |  "outsideOfSchool": false,
        |  "testId": "111178",
        |  "suid": "7823766",
        |  "eventDateDw": "20181029",
        |  "stars": null,
        |  "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
        |  "contentAcademicYear": "2019",
        |  "timeSpent": null,
        |  "lessonCategory":"INSTRUCTIONAL_LESSON",
        |  "level":"B2",
        |  "totalScore":20.1,
        |  "totalStars":5,
        |  "activityCompleted": null,
        |  "materialType":"INSTRUCTIONAL_PLAN",
        |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
        |  "source": null,
        |  "academicYear": "2024",
        |  "teachingPeriodId": "period_id1",
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68",
        |  "isInClassGameExperience": false
        |}
    """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val ExperienceFinishedTransformer = new LearningExperienceAggregation(sprk, service)

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())
    val ExperienceDiscardedDF = spark.read.json(Seq(ExperienceDiscardedEvent).toDS())

    when(service.readOptional(LearningExperienceFinishedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ExperienceFinishedDF))
    when(service.readOptional(LearningExperienceDiscardedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ExperienceDiscardedDF))

    val transformedExperienceFinishedDF = ExperienceFinishedTransformer.transform().get.output


    assert(transformedExperienceFinishedDF.columns.toSet === expExperienceCols)
    assert[Int](transformedExperienceFinishedDF, "fle_total_time", 60)
    assert[Boolean](transformedExperienceFinishedDF, "fle_is_retry", true)
    assert[String](transformedExperienceFinishedDF, "fle_score", "-1")
    assert[String](transformedExperienceFinishedDF, "fle_star_earned", "-1")
    assert[String](transformedExperienceFinishedDF, "academic_year_uuid", null)
    assert[String](transformedExperienceFinishedDF, "fle_content_academic_year", null)
    assert[Int](transformedExperienceFinishedDF, "fle_time_spent_app", 15)
    assert[Double](transformedExperienceFinishedDF, "fle_total_score", 20.10)
    assert[Int](transformedExperienceFinishedDF, "fle_total_stars", 5)
    assert[String](transformedExperienceFinishedDF, "fle_instructional_plan_id", "instructional-plan-id")
    assert[String](transformedExperienceFinishedDF, "fle_lesson_category", null)
    assert[String](transformedExperienceFinishedDF, "fle_adt_level", null)
    assert[String](transformedExperienceFinishedDF, "fle_material_type", "INSTRUCTIONAL_PLAN")
    assert[String](transformedExperienceFinishedDF, "fle_material_id", "71d76a2c-8f62-4e91-b342-46150de25218")
    assert[String](transformedExperienceFinishedDF, "fle_academic_year", "2024")
    assert[String](transformedExperienceFinishedDF, "fle_teaching_period_id", "period_id1")
    assert[String](transformedExperienceFinishedDF, "fle_assessment_id", "7823766")
  }

  test("transform experience events when ScoreBreakdown is an empty array") {
    val LearningExperienceFinishedEvent =
      """
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ExperienceFinished",
        |  "occurredOn": "2018-09-08 02:40:00.0",
        |  "startTime": "2018-09-08 02:39:00.0",
        |  "attempt": 2,
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true,
        |  "mainComponent": true,
        |  "completionNode": true,
        |  "activityType": "INSTRUCTIONAL_LESSON",
        |  "abbreviation": "TEQ_1",
        |  "activityComponentResources": [
        |  {
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true
        |  }
        |  ],
        |  "schoolId": "school-id",
        |  "experienceId": "experience-id",
        |  "learningSessionId": "learning-session-id",
        |  "studentId": "student-id",
        |  "studentGrade": "8",
        |  "studentGradeId": "school-grade-id",
        |  "studentSection": "class-id",
        |  "classId": "424950eb-e692-476d-a9d1-e4db11dd853b",
        |  "subjectId": "school-subject-id",
        |  "subjectCode": "SCIENCE",
        |  "learningObjectiveId": "learning-objective-id",
        |  "contentPackageId": "content-package-id",
        |  "contentId": "content-id",
        |  "contentTitle": "My Exit Ticket",
        |  "lessonType": "SA",
        |  "flag": "TURQUOISE",
        |  "redo": false,
        |  "contentPackageItem": {
        |    "uuid": "content-package-id",
        |    "contentUuid": "content-id",
        |    "title": "My Exit Ticket",
        |    "lessonType": "SA"
        |  },
        |  "learningPathId" : "learning-path-id",
        |  "trimesterOrder" : "1",
        |  "curriculumId" : "curr1",
        |  "curriculumGradeId" : "curr_grade1",
        |  "curriculumSubjectId" : "curr_subject1",
        |  "score": null,
        |  "scoreBreakDown": [],
        |  "scorePresent": true,
        |  "outsideOfSchool": false,
        |  "instructionalPlanId": "instructional-plan-id",
        |  "testId": "111178",
        |  "suid": "7823766",
        |  "eventDateDw": "20181029",
        |  "stars": null,
        |  "academicYearId": null,
        |  "contentAcademicYear": null,
        |  "timeSpent": 15,
        |  "lessonCategory": null,
        |  "level": null,
        |  "totalScore":20.10,
        |  "totalStars":5,
        |  "activityCompleted": true,
        |  "materialType":"INSTRUCTIONAL_PLAN",
        |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
        |  "source": "WEB",
        |  "academicYear": "2024",
        |  "teachingPeriodId": "period_id1",
        |  "replayed": false,
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68",
        |  "isInClassGameExperience": false,
        |  "isAdditionalResource":true
        |}
    """.stripMargin

    val ExperienceDiscardedEvent =
      """
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ExperienceDiscarded",
        |  "occurredOn": "2018-09-08 02:40:00.0",
        |  "startTime": "2018-09-08 02:39:00.0",
        |  "attempt": 1,
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true,
        |  "mainComponent": true,
        |  "completionNode": true,
        |  "activityType": "INSTRUCTIONAL_LESSON",
        |  "abbreviation": "TEQ_1",
        |  "activityComponentResources": [
        |  {
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true
        |  }
        |  ],
        |  "schoolId": "school-id",
        |  "experienceId": "experience-id",
        |  "learningSessionId": "learning-session-id",
        |  "studentId": "student-id",
        |  "studentGrade": "8",
        |  "studentGradeId": "school-grade-id",
        |  "studentSection": "class-id",
        |  "classId": "424950eb-e692-476d-a9d1-e4db11dd853b",
        |  "subjectId": "school-subject-id",
        |  "subjectCode": "SCIENCE",
        |  "learningObjectiveId": "learning-objective-id",
        |  "contentPackageId": "content-package-id",
        |  "contentId": "content-id",
        |  "contentTitle": "My Exit Ticket",
        |  "lessonType": "SA",
        |  "flag": "TURQUOISE",
        |  "redo": false,
        |  "contentPackageItem": {
        |    "uuid": "content-package-id",
        |    "contentUuid": "content-id",
        |    "title": "My Exit Ticket",
        |    "lessonType": "SA"
        |  },
        |  "learningPathId" : "learning-path-id",
        |  "trimesterOrder" : "1",
        |  "instructionalPlanId": "instructional-plan-id",
        |  "curriculumId" : "curr1",
        |  "curriculumGradeId" : "curr_grade1",
        |  "curriculumSubjectId" : "curr_subject1",
        |  "score": 33,
        |  "scoreBreakDown": [],
        |  "scorePresent": true,
        |  "outsideOfSchool": false,
        |  "testId": "111178",
        |  "suid": "7823766",
        |  "eventDateDw": "20181029",
        |  "stars": null,
        |  "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
        |  "contentAcademicYear": "2019",
        |  "timeSpent": null,
        |  "lessonCategory":"INSTRUCTIONAL_LESSON",
        |  "level":"B2",
        |  "totalScore":20.1,
        |  "activityCompleted": null,
        |  "materialType":"INSTRUCTIONAL_PLAN",
        |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
        |  "source": "WEB",
        |  "academicYear": "2024",
        |  "teachingPeriodId": "period_id1",
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68",
        |  "isInClassGameExperience": false
        |}
    """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val ExperienceFinishedTransformer = new LearningExperienceAggregation(sprk, service)

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())
    val ExperienceDiscardedDF = spark.read.json(Seq(ExperienceDiscardedEvent).toDS())

    when(service.readOptional(LearningExperienceFinishedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ExperienceFinishedDF))
    when(service.readOptional(LearningExperienceDiscardedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ExperienceDiscardedDF))

    val transformedExperienceFinishedDF = ExperienceFinishedTransformer.transform().get.output

    transformedExperienceFinishedDF.count() mustBe 2

  }

  test("transform experience events with null material columns") {
    val LearningExperienceFinishedEvent =
      """
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ExperienceFinished",
        |  "occurredOn": "2018-09-08 02:40:00.0",
        |  "startTime": "2018-09-08 02:39:00.0",
        |  "attempt": 2,
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true,
        |  "mainComponent": true,
        |  "completionNode": true,
        |  "activityType": "INSTRUCTIONAL_LESSON",
        |  "abbreviation": "TEQ_1",
        |  "activityComponentResources": [
        |  {
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true
        |  }
        |  ],
        |  "schoolId": "school-id",
        |  "experienceId": "experience-id",
        |  "learningSessionId": "learning-session-id",
        |  "studentId": "student-id",
        |  "studentGrade": "8",
        |  "studentGradeId": "school-grade-id",
        |  "studentSection": "class-id",
        |  "classId": "424950eb-e692-476d-a9d1-e4db11dd853b",
        |  "subjectId": "school-subject-id",
        |  "subjectCode": "SCIENCE",
        |  "learningObjectiveId": "learning-objective-id",
        |  "contentPackageId": "content-package-id",
        |  "contentId": "content-id",
        |  "contentTitle": "My Exit Ticket",
        |  "lessonType": "SA",
        |  "flag": "TURQUOISE",
        |  "redo": false,
        |  "contentPackageItem": {
        |    "uuid": "content-package-id",
        |    "contentUuid": "content-id",
        |    "title": "My Exit Ticket",
        |    "lessonType": "SA"
        |  },
        |  "learningPathId" : "learning-path-id",
        |  "trimesterOrder" : "1",
        |  "curriculumId" : "curr1",
        |  "curriculumGradeId" : "curr_grade1",
        |  "curriculumSubjectId" : "curr_subject1",
        |  "score": null,
        |  "scoreBreakDown": [],
        |  "scorePresent": true,
        |  "outsideOfSchool": false,
        |  "instructionalPlanId": "instructional-plan-id",
        |  "testId": "111178",
        |  "suid": "7823766",
        |  "eventDateDw": "20181029",
        |  "stars": null,
        |  "academicYearId": null,
        |  "contentAcademicYear": null,
        |  "timeSpent": 15,
        |  "lessonCategory": null,
        |  "level": null,
        |  "totalScore":20.10,
        |  "totalStars":5,
        |  "activityCompleted": true,
        |  "materialType":null,
        |  "materialId":null,
        |  "source": "APP",
        |  "academicYear": "2024",
        |  "teachingPeriodId": "period_id1",
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68",
        |  "isInClassGameExperience": false,
        |  "isAdditionalResource":true
        |}
      """.stripMargin

    val ExperienceDiscardedEvent =
      """
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ExperienceDiscarded",
        |  "occurredOn": "2018-09-08 02:40:00.0",
        |  "startTime": "2018-09-08 02:39:00.0",
        |  "attempt": 1,
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true,
        |  "mainComponent": true,
        |  "completionNode": true,
        |  "activityType": "INSTRUCTIONAL_LESSON",
        |  "abbreviation": "TEQ_1",
        |  "activityComponentResources": [
        |  {
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true
        |  }
        |  ],
        |  "schoolId": "school-id",
        |  "experienceId": "experience-id",
        |  "learningSessionId": "learning-session-id",
        |  "studentId": "student-id",
        |  "studentGrade": "8",
        |  "studentGradeId": "school-grade-id",
        |  "studentSection": "class-id",
        |  "classId": "424950eb-e692-476d-a9d1-e4db11dd853b",
        |  "subjectId": "school-subject-id",
        |  "subjectCode": "SCIENCE",
        |  "learningObjectiveId": "learning-objective-id",
        |  "contentPackageId": "content-package-id",
        |  "contentId": "content-id",
        |  "contentTitle": "My Exit Ticket",
        |  "lessonType": "SA",
        |  "flag": "TURQUOISE",
        |  "redo": false,
        |  "contentPackageItem": {
        |    "uuid": "content-package-id",
        |    "contentUuid": "content-id",
        |    "title": "My Exit Ticket",
        |    "lessonType": "SA"
        |  },
        |  "learningPathId" : "learning-path-id",
        |  "trimesterOrder" : "1",
        |  "instructionalPlanId": "instructional-plan-id",
        |  "curriculumId" : "curr1",
        |  "curriculumGradeId" : "curr_grade1",
        |  "curriculumSubjectId" : "curr_subject1",
        |  "score": 33,
        |  "scoreBreakDown": [],
        |  "scorePresent": true,
        |  "outsideOfSchool": false,
        |  "testId": "111178",
        |  "suid": "7823766",
        |  "eventDateDw": "20181029",
        |  "stars": null,
        |  "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
        |  "contentAcademicYear": "2019",
        |  "timeSpent": null,
        |  "lessonCategory":"INSTRUCTIONAL_LESSON",
        |  "level":"B2",
        |  "totalScore":20.1,
        |  "activityCompleted": null,
        |  "materialType":null,
        |  "materialId":null,
        |  "source": "APP",
        |  "academicYear": "2024",
        |  "teachingPeriodId": "period_id1",
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68",
        |  "isInClassGameExperience": false
        |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val ExperienceFinishedTransformer = new LearningExperienceAggregation(sprk, service)

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())
    val ExperienceDiscardedDF = spark.read.json(Seq(ExperienceDiscardedEvent).toDS())

    when(service.readOptional(LearningExperienceFinishedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ExperienceFinishedDF))
    when(service.readOptional(LearningExperienceDiscardedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ExperienceDiscardedDF))

    val transformedExperienceFinishedDF = ExperienceFinishedTransformer.transform().get.output

    assert(transformedExperienceFinishedDF.columns.toSet === expExperienceCols)
    assert[String](transformedExperienceFinishedDF, "fle_academic_period_order", "1")
    assert[String](transformedExperienceFinishedDF, "curr_uuid", "curr1")
    assert[String](transformedExperienceFinishedDF, "curr_subject_uuid", "curr_subject1")
    assert[String](transformedExperienceFinishedDF, "curr_grade_uuid", "curr_grade1")
    assert[Boolean](transformedExperienceFinishedDF, "fle_is_retry", true)
    assert[String](transformedExperienceFinishedDF, "academic_year_uuid", null)
    assert[String](transformedExperienceFinishedDF, "fle_content_academic_year", null)
    assert[Int](transformedExperienceFinishedDF, "fle_time_spent_app", 15)
    assert[String](transformedExperienceFinishedDF, "fle_material_type", "INSTRUCTIONAL_PLAN")
    assert[String](transformedExperienceFinishedDF, "fle_material_id", "instructional-plan-id")
    assert[String](transformedExperienceFinishedDF, "fle_academic_year", "2024")
    assert[String](transformedExperienceFinishedDF, "fle_teaching_period_id", "period_id1")
    assert[String](transformedExperienceFinishedDF, "fle_assessment_id", "7823766")
  }

  test("transform experience events without material columns") {
    val LearningExperienceFinishedEvent =
      """
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ExperienceFinished",
        |  "occurredOn": "2018-09-08 02:40:00.0",
        |  "startTime": "2018-09-08 02:39:00.0",
        |  "attempt": 2,
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true,
        |  "mainComponent": true,
        |  "completionNode": true,
        |  "activityType": "INSTRUCTIONAL_LESSON",
        |  "abbreviation": "TEQ_1",
        |  "activityComponentResources": [
        |  {
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true
        |  }
        |  ],
        |  "schoolId": "school-id",
        |  "experienceId": "experience-id",
        |  "learningSessionId": "learning-session-id",
        |  "studentId": "student-id",
        |  "studentGrade": "8",
        |  "studentGradeId": "school-grade-id",
        |  "studentSection": "class-id",
        |  "classId": "424950eb-e692-476d-a9d1-e4db11dd853b",
        |  "subjectId": "school-subject-id",
        |  "subjectCode": "SCIENCE",
        |  "learningObjectiveId": "learning-objective-id",
        |  "contentPackageId": "content-package-id",
        |  "contentId": "content-id",
        |  "contentTitle": "My Exit Ticket",
        |  "lessonType": "SA",
        |  "flag": "TURQUOISE",
        |  "redo": false,
        |  "contentPackageItem": {
        |    "uuid": "content-package-id",
        |    "contentUuid": "content-id",
        |    "title": "My Exit Ticket",
        |    "lessonType": "SA"
        |  },
        |  "learningPathId" : "learning-path-id",
        |  "trimesterOrder" : "1",
        |  "curriculumId" : "curr1",
        |  "curriculumGradeId" : "curr_grade1",
        |  "curriculumSubjectId" : "curr_subject1",
        |  "score": null,
        |  "scoreBreakDown": [],
        |  "scorePresent": true,
        |  "outsideOfSchool": false,
        |  "instructionalPlanId": "instructional-plan-id",
        |  "testId": "111178",
        |  "suid": "7823766",
        |  "eventDateDw": "20181029",
        |  "stars": null,
        |  "academicYearId": null,
        |  "contentAcademicYear": null,
        |  "timeSpent": 15,
        |  "lessonCategory": null,
        |  "level": null,
        |  "totalScore":20.10,
        |  "totalStars":5,
        |  "activityCompleted": true,
        |  "source": "APP",
        |  "academicYear": "2024",
        |  "teachingPeriodId": "period_id1",
        |  "replayed": false,
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68",
        |  "isInClassGameExperience": false,
        |  "isAdditionalResource":true
        |}
  """.stripMargin

    val ExperienceDiscardedEvent =
      """
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ExperienceDiscarded",
        |  "occurredOn": "2018-09-08 02:40:00.0",
        |  "startTime": "2018-09-08 02:39:00.0",
        |  "attempt": 1,
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true,
        |  "mainComponent": true,
        |  "completionNode": true,
        |  "activityType": "INSTRUCTIONAL_LESSON",
        |  "abbreviation": "TEQ_1",
        |  "activityComponentResources": [
        |  {
        |  "activityComponentType": "ASSESSMENT",
        |  "exitTicket": true
        |  }
        |  ],
        |  "schoolId": "school-id",
        |  "experienceId": "experience-id",
        |  "learningSessionId": "learning-session-id",
        |  "studentId": "student-id",
        |  "studentGrade": "8",
        |  "studentGradeId": "school-grade-id",
        |  "studentSection": "class-id",
        |  "classId": "424950eb-e692-476d-a9d1-e4db11dd853b",
        |  "subjectId": "school-subject-id",
        |  "subjectCode": "SCIENCE",
        |  "learningObjectiveId": "learning-objective-id",
        |  "contentPackageId": "content-package-id",
        |  "contentId": "content-id",
        |  "contentTitle": "My Exit Ticket",
        |  "lessonType": "SA",
        |  "flag": "TURQUOISE",
        |  "redo": false,
        |  "contentPackageItem": {
        |    "uuid": "content-package-id",
        |    "contentUuid": "content-id",
        |    "title": "My Exit Ticket",
        |    "lessonType": "SA"
        |  },
        |  "learningPathId" : "learning-path-id",
        |  "trimesterOrder" : "1",
        |  "instructionalPlanId": "instructional-plan-id",
        |  "curriculumId" : "curr1",
        |  "curriculumGradeId" : "curr_grade1",
        |  "curriculumSubjectId" : "curr_subject1",
        |  "score": 33,
        |  "scoreBreakDown": [],
        |  "scorePresent": true,
        |  "outsideOfSchool": false,
        |  "testId": "111178",
        |  "suid": "7823766",
        |  "eventDateDw": "20181029",
        |  "stars": null,
        |  "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
        |  "contentAcademicYear": "2019",
        |  "timeSpent": null,
        |  "lessonCategory":"INSTRUCTIONAL_LESSON",
        |  "level":"B2",
        |  "totalScore":20.1,
        |  "activityCompleted": null,
        |  "source": "APP",
        |  "academicYear": "2024",
        |  "teachingPeriodId": "period_id1",
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68",
        |  "isInClassGameExperience": false
        |}
  """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val ExperienceFinishedTransformer = new LearningExperienceAggregation(sprk, service)

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())
    val ExperienceDiscardedDF = spark.read.json(Seq(ExperienceDiscardedEvent).toDS())

    when(service.readOptional(LearningExperienceFinishedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ExperienceFinishedDF))
    when(service.readOptional(LearningExperienceDiscardedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ExperienceDiscardedDF))

    val transformedExperienceFinishedDF = ExperienceFinishedTransformer.transform().get.output

    assert(transformedExperienceFinishedDF.columns.toSet === expExperienceCols)
    assert[String](transformedExperienceFinishedDF, "fle_academic_period_order", "1")
    assert[String](transformedExperienceFinishedDF, "curr_uuid", "curr1")
    assert[String](transformedExperienceFinishedDF, "curr_subject_uuid", "curr_subject1")
    assert[String](transformedExperienceFinishedDF, "curr_grade_uuid", "curr_grade1")
    assert[Boolean](transformedExperienceFinishedDF, "fle_is_retry", true)
    assert[String](transformedExperienceFinishedDF, "academic_year_uuid", null)
    assert[String](transformedExperienceFinishedDF, "fle_content_academic_year", null)
    assert[Int](transformedExperienceFinishedDF, "fle_time_spent_app", 15)
    assert[Int](transformedExperienceFinishedDF, "fle_total_stars", 5)
    assert[String](transformedExperienceFinishedDF, "fle_material_type", "INSTRUCTIONAL_PLAN")
    assert[String](transformedExperienceFinishedDF, "fle_material_id", "instructional-plan-id")
    assert[String](transformedExperienceFinishedDF, "fle_academic_year", "2024")
    assert[String](transformedExperienceFinishedDF, "fle_teaching_period_id", "period_id1")
    assert[String](transformedExperienceFinishedDF, "fle_assessment_id", "7823766")
  }

}
