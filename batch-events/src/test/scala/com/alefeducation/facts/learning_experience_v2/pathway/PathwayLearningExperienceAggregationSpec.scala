package com.alefeducation.facts.learning_experience_v2.pathway

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.learning_experience_v2.pathway.LearningSessionTestUtils.expExperienceCols
import com.alefeducation.facts.learning_experience_v2.transform.pathway.PathwayLearningExperienceAggregation
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper

class PathwayLearningExperienceAggregationSpec extends SparkSuite {

  val sprk: SparkSession = spark

  test("transform experience events successfully") {
    val LearningExperienceFinishedEvent = """
          |{
          |  "_app_tenant": "tenant-id",
          |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
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
          |  "bonusStars": 3,
          |  "bonusStarsScheme": "BONUS_2X"
          |}
      """.stripMargin

    val ExperienceDiscardedEvent = """
          |{
          |  "_app_tenant": "tenant-id",
          |  "eventType": "ExperienceDiscarded",
          |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
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
          |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
          |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val ExperienceFinishedTransformer = new PathwayLearningExperienceAggregation("pathway-learning-experience-transform")

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())
    val ExperienceDiscardedDF = spark.read.json(Seq(ExperienceDiscardedEvent).toDS())
    val input = Map(
      "pathway_experience_start_finish_events" -> Some(ExperienceFinishedDF),
      "pathway_experience_discarded_event" -> Some(ExperienceDiscardedDF)
    )

    val transformedExperienceFinishedDF = ExperienceFinishedTransformer.transform(input, 0L).get

    assert(transformedExperienceFinishedDF.columns.toSet === expExperienceCols)
    val row = transformedExperienceFinishedDF.head()
    assertRow[String](row, "trimester_order", "1")
    assertRow[String](row, "curriculum_id", "curr1")
    assertRow[String](row, "curriculum_subject_id", "curr_subject1")
    assertRow[String](row, "curriculum_grade_id", "curr_grade1")
    assertRow[Boolean](row, "retry", false)
    assertRow[String](row, "academic_year_id", "e1eff0a1-9469-4581-a4c3-12dbe777c984")
    assertRow[String](row, "content_academic_year", "2019")
    assertRow[String](row, "time_spent", "10")
    assertRow[String](row, "instructional_plan_id", "instructional-plan-id")
    assertRow[String](row, "lesson_category", "INSTRUCTIONAL_LESSON")
    assertRow[String](row, "level", "B2")
    assertRow[String](row, "date_dw_id", "20180908")
    assertRow[Double](row, "total_score", 20.10)
    assertRow[Int](row, "total_stars", 5)
    assertRow[Boolean](row, "activity_completed", true)
    assertRow[String](row, "material_type", "INSTRUCTIONAL_PLAN")
    assertRow[String](row, "material_id", "71d76a2c-8f62-4e91-b342-46150de25218")
    assertRow[String](row, "source", "WEB")
    assertRow[String](row, "academic_year", "2024")
    assertRow[Int](row, "bonus_stars", 3)
    assertRow[String](row, "bonus_stars_scheme", "BONUS_2X")
    assertRow[String](row, "teaching_period_id", null)
    assertRow[String](row, "assessment_id", "7823766")
  }

  test("transform experience events when score and stars are null") {

    val LearningExperienceFinishedEvent = """
          |{
          |  "_app_tenant": "tenant-id",
          |  "eventType": "ExperienceFinished",
          |   "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
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
          |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
          |}
        """.stripMargin

    val ExperienceDiscardedEvent =
      """
        |{
        |  "_app_tenant": "tenant-id",
        |  "eventType": "ExperienceDiscarded",
        |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
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
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
        |}
    """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val ExperienceFinishedTransformer = new PathwayLearningExperienceAggregation("pathway-learning-experience-transform")

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())
    val ExperienceDiscardedDF = spark.read.json(Seq(ExperienceDiscardedEvent).toDS())
    val input = Map(
      "pathway_experience_start_finish_events" -> Some(ExperienceFinishedDF),
      "pathway_experience_discarded_event" -> Some(ExperienceDiscardedDF)
    )

    val transformedExperienceFinishedDF = ExperienceFinishedTransformer.transform(input, 0L).get

    assert(transformedExperienceFinishedDF.columns.toSet === expExperienceCols)
    val row = transformedExperienceFinishedDF.head()
    assertRow[Int](row, "total_time", 60)
    assertRow[Boolean](row, "retry", true)
    assertRow[String](row, "score", "-1")
    assertRow[String](row, "stars", "-1")
    assertRow[String](row, "academic_year_id", null)
    assertRow[String](row, "content_academic_year", null)
    assertRow[String](row, "time_spent", "15")
    assertRow[Double](row, "total_score", 20.10)
    assertRow[Int](row, "total_stars", 5)
    assertRow[String](row, "instructional_plan_id", "instructional-plan-id")
    assertRow[String](row, "lesson_category", null)
    assertRow[String](row, "level", null)
    assertRow[String](row, "material_type", "INSTRUCTIONAL_PLAN")
    assertRow[String](row, "material_id", "71d76a2c-8f62-4e91-b342-46150de25218")
    assertRow[String](row, "academic_year", "2024")
    assertRow[String](row, "teaching_period_id", "period_id1")
    assertRow[String](row, "assessment_id", "7823766")
  }

  test("transform experience events when ScoreBreakdown is an empty array") {
    val LearningExperienceFinishedEvent =
      """
        |{
        |  "_app_tenant": "tenant-id",
        |  "eventType": "ExperienceFinished",
        |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
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
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
        |}
    """.stripMargin

    val ExperienceDiscardedEvent =
      """
        |{
        |  "_app_tenant": "tenant-id",
        |  "eventType": "ExperienceDiscarded",
        |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
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
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
        |}
    """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val ExperienceFinishedTransformer = new PathwayLearningExperienceAggregation("pathway-learning-experience-transform")

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())
    val ExperienceDiscardedDF = spark.read.json(Seq(ExperienceDiscardedEvent).toDS())
    val input = Map(
      "pathway_experience_start_finish_events" -> Some(ExperienceFinishedDF),
      "pathway_experience_discarded_event" -> Some(ExperienceDiscardedDF)
    )

    val transformedExperienceFinishedDF = ExperienceFinishedTransformer.transform(input, 0L).get

    transformedExperienceFinishedDF.count() mustBe 2

  }

  test("transform experience events with null material columns") {
    val LearningExperienceFinishedEvent =
      """
        |{
        |  "_app_tenant": "tenant-id",
        |  "eventType": "ExperienceFinished",
        |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
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
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
        |}
      """.stripMargin

    val ExperienceDiscardedEvent =
      """
        |{
        |  "_app_tenant": "tenant-id",
        |  "eventType": "ExperienceDiscarded",
        |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
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
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
        |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val ExperienceFinishedTransformer = new PathwayLearningExperienceAggregation("pathway-learning-experience-transform")

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())
    val ExperienceDiscardedDF = spark.read.json(Seq(ExperienceDiscardedEvent).toDS())

    val input = Map(
      "pathway_experience_start_finish_events" -> Some(ExperienceFinishedDF),
      "pathway_experience_discarded_event" -> Some(ExperienceDiscardedDF)
    )

    val transformedExperienceFinishedDF = ExperienceFinishedTransformer.transform(input, 0L).get
    val row = transformedExperienceFinishedDF.head()
    assert(transformedExperienceFinishedDF.columns.toSet === expExperienceCols)
    assertRow[String](row, "trimester_order", "1")
    assertRow[String](row, "curriculum_id", "curr1")
    assertRow[String](row, "curriculum_subject_id", "curr_subject1")
    assertRow[String](row, "curriculum_grade_id", "curr_grade1")
    assertRow[Boolean](row, "retry", true)
    assertRow[String](row, "academic_year_id", null)
    assertRow[String](row, "content_academic_year", null)
    assertRow[String](row, "time_spent", "15")
    assertRow[String](row, "material_type", "INSTRUCTIONAL_PLAN")
    assertRow[String](row, "material_id", "instructional-plan-id")
    assertRow[String](row, "academic_year", "2024")
    assertRow[String](row, "teaching_period_id", "period_id1")
    assertRow[String](row, "assessment_id", "7823766")
  }

  test("transform experience events without material columns") {
    val LearningExperienceFinishedEvent =
      """
        |{
        |  "_app_tenant": "tenant-id",
        |  "eventType": "ExperienceFinished",
        |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
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
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
        |}
  """.stripMargin

    val ExperienceDiscardedEvent =
      """
        |{
        |  "_app_tenant": "tenant-id",
        |  "eventType": "ExperienceDiscarded",
        |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
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
        |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
        |}
  """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val ExperienceFinishedTransformer = new PathwayLearningExperienceAggregation("pathway-learning-experience-transform")

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())
    val ExperienceDiscardedDF = spark.read.json(Seq(ExperienceDiscardedEvent).toDS())
    val input = Map(
      "pathway_experience_start_finish_events" -> Some(ExperienceFinishedDF),
      "pathway_experience_discarded_event" -> Some(ExperienceDiscardedDF)
    )
    val transformedExperienceFinishedDF = ExperienceFinishedTransformer.transform(input, 0L).get
    val row = transformedExperienceFinishedDF.head()
    assert(transformedExperienceFinishedDF.columns.toSet === expExperienceCols)
    assertRow[String](row, "trimester_order", "1")
    assertRow[String](row, "curriculum_id", "curr1")
    assertRow[String](row, "curriculum_subject_id", "curr_subject1")
    assertRow[String](row, "curriculum_grade_id", "curr_grade1")
    assertRow[Boolean](row, "retry", true)
    assertRow[String](row, "academic_year_id", null)
    assertRow[String](row, "content_academic_year", null)
    assertRow[String](row, "time_spent", "15") //
    assertRow[Int](row, "total_stars", 5)
    assertRow[String](row, "material_type", "INSTRUCTIONAL_PLAN")
    assertRow[String](row, "material_id", "instructional-plan-id")
    assertRow[String](row, "academic_year", "2024")
    assertRow[String](row, "teaching_period_id", "period_id1")
    assertRow[String](row, "assessment_id", "7823766")
  }

}
