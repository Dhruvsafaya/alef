package com.alefeducation.facts.learning_experience

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.learning_experience.transform.LearningExperienceAggregation.LearningExperienceFinishedSource
import com.alefeducation.facts.learning_experience.transform.ScoreBreakdownAggregation
import com.alefeducation.util.DataFrameEqualityUtils.assertSmallDatasetEquality
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Timestamp

class ScoreBreakdownAggregationSpec extends SparkSuite {

  val sep2018Timestamp = Timestamp.valueOf("2018-09-08 02:40:00.0")
  val nov2021Timestamp = Timestamp.valueOf("2021-11-23 02:40:00.0")

  val expectedScoreBreakdownColumns = Seq("fle_scbd_created_time",
    "fle_scbd_dw_created_time",
    "fle_scbd_date_dw_id",
    "fle_scbd_fle_exp_id",
    "fle_scbd_fle_ls_id",
    "fle_scbd_question_id",
    "fle_scbd_code",
    "fle_scbd_time_spent",
    "fle_scbd_hints_used",
    "fle_scbd_max_score",
    "fle_scbd_score",
    "fle_scbd_lo_id",
    "fle_scbd_type",
    "fle_scbd_version",
    "fle_scbd_is_attended",
    "eventdate"
  )

  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  test("empty scoreBreakdown") {
    val LearningExperienceFinishedEvent =
      """
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
        |  "scoreBreakDown": [],
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
        |  "activityCompleted": true,
        |  "materialType":"INSTRUCTIONAL_PLAN",
        |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
        |  "replayed": false
        |}
  """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val ScoreBreakdownTransformer = new ScoreBreakdownAggregation(sprk, service)

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())

    when(service.readOptional(LearningExperienceFinishedSource, sprk)).thenReturn(Some(ExperienceFinishedDF))

    val scoreBreakdownDF = ScoreBreakdownTransformer.transform().get.output
    assert(scoreBreakdownDF.count() == 0)
  }

  test("null scoreBreakdown") {
    val LearningExperienceFinishedEvent =
      """
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
        |  "scoreBreakDown": null,
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
        |  "activityCompleted": true,
        |  "materialType":"INSTRUCTIONAL_PLAN",
        |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218"
        |}
    """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val ScoreBreakdownTransformer = new ScoreBreakdownAggregation(sprk, service)

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())

    when(service.readOptional(LearningExperienceFinishedSource, sprk)).thenReturn(Some(ExperienceFinishedDF))

    val scoreBreakdownDF = ScoreBreakdownTransformer.transform().get.output
    assert(scoreBreakdownDF.count() == 0)
  }

  test("when ScoreBreakdown details available with lessonIds") {
    val LearningExperienceFinishedEvent =
      """
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
        |		"id": "question-id-1",
        |		"code": "QUESTION_CODE_1",
        |		"timeSpent": "4.944",
        |		"hintsUsed": "true",
        |		"type": "TYPE_1",
        |		"version": "3",
        |		"maxScore": "1",
        |		"score": "0",
        |  "lessonIds": ["lesson-id-1-question-id-1", "lesson-id-2-question-id-1"],
        |		"isAttended": "true",
        |		"attempts": [{
        |			"score": "0",
        |			"hintsUsed": "false",
        |			"isAttended": null,
        |			"maxScore": null,
        |			"suid": null,
        |			"timestamp": "2020-05-31T04:57:01.669",
        |			"timeSpent": "2.969"
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
        |   "lessonIds": ["lesson-id-1-1234-question-id-2"],
        |		"attempts": [{
        |			"score": "0",
        |			"hintsUsed": "false",
        |			"isAttended": null,
        |			"maxScore": null,
        |			"suid": null,
        |			"timestamp": "2020-05-31T04:57:08.059",
        |			"timeSpent": "2.714"
        |		  }]
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
        |  "activityCompleted": true,
        |  "materialType":"INSTRUCTIONAL_PLAN",
        |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218"
        |}
""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val ScoreBreakdownTransformer = new ScoreBreakdownAggregation(sprk, service)

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())

    when(service.readOptional(LearningExperienceFinishedSource, sprk)).thenReturn(Some(ExperienceFinishedDF))

    val scoreBreakdownDF = ScoreBreakdownTransformer.transform().get.output
    assert(scoreBreakdownDF.count() == 3)

    val expectedDF = List(
      (sep2018Timestamp, nov2021Timestamp, 20180908, "experience-id", "learning-session-id", "question-id-1", "QUESTION_CODE_1", 4.944, true, 1.0, 0.0, "lesson-id-1-question-id-1", "TYPE_1", 3, true, "2018-09-08"),
      (sep2018Timestamp, nov2021Timestamp, 20180908, "experience-id", "learning-session-id", "question-id-1", "QUESTION_CODE_1", 4.944, true, 1.0, 0.0, "lesson-id-2-question-id-1", "TYPE_1", 3, true, "2018-09-08"),
      (sep2018Timestamp, nov2021Timestamp, 20180908, "experience-id", "learning-session-id", "b5b8f4c2-e8b1-4435-aca5-50fb5b69198d", "MA8_MLO_024_Q_11_IMPORT", 4.66, true, 1.0, 0.0, "lesson-id-1-1234-question-id-2", "MULTIPLE_CHOICE", 3, true, "2018-09-08")
    ).toDF(expectedScoreBreakdownColumns: _*)

    assertSmallDatasetEquality("fle_scbd", scoreBreakdownDF, expectedDF)

  }

  test("when ScoreBreakdown details available without lessonIds") {
    val LearningExperienceFinishedEvent =
      """
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
        |		"id": "question-id-1",
        |		"code": "QUESTION_CODE_1",
        |		"timeSpent": "4.944",
        |		"hintsUsed": "true",
        |		"type": "TYPE_1",
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
        |  "activityCompleted": true,
        |  "materialType":"INSTRUCTIONAL_PLAN",
        |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218"
        |}
""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val ScoreBreakdownTransformer = new ScoreBreakdownAggregation(sprk, service)

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())

    when(service.readOptional(LearningExperienceFinishedSource, sprk)).thenReturn(Some(ExperienceFinishedDF))

    val scoreBreakdownDF = ScoreBreakdownTransformer.transform().get.output
    assert(scoreBreakdownDF.count() == 1)

    val expectedDF = List(
      (sep2018Timestamp, nov2021Timestamp, 20180908, "experience-id", "learning-session-id", "question-id-1", "QUESTION_CODE_1", 4.944, true, 1.0, 0.0, "n/a", "TYPE_1", 3, true, "2018-09-08")
    ).toDF(expectedScoreBreakdownColumns: _*)

    assertSmallDatasetEquality("fle_scbd", scoreBreakdownDF, expectedDF)

  }

  test("when ScoreBreakdown details available with lessonIds as empty array") {
    val LearningExperienceFinishedEvent =
      """
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
        |		"id": "question-id-1",
        |		"code": "QUESTION_CODE_1",
        |		"timeSpent": "4.944",
        |		"hintsUsed": "true",
        |		"type": "TYPE_1",
        |		"version": "3",
        |		"maxScore": "1",
        |		"score": "0",
        |		"isAttended": "true",
        |   "lessonIds": [],
        |		"attempts": [{
        |			"score": "0",
        |			"hintsUsed": "false",
        |			"isAttended": null,
        |			"maxScore": null,
        |			"suid": null,
        |			"timestamp": "2020-05-31T04:57:01.669",
        |			"timeSpent": "2.969"
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
        |  "activityCompleted": true,
        |  "materialType":"INSTRUCTIONAL_PLAN",
        |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218"
        |}
""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val ScoreBreakdownTransformer = new ScoreBreakdownAggregation(sprk, service)

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())

    when(service.readOptional(LearningExperienceFinishedSource, sprk)).thenReturn(Some(ExperienceFinishedDF))

    val scoreBreakdownDF = ScoreBreakdownTransformer.transform().get.output
    assert(scoreBreakdownDF.count() == 1)

    val expectedDF = List(
      (sep2018Timestamp, nov2021Timestamp, 20180908, "experience-id", "learning-session-id", "question-id-1", "QUESTION_CODE_1", 4.944, true, 1.0, 0.0, "n/a", "TYPE_1", 3, true, "2018-09-08")
    ).toDF(expectedScoreBreakdownColumns: _*)

    assertSmallDatasetEquality("fle_scbd", scoreBreakdownDF, expectedDF)

  }

  test("when ScoreBreakdown details available with lessonIds as null") {
    val LearningExperienceFinishedEvent =
      """
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
        |		"id": "question-id-1",
        |		"code": "QUESTION_CODE_1",
        |		"timeSpent": "4.944",
        |		"hintsUsed": "true",
        |		"type": "TYPE_1",
        |		"version": "3",
        |		"maxScore": "1",
        |		"score": "0",
        |		"isAttended": "true",
        |   "lessonIds": null,
        |		"attempts": [{
        |			"score": "0",
        |			"hintsUsed": "false",
        |			"isAttended": null,
        |			"maxScore": null,
        |			"suid": null,
        |			"timestamp": "2020-05-31T04:57:01.669",
        |			"timeSpent": "2.969"
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
        |  "activityCompleted": true,
        |  "materialType":"INSTRUCTIONAL_PLAN",
        |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218"
        |}
""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val ScoreBreakdownTransformer = new ScoreBreakdownAggregation(sprk, service)

    val ExperienceFinishedDF = spark.read.json(Seq(LearningExperienceFinishedEvent).toDS())

    when(service.readOptional(LearningExperienceFinishedSource, sprk)).thenReturn(Some(ExperienceFinishedDF))

    val scoreBreakdownDF = ScoreBreakdownTransformer.transform().get.output
    assert(scoreBreakdownDF.count() == 1)

    val expectedDF = List(
      (sep2018Timestamp, nov2021Timestamp, 20180908, "experience-id", "learning-session-id", "question-id-1", "QUESTION_CODE_1", 4.944, true, 1.0, 0.0, "n/a", "TYPE_1", 3, true, "2018-09-08")
    ).toDF(expectedScoreBreakdownColumns: _*)

    assertSmallDatasetEquality("fle_scbd", scoreBreakdownDF, expectedDF)

  }

}
