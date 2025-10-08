package com.alefeducation.facts.learning_experience_v2.pathway

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.learning_experience_v2.transform.pathway.PathwayLearningSessionAggregation

class PathwayLearningSessionAggregationSpec extends SparkSuite {

  test("Transform LearningSessionFinishedEvent successfully") {

    val LearningSessionFinishedEvent = """
                                         |{
                                         |  "_app_tenant": "tenant-id",
                                         |  "eventType": "LearningSessionFinished",
                                         |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
                                         |  "occurredOn": "2018-09-08 02:40:00.0",
                                         |  "startTime": "2018-09-08 02:30:00.0",
                                         |  "attempt": 2,
                                         |  "learningSessionId": "learning-session-id",
                                         |  "studentId": "student-id",
                                         |  "studentGrade": "8",
                                         |  "studentGradeId": "school-grade-id",
                                         |  "studentSection": "class-id",
                                         |  "classId": "classId-uuid",
                                         |  "subjectId": "school-subject-id",
                                         |  "subjectCode": "ARABIC",
                                         |  "subjectName": "Arabic",
                                         |  "learningObjectiveId": "learning-objective-id",
                                         |  "learningObjectiveCode": "AR8_MLO_023",
                                         |  "learningObjectiveTitle": "الحال - الجزء الثاني",
                                         |  "learningObjectiveType": "FF3",
                                         |  "schoolId": "school-id",
                                         |  "learningPathId" : "learning-path-id",
                                         |  "trimesterId": "trimester-id",
                                         |  "trimesterOrder": 1,
                                         |  "curriculumId": "392027",
                                         |  "curriculumName": "UAE MOE",
                                         |  "curriculumSubjectId": "352071",
                                         |  "curriculumSubjectName": "Arabic",
                                         |  "curriculumGradeId": "768780",
                                         |  "outsideOfSchool": true,
                                         |  "redo": true,
                                         |  "eventDateDw": "20180908",
                                         |  "stars": 2,
                                         |  "score": 34.0,
                                         |  "instructionalPlanId": "instructional-plan-id",
                                         |  "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
                                         |  "activityTemplateId": "FF4",
                                         |  "activityType": "INSTRUCTIONAL_LESSON",
                                         |  "activityComponentResources": null,
                                         |  "contentAcademicYear": "2019",
                                         |  "timeSpent" : 10,
                                         |  "lessonCategory":"INSTRUCTIONAL_LESSON",
                                         |  "totalScore":20.0,
                                         |  "materialType":"INSTRUCTIONAL_PLAN",
                                         |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
                                         |  "source": "WEB",
                                         |  "academicYear": "2024",
                                         |  "teachingPeriodId": "period_id1",
                                         |  "replayed": false,
                                         |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
                                         |}
    """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val sessionFinishedTransformer = new PathwayLearningSessionAggregation("pathway-learning-session-transform")
    val sessionFinishedDF = spark.read.json(Seq(LearningSessionFinishedEvent).toDS())
    val input = Map(
      "pathway_learning_session_start_finish_events" -> Some(sessionFinishedDF),
      "pathway_learning_session_deleted_event" -> None
    )
    val transformedSessionFinishedDF = sessionFinishedTransformer.transform(input, 0L).get

    assert(transformedSessionFinishedDF.columns.toSet === LearningSessionTestUtils.expLearningSessionCols)
    val row = transformedSessionFinishedDF.head()
    assertRow[Double](row, "total_time", 600.0)
    assertRow[Double](row, "score", 34.0)
    assertRow[Boolean](row, "outside_of_school", true)
    assertRow[Boolean](row, "retry", true)
    assertRow[String](row, "learning_path_id", "learning-path-id")
    assertRow[String](row, "academic_year_id", "e1eff0a1-9469-4581-a4c3-12dbe777c984")
    assertRow[String](row, "content_academic_year", "2019")
    assertRow[Int](row, "time_spent", 10)
    assertRow[String](row, "instructional_plan_id", "instructional-plan-id")
    assertRow[String](row, "lesson_category", "INSTRUCTIONAL_LESSON")
    assertRow[Double](row, "total_score", 20.0)
    assertRow[String](row, "material_type", "INSTRUCTIONAL_PLAN")
    assertRow[String](row, "material_id", "71d76a2c-8f62-4e91-b342-46150de25218")
    assertRow[Int](row, "state", 2)
    assertRow[String](row, "source", "WEB")
    assertRow[String](row, "academic_year", "2024")
    assertRow[String](row, "teaching_period_id", "period_id1")
  }

  test("transform LearningSessionFinished when score, stars, academicYearId, lessonCategory and contentAcademicYear are null in event") {

    val LearningSessionFinishedEvent = """
                                         |{
                                         |  "_app_tenant": "tenant-id",
                                         |  "eventType": "LearningSessionFinished",
                                         |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
                                         |  "occurredOn": "2018-09-08 02:40:00.0",
                                         |  "startTime": "2018-09-08 02:30:00.0",
                                         |  "attempt": 1,
                                         |  "learningSessionId": "learning-session-id",
                                         |  "studentId": "student-id",
                                         |  "studentGrade": "8",
                                         |  "studentGradeId": "school-grade-id",
                                         |  "studentSection": "class-id",
                                         |  "classId": "classId-uuid",
                                         |  "subjectId": "school-subject-id",
                                         |  "subjectCode": "ARABIC",
                                         |  "subjectName": "Arabic",
                                         |  "learningObjectiveId": "learning-objective-id",
                                         |  "learningObjectiveCode": "AR8_MLO_023",
                                         |  "learningObjectiveTitle": "الحال - الجزء الثاني",
                                         |  "learningObjectiveType": "FF3",
                                         |  "schoolId": "school-id",
                                         |  "learningPathId" : "learning-path-id",
                                         |  "trimesterId": "trimester-id",
                                         |  "trimesterOrder": 1,
                                         |  "curriculumId": "392027",
                                         |  "curriculumName": "UAE MOE",
                                         |  "curriculumSubjectId": "352071",
                                         |  "curriculumSubjectName": "Arabic",
                                         |  "curriculumGradeId": "768780",
                                         |  "instructionalPlanId": "instructional-plan-id",
                                         |  "outsideOfSchool": false,
                                         |  "redo": true,
                                         |  "eventDateDw": "20180908",
                                         |  "stars": null,
                                         |  "score": null,
                                         |  "academicYearId": null,
                                         |  "activityTemplateId": "FF4",
                                         |  "activityType": "INSTRUCTIONAL_LESSON",
                                         |  "activityComponentResources": null,
                                         |  "contentAcademicYear": null,
                                         |  "timeSpent": 12,
                                         |  "lessonCategory":null,
                                         |  "totalScore":null,
                                         |  "materialType":"INSTRUCTIONAL_PLAN",
                                         |  "materialId":"71d76a2c-8f62-4e91-b342-46150de25218",
                                         |  "source":"APP",
                                         |  "academicYear": "2024",
                                         |  "teachingPeriodId": "period_id1",
                                         |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
                                         |}
    """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val sessionFinishedTransformer = new PathwayLearningSessionAggregation("pathway-learning-session-transform")
    val sessionFinishedDF = spark.read.json(Seq(LearningSessionFinishedEvent).toDS())
    val input = Map(
      "pathway_learning_session_start_finish_events" -> Some(sessionFinishedDF),
      "pathway_learning_session_deleted_event" -> None
    )

    val transformedSessionFinishedDF = sessionFinishedTransformer.transform(input, 0L).get

    assert(transformedSessionFinishedDF.columns.toSet === LearningSessionTestUtils.expLearningSessionCols)
    val row = transformedSessionFinishedDF.head()
    assertRow[Double](row, "total_time", 600.0)
    assertRow[String](row, "score", "-1")
    assertRow[String](row, "stars", "-1")
    assertRow[Boolean](row, "outside_of_school", false)
    assertRow[Boolean](row, "retry", false)
    assertRow[String](row, "learning_path_id", "learning-path-id")
    assertRow[String](row, "academic_year_id", null)
    assertRow[String](row, "content_academic_year", null)
    assertRow[Long](row, "time_spent", 12)
    assertRow[String](row, "instructional_plan_id", "instructional-plan-id")
    assertRow[String](row, "lesson_category", null)
    assertRow[String](row, "material_type", "INSTRUCTIONAL_PLAN")
    assertRow[String](row, "material_id", "71d76a2c-8f62-4e91-b342-46150de25218")
    assertRow[String](row, "source", "APP")
    assertRow[String](row, "academic_year", "2024")
    assertRow[String](row, "teaching_period_id", "period_id1")
  }

  test("should be considering is_activity_completed as true for LearningSessionFinished event") {
    val LearningSessionFinishedEvent = """
                                         |{
                                         |  "_app_tenant": "tenant-id",
                                         |  "eventType": "LearningSessionFinished",
                                         |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
                                         |  "occurredOn": "2018-09-08 02:40:00.0",
                                         |  "startTime": "2018-09-08 02:30:00.0",
                                         |  "attempt": 2,
                                         |  "learningSessionId": "learning-session-id",
                                         |  "studentId": "student-id",
                                         |  "studentGrade": "8",
                                         |  "studentGradeId": "school-grade-id",
                                         |  "studentSection": "class-id",
                                         |  "classId": "classId-uuid",
                                         |  "subjectId": "school-subject-id",
                                         |  "subjectCode": "ARABIC",
                                         |  "subjectName": "Arabic",
                                         |  "learningObjectiveId": "learning-objective-id",
                                         |  "learningObjectiveCode": "AR8_MLO_023",
                                         |  "learningObjectiveTitle": "الحال - الجزء الثاني",
                                         |  "learningObjectiveType": "FF3",
                                         |  "schoolId": "school-id",
                                         |  "learningPathId" : "learning-path-id",
                                         |  "trimesterId": "trimester-id",
                                         |  "trimesterOrder": 1,
                                         |  "curriculumId": "392027",
                                         |  "curriculumName": "UAE MOE",
                                         |  "curriculumSubjectId": "352071",
                                         |  "curriculumSubjectName": "Arabic",
                                         |  "curriculumGradeId": "768780",
                                         |  "outsideOfSchool": true,
                                         |  "redo": true,
                                         |  "eventDateDw": "20180908",
                                         |  "stars": 2,
                                         |  "score": 34.0,
                                         |  "instructionalPlanId": "instructional-plan-id",
                                         |  "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
                                         |  "activityTemplateId": "FF4",
                                         |  "activityType": "INSTRUCTIONAL_LESSON",
                                         |  "activityComponentResources": null,
                                         |  "contentAcademicYear": "2019",
                                         |  "timeSpent" : 10,
                                         |  "lessonCategory":"INSTRUCTIONAL_LESSON",
                                         |  "totalScore":20.0,
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

    val sessionFinishedTransformer = new PathwayLearningSessionAggregation("pathway-learning-session-transform")
    val sessionFinishedDF = spark.read.json(Seq(LearningSessionFinishedEvent).toDS())
    val input = Map(
      "pathway_learning_session_start_finish_events" -> Some(sessionFinishedDF),
      "pathway_learning_session_deleted_event" -> None
    )

    val transformedSessionFinishedDF = sessionFinishedTransformer.transform(input, 0L).get

    assert(transformedSessionFinishedDF.columns.toSet === LearningSessionTestUtils.expLearningSessionCols)

    val row = transformedSessionFinishedDF.head()
    assertRow[Boolean](row, "activity_completed", true)
    assertRow[String](row, "material_type", "INSTRUCTIONAL_PLAN")
    assertRow[String](row, "material_id", "71d76a2c-8f62-4e91-b342-46150de25218")
    assertRow[String](row, "academic_year", "2024")
    assertRow[String](row, "teaching_period_id", "period_id1")

  }

  test("transform learning events with material null columns") {

    val LearningSessionFinishedEvent = """
                                         |{
                                         |  "_app_tenant": "tenant-id",
                                         |  "eventType": "LearningSessionFinished",
                                         |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
                                         |  "occurredOn": "2018-09-08 02:40:00.0",
                                         |  "startTime": "2018-09-08 02:30:00.0",
                                         |  "attempt": 2,
                                         |  "learningSessionId": "learning-session-id",
                                         |  "studentId": "student-id",
                                         |  "studentGrade": "8",
                                         |  "studentGradeId": "school-grade-id",
                                         |  "studentSection": "class-id",
                                         |  "classId": "classId-uuid",
                                         |  "subjectId": "school-subject-id",
                                         |  "subjectCode": "ARABIC",
                                         |  "subjectName": "Arabic",
                                         |  "learningObjectiveId": "learning-objective-id",
                                         |  "learningObjectiveCode": "AR8_MLO_023",
                                         |  "learningObjectiveTitle": "الحال - الجزء الثاني",
                                         |  "learningObjectiveType": "FF3",
                                         |  "schoolId": "school-id",
                                         |  "learningPathId" : "learning-path-id",
                                         |  "trimesterId": "trimester-id",
                                         |  "trimesterOrder": 1,
                                         |  "curriculumId": "392027",
                                         |  "curriculumName": "UAE MOE",
                                         |  "curriculumSubjectId": "352071",
                                         |  "curriculumSubjectName": "Arabic",
                                         |  "curriculumGradeId": "768780",
                                         |  "outsideOfSchool": true,
                                         |  "redo": true,
                                         |  "eventDateDw": "20180908",
                                         |  "stars": 2,
                                         |  "score": 34.0,
                                         |  "instructionalPlanId": "instructional-plan-id",
                                         |  "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
                                         |  "activityTemplateId": "FF4",
                                         |  "activityType": "INSTRUCTIONAL_LESSON",
                                         |  "activityComponentResources": null,
                                         |  "contentAcademicYear": "2019",
                                         |  "timeSpent" : 10,
                                         |  "lessonCategory":"INSTRUCTIONAL_LESSON",
                                         |  "totalScore":20.0,
                                         |  "materialType":null,
                                         |  "materialId":null,
                                         |  "source": "WEB",
                                         |  "academicYear": "2024",
                                         |  "teachingPeriodId": "period_id1",
                                         |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
                                         |}
        """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val sessionFinishedTransformer = new PathwayLearningSessionAggregation("pathway-learning-session-transform")
    val sessionFinishedDF = spark.read.json(Seq(LearningSessionFinishedEvent).toDS())
    val input = Map(
      "pathway_learning_session_start_finish_events" -> Some(sessionFinishedDF),
      "pathway_learning_session_deleted_event" -> None
    )

    val transformedSessionFinishedDF = sessionFinishedTransformer.transform(input, 0L).get

    assert(transformedSessionFinishedDF.columns.toSet === LearningSessionTestUtils.expLearningSessionCols)

    val row = transformedSessionFinishedDF.head
    assertRow[String](row, "material_type", "INSTRUCTIONAL_PLAN")
    assertRow[String](row, "material_id", "instructional-plan-id")
    assertRow[String](row, "academic_year", "2024")
    assertRow[String](row, "teaching_period_id", "period_id1")
  }

  test("transform learning events without material columns") {
    val LearningSessionFinishedEvent = """
                                         |{
                                         |  "_app_tenant": "tenant-id",
                                         |  "eventType": "LearningSessionFinished",
                                         |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
                                         |  "occurredOn": "2018-09-08 02:40:00.0",
                                         |  "startTime": "2018-09-08 02:30:00.0",
                                         |  "attempt": 2,
                                         |  "learningSessionId": "learning-session-id",
                                         |  "studentId": "student-id",
                                         |  "studentGrade": "8",
                                         |  "studentGradeId": "school-grade-id",
                                         |  "studentSection": "class-id",
                                         |  "classId": "classId-uuid",
                                         |  "subjectId": "school-subject-id",
                                         |  "subjectCode": "ARABIC",
                                         |  "subjectName": "Arabic",
                                         |  "learningObjectiveId": "learning-objective-id",
                                         |  "learningObjectiveCode": "AR8_MLO_023",
                                         |  "learningObjectiveTitle": "الحال - الجزء الثاني",
                                         |  "learningObjectiveType": "FF3",
                                         |  "schoolId": "school-id",
                                         |  "learningPathId" : "learning-path-id",
                                         |  "trimesterId": "trimester-id",
                                         |  "trimesterOrder": 1,
                                         |  "curriculumId": "392027",
                                         |  "curriculumName": "UAE MOE",
                                         |  "curriculumSubjectId": "352071",
                                         |  "curriculumSubjectName": "Arabic",
                                         |  "curriculumGradeId": "768780",
                                         |  "outsideOfSchool": true,
                                         |  "redo": true,
                                         |  "eventDateDw": "20180908",
                                         |  "stars": 2,
                                         |  "score": 34.0,
                                         |  "instructionalPlanId": "instructional-plan-id",
                                         |  "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
                                         |  "activityTemplateId": "FF4",
                                         |  "activityType": "INSTRUCTIONAL_LESSON",
                                         |  "activityComponentResources": null,
                                         |  "contentAcademicYear": "2019",
                                         |  "timeSpent" : 10,
                                         |  "lessonCategory":"INSTRUCTIONAL_LESSON",
                                         |  "totalScore":20.0,
                                         |  "source": "WEB",
                                         |  "academicYear": "2024",
                                         |  "teachingPeriodId": "period_id1",
                                         |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
                                         |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val sessionFinishedTransformer = new PathwayLearningSessionAggregation("pathway-learning-session-transform")
    val sessionFinishedDF = spark.read.json(Seq(LearningSessionFinishedEvent).toDS())
    val input = Map(
      "pathway_learning_session_start_finish_events" -> Some(sessionFinishedDF),
      "pathway_learning_session_deleted_event" -> None
    )

    val transformedSessionFinishedDF = sessionFinishedTransformer.transform(input, 0L).get

    assert(transformedSessionFinishedDF.columns.toSet === LearningSessionTestUtils.expLearningSessionCols)
    val row = transformedSessionFinishedDF.head()
    assertRow[String](row, "material_type", "INSTRUCTIONAL_PLAN")
    assertRow[String](row, "material_id", "instructional-plan-id")
    assertRow[String](row, "academic_year", "2024")
    assertRow[String](row, "teaching_period_id", "period_id1")
  }

  test("transform learning session deleted events") {
    val LearningSessionFinishedEvent = """
                                         |{
                                         |  "_app_tenant": "tenant-id",
                                         |  "eventType": "LearningSessionFinished",
                                         |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
                                         |  "occurredOn": "2018-09-08 02:40:00.0",
                                         |  "startTime": "2018-09-08 02:30:00.0",
                                         |  "attempt": 2,
                                         |  "learningSessionId": "learning-session-id",
                                         |  "studentId": "student-id",
                                         |  "studentGrade": "8",
                                         |  "studentGradeId": "school-grade-id",
                                         |  "studentSection": "class-id",
                                         |  "classId": "classId-uuid",
                                         |  "subjectId": "school-subject-id",
                                         |  "subjectCode": "ARABIC",
                                         |  "subjectName": "Arabic",
                                         |  "learningObjectiveId": "learning-objective-id",
                                         |  "learningObjectiveCode": "AR8_MLO_023",
                                         |  "learningObjectiveTitle": "الحال - الجزء الثاني",
                                         |  "learningObjectiveType": "FF3",
                                         |  "schoolId": "school-id",
                                         |  "learningPathId" : "learning-path-id",
                                         |  "trimesterId": "trimester-id",
                                         |  "trimesterOrder": 1,
                                         |  "curriculumId": "392027",
                                         |  "curriculumName": "UAE MOE",
                                         |  "curriculumSubjectId": "352071",
                                         |  "curriculumSubjectName": "Arabic",
                                         |  "curriculumGradeId": "768780",
                                         |  "outsideOfSchool": true,
                                         |  "redo": true,
                                         |  "eventDateDw": "20180908",
                                         |  "stars": 2,
                                         |  "score": 34.0,
                                         |  "source": "WEB",
                                         |  "instructionalPlanId": "instructional-plan-id",
                                         |  "academicYearId": "e1eff0a1-9469-4581-a4c3-12dbe777c984",
                                         |  "activityTemplateId": "FF4",
                                         |  "activityType": "INSTRUCTIONAL_LESSON",
                                         |  "activityComponentResources": null,
                                         |  "contentAcademicYear": "2019",
                                         |  "timeSpent" : 10,
                                         |  "lessonCategory":"INSTRUCTIONAL_LESSON",
                                         |  "totalScore":20.0,
                                         |  "materialType": "INSTRUCTIONAL_PLAN",
                                         |	"materialId": "eadd3f12-9970-4349-97b8-53c835a6a799",
                                         |  "academicYear": "2024",
                                         |  "teachingPeriodId": "period_id1",
                                         |  "inClassGameOutcomeId": "923c0f8a-83e1-4e3b-9d43-7f0188091c68"
                                         |}
      """.stripMargin

    val LearningSessionDeletedEvent = """
                                        |{
                                        | "_app_tenant": "tenant-id",
                                        |	"occurredOn": "2023-02-13T10:29:50.450",
                                        |	"learningSessionId": "d8eb3dce-5298-4230-a6ff-21886247d81d",
                                        |	"learningObjectiveId": "8f05a0b3-c8dc-4e99-966d-000000102165",
                                        |	"studentId": "2753611b-56a4-46a5-aeb9-8c68b752f8b2",
                                        |	"schoolId": "dd0b3445-af09-4883-b8b0-e3e7d6c1892b",
                                        |	"studentGradeId": "ba52db7c-3bef-4583-8528-5e19a46e2d2b",
                                        |	"studentSection": "024cba4d-155d-4e80-984b-4b92b370bcd6",
                                        |	"learningPathId": "3e2e6d9c-fdcf-48fc-9148-86ff5157422a",
                                        |	"instructionalPlanId": "eadd3f12-9970-4349-97b8-53c835a6a799",
                                        |	"subjectId": null,
                                        |	"subjectCode": "MATH",
                                        |	"subjectName": "Math",
                                        |	"academicYearId": "15cf7e91-86f0-4817-bc15-657ad524422d",
                                        |	"contentAcademicYear": "2023",
                                        |	"classId": "48145d3c-f9bf-4a47-bf0a-df5116c0c2cd",
                                        |	"outsideOfSchool": false,
                                        |	"lessonCategory": "DIAGNOSTIC_TEST",
                                        |	"activityType": "TEST",
                                        |	"materialType": "INSTRUCTIONAL_PLAN",
                                        |	"materialId": "eadd3f12-9970-4349-97b8-53c835a6a799",
                                        |	"attempt": 1,
                                        |	"learningObjectiveCode": "MathAbilityTest-RC",
                                        |	"learningObjectiveTitle": "Math Ability Test - RC",
                                        |	"learningObjectiveType": "FF4",
                                        |	"trimesterId": "f226187b-011f-4998-9282-8be2d71bad33",
                                        |	"trimesterOrder": 2,
                                        |	"curriculumId": "392027",
                                        |	"curriculumName": "UAE MOE",
                                        |	"curriculumSubjectId": "963526",
                                        |	"curriculumSubjectName": "Math",
                                        |	"curriculumGradeId": "596550",
                                        |	"startTime": "2023-02-13T10:28:39.722",
                                        |	"activityTemplateId": "FF4",
                                        |	"studentGrade": "6",
                                        |   "eventDateDw": "20180908",
                                        |   "eventType": "LearningSessionDeletedEvent",
                                        |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
                                        |   "academicYear": "2024",
                                        |   "teachingPeriodId": "period_id1"
                                        |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val sessionFinishedTransformer = new PathwayLearningSessionAggregation("pathway-learning-session-transform")
    val sessionFinishedDF = spark.read.json(Seq(LearningSessionFinishedEvent).toDS())
    val sessionDeletedDF = spark.read.json(Seq(LearningSessionDeletedEvent).toDS())
    val input = Map(
      "pathway_learning_session_start_finish_events" -> Some(sessionFinishedDF),
      "pathway_learning_session_deleted_event" -> Some(sessionDeletedDF)
    )

    val transformedSessionFinishedDF = sessionFinishedTransformer.transform(input, 0L).get

    assert(transformedSessionFinishedDF.columns.toSet === LearningSessionTestUtils.expLearningSessionCols)
    assert(transformedSessionFinishedDF.count() === 2)
    val row = transformedSessionFinishedDF.head()
    assertRow[String](row, "material_type", "INSTRUCTIONAL_PLAN")
    assertRow[String](row, "material_id", "eadd3f12-9970-4349-97b8-53c835a6a799")
    assertRow[String](row, "academic_year", "2024")
    assertRow[String](row, "teaching_period_id", "period_id1")
  }
}
