package com.alefeducation.facts

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers._

class PracticeSessionSpec extends SparkSuite {

  trait Setup {
    implicit val transformer = new PracticeSessionTransformer(PracticeFactName, spark)
  }

  val expectedRedshiftColsPSession = Set(
    "tenant_uuid",
    "practice_session_id",
    "student_uuid",
    "practice_session_stars",
    "subject_uuid",
    "school_uuid",
    "grade_uuid",
    "section_uuid",
    "lo_uuid",
    "practice_session_score",
    "practice_session_sa_score",
    "practice_session_dw_created_time",
    "practice_session_created_time",
    "practice_session_date_dw_id",
    "practice_session_event_type",
    "practice_session_is_start",
    "practice_session_is_start_event_processed",
    "practice_session_outside_of_school",
    "academic_year_uuid",
    "practice_session_instructional_plan_id",
    "practice_session_learning_path_id",
    "class_uuid",
    "practice_session_material_id",
    "practice_session_material_type"
  )

  val expectedDeltaColsPSession = Set(
    "practice_session_class_id",
    "practice_session_academic_year_id",
    "practice_session_sa_score",
    "practice_session_score",
    "practice_session_is_start",
    "eventdate",
    "practice_session_school_id",
    "practice_session_section_id",
    "practice_session_id",
    "practice_session_subject_id",
    "practice_session_instructional_plan_id",
    "practice_session_start_time",
    "practice_session_end_time",
    "practice_session_time_spent",
    "practice_session_grade_id",
    "practice_session_tenant_id",
    "practice_session_stars",
    "practice_session_event_type",
    "practice_session_lo_id",
    "practice_session_outside_of_school",
    "practice_session_learning_path_id",
    "practice_session_date_dw_id",
    "practice_session_dw_created_time",
    "practice_session_student_id",
    "practice_session_item_content_lesson_type",
    "practice_session_item_step_id",
    "practice_session_item_content_location",
    "practice_session_item_content_title",
    "practice_session_item_lo_id",
    "practice_session_is_start_event_processed",
    "practice_session_material_id",
    "practice_session_material_type"
  )

  val expectedRedshiftColsPItemSession = Set(
    "tenant_uuid",
    "practice_session_id",
    "practice_session_item_lo_uuid",
    "practice_session_score",
    "student_uuid",
    "practice_session_stars",
    "subject_uuid",
    "school_uuid",
    "grade_uuid",
    "section_uuid",
    "lo_uuid",
    "practice_session_sa_score",
    "practice_session_dw_created_time",
    "practice_session_created_time",
    "practice_session_date_dw_id",
    "practice_session_event_type",
    "practice_session_is_start",
    "practice_session_is_start_event_processed",
    "practice_session_outside_of_school",
    "academic_year_uuid",
    "practice_session_instructional_plan_id",
    "practice_session_learning_path_id",
    "class_uuid",
    "practice_session_material_id",
    "practice_session_material_type"
  )

  val expectedDeltaColsPItemSession = Set(
    "practice_session_class_id",
    "practice_session_academic_year_id",
    "practice_session_sa_score",
    "practice_session_score",
    "practice_session_is_start",
    "eventdate",
    "practice_session_school_id",
    "practice_session_section_id",
    "practice_session_id",
    "practice_session_subject_id",
    "practice_session_instructional_plan_id",
    "practice_session_start_time",
    "practice_session_end_time",
    "practice_session_time_spent",
    "practice_session_grade_id",
    "practice_session_tenant_id",
    "practice_session_stars",
    "practice_session_event_type",
    "practice_session_lo_id",
    "practice_session_outside_of_school",
    "practice_session_learning_path_id",
    "practice_session_date_dw_id",
    "practice_session_dw_created_time",
    "practice_session_item_lo_id",
    "practice_session_student_id",
    "practice_session_item_content_lesson_type",
    "practice_session_item_step_id",
    "practice_session_item_content_location",
    "practice_session_item_content_title",
    "practice_session_item_lo_id",
    "practice_session_is_start_event_processed",
    "practice_session_material_id",
    "practice_session_material_type"
  )

  val expectedRedshiftColsPItemContentSession = Set(
    "tenant_uuid",
    "practice_session_id",
    "practice_session_item_step_id",
    "practice_session_item_content_title",
    "practice_session_item_content_lesson_type",
    "practice_session_item_content_location",
    "practice_session_score",
    "student_uuid",
    "practice_session_stars",
    "subject_uuid",
    "school_uuid",
    "grade_uuid",
    "section_uuid",
    "lo_uuid",
    "practice_session_sa_score",
    "practice_session_item_lo_uuid",
    "practice_session_dw_created_time",
    "practice_session_created_time",
    "practice_session_date_dw_id",
    "practice_session_event_type",
    "practice_session_is_start",
    "practice_session_is_start_event_processed",
    "practice_session_outside_of_school",
    "academic_year_uuid",
    "practice_session_instructional_plan_id",
    "practice_session_learning_path_id",
    "class_uuid",
    "practice_session_material_id",
    "practice_session_material_type"
  )

  val expectedDeltaColsPItemContentSession = Set(
    "practice_session_class_id",
    "practice_session_academic_year_id",
    "practice_session_sa_score",
    "practice_session_score",
    "practice_session_is_start",
    "eventdate",
    "practice_session_item_content_lesson_type",
    "practice_session_item_step_id",
    "practice_session_school_id",
    "practice_session_section_id",
    "practice_session_id",
    "practice_session_subject_id",
    "practice_session_instructional_plan_id",
    "practice_session_start_time",
    "practice_session_end_time",
    "practice_session_time_spent",
    "practice_session_grade_id",
    "practice_session_tenant_id",
    "practice_session_stars",
    "practice_session_event_type",
    "practice_session_lo_id",
    "practice_session_outside_of_school",
    "practice_session_learning_path_id",
    "practice_session_date_dw_id",
    "practice_session_dw_created_time",
    "practice_session_student_id",
    "practice_session_item_content_lesson_type",
    "practice_session_item_content_location",
    "practice_session_item_content_title",
    "practice_session_item_lo_id",
    "practice_session_is_start_event_processed",
    "practice_session_material_id",
    "practice_session_material_type"
  )

  test("transform practice session events successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = PracticeSessionStarted,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "PracticeSessionStarted",
              |  "occurredOn": "2018-09-08T12:35:00.0",
              |  "practiceId": "practice-id1",
              |  "learnerId": "student-id1",
              |  "subjectId": "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a",
              |  "subjectName": "subject-name1",
              |  "schoolId": "school-id1",
              |  "gradeId": "grade-id1",
              |  "grade": 8,
              |  "sectionId": "7df52e2e-a394-4d22-be58-83497631d256",
              |  "subjectCode": "subject-code1",
              |  "learningObjectiveId": "lo-id1",
              |  "learningObjectiveCode": "lo-code1",
              |  "learningObjectiveTitle": "lo-title1",
              |  "skills": [{
              |               "uuid": "skill-uuid-1",
              |               "code": "skill-code-1",
              |               "name": "skill-name-1"
              |             }],
              |  "saScore" : 35.0,
              |  "created": "2018-09-08T12:35:00.0",
              |  "updated": "2018-09-08T12:35:00.0",
              |  "status": "IN_PROGRESS",
              |  "outsideOfSchool" : false,
              |  "academicYearId": "6074385a-7328-4f8e-92c2-9ee0d51e36d8",
              |  "eventDateDw": "20180908",
              |  "instructionalPlanId": "instructionalPlanId",
              |  "learningPathId": "learningPathId",
              |  "classId": "a6749ea5-6657-4816-9bba-def8ac525515",
              |  "materialId": "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f",
              |  "materialType": "CORE"
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = PracticeSessionFinished,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "PracticeSessionFinished",
              |  "occurredOn": "2018-09-08T13:35:00.0",
              |  "practiceId": "practice-id1",
              |  "learnerId": "student-id1",
              |  "subjectId": "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a",
              |  "subjectName": "subject-name1",
              |  "schoolId": "school-id1",
              |  "gradeId": "grade-id1",
              |  "grade": 8,
              |  "sectionId": "7df52e2e-a394-4d22-be58-83497631d256",
              |  "subjectCode": "subject-code1",
              |  "learningObjectiveId": "lo-id1",
              |  "learningObjectiveCode": "lo-code1",
              |  "learningObjectiveTitle": "lo-title1",
              |  "skills": [{
              |               "uuid": "skill-uuid-1",
              |               "code": "skill-code-1",
              |               "name": "skill-name-1"
              |             }],
              |  "saScore" : 35.0,
              |  "stars": 1,
              |  "created": "2018-09-08T12:35:00.0",
              |  "updated": "2018-09-08T13:35:00.0",
              |  "status": "COMPLETED",
              |  "score": 40.0,
              |  "outsideOfSchool" : true,
              |  "academicYearId": "6074385a-7328-4f8e-92c2-9ee0d51e36d8",
              |  "eventDateDw": "20180908",
              |  "instructionalPlanId": "instructionalPlanId",
              |  "learningPathId": "learningPathId",
              |  "classId": "a6749ea5-6657-4816-9bba-def8ac525515",
              |  "materialId": "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f",
              |  "materialType": "CORE"
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = PracticeItemSessionStarted,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "PracticeItemSessionStarted",
              |  "occurredOn": "2018-09-08T12:36:00.0",
              |  "practiceId": "practice-id1",
              |  "learningObjectiveId": "lo-id2",
              |  "learningObjectiveCode": "lo-code-2",
              |  "learningObjectiveTitle": "lo-title-2",
              |  "skills": [{
              |               "uuid": "skill-uuid-1",
              |               "code": "skill-code-1",
              |               "name": "skill-name-1"
              |             }],
              |  "status": "IN_PROGRESS",
              |  "practiceLearnerId": "student-id1",
              |  "practiceSubjectId": "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a",
              |  "practiceSubjectName": "subject-name1",
              |  "practiceSchoolId": "school-id1",
              |  "practiceGradeId": "grade-id1",
              |  "practiceGrade": 8,
              |  "practiceSectionId": "7df52e2e-a394-4d22-be58-83497631d256",
              |  "practiceLearningObjectiveId": "lo-id1",
              |  "practiceLearningObjectiveCode": "lo-code1",
              |  "practiceLearningObjectiveTitle": "lo-title1",
              |  "practiceSkills": [{
              |               "uuid": "skill-uuid-2",
              |               "code": "skill-code-2",
              |               "name": "skill-name-2"
              |             }],
              |  "practiceSaScore": 35.0,
              |  "outsideOfSchool" : true,
              |  "practiceAcademicYearId": "f9d32bde-40ce-43ca-874d-f7e0065f495a",
              |  "eventDateDw": "20180908",
              |  "instructionalPlanId": "instructionalPlanId",
              |  "learningPathId": "learningPathId",
              |  "practiceClassId": "c4aae384-befc-443a-82de-196753f2c88a",
              |  "materialId": "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f",
              |  "materialType": "CORE"
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = PracticeItemSessionFinished,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "PracticeItemSessionFinished",
              |  "occurredOn": "2018-09-08T12:40:00.0",
              |  "practiceId": "practice-id1",
              |  "learningObjectiveId": "lo-id2",
              |  "learningObjectiveCode": "lo-code-2",
              |  "learningObjectiveTitle": "lo-title-2",
              |  "skills": [{
              |               "uuid": "skill-uuid-1",
              |               "code": "skill-code-1",
              |               "name": "skill-name-1"
              |             }],
              |  "status": "IN_PROGRESS",
              |  "score": 10.0,
              |  "practiceLearnerId": "student-id1",
              |  "practiceSubjectId": "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a",
              |  "practiceSubjectName": "subject-name1",
              |  "practiceSchoolId": "school-id1",
              |  "practiceGradeId": "grade-id1",
              |  "practiceGrade": 8,
              |  "practiceSectionId": "7df52e2e-a394-4d22-be58-83497631d256",
              |  "practiceLearningObjectiveId": "lo-id1",
              |  "practiceLearningObjectiveCode": "lo-code1",
              |  "practiceLearningObjectiveTitle": "lo-title1",
              |  "practiceSkills": [{
              |               "uuid": "skill-uuid-2",
              |               "code": "skill-code-2",
              |               "name": "skill-name-2"
              |             }],
              |  "practiceSaScore": 35.0,
              |  "outsideOfSchool" : true,
              |  "practiceAcademicYearId": "f9d32bde-40ce-43ca-874d-f7e0065f495a",
              |  "eventDateDw": "20180908",
              |  "instructionalPlanId": "instructionalPlanId",
              |  "learningPathId": "learningPathId",
              |  "practiceClassId": "c4aae384-befc-443a-82de-196753f2c88a",
              |  "materialId": "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f",
              |  "materialType": "CORE"
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = PracticeItemContentSessionStarted,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "PracticeItemContentSessionStarted",
              |  "occurredOn": "2018-09-08T12:37:00.0",
              |  "practiceId": "practice-id1",
              |  "id": "content-id1",
              |  "title": "content-title1",
              |  "lessonType": "content-lesson-type1",
              |  "location": "content-lesson-location1",
              |  "status": "IN_PROGRESS",
              |  "practiceLearnerId": "student-id1",
              |  "practiceSubjectId": "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a",
              |  "practiceSubjectName": "subject-name1",
              |  "practiceSchoolId": "school-id1",
              |  "practiceGradeId": "grade-id1",
              |  "practiceGrade": 8,
              |  "practiceSectionId": "7df52e2e-a394-4d22-be58-83497631d256",
              |  "practiceLearningObjectiveId": "lo-id1",
              |  "practiceLearningObjectiveCode": "lo-code1",
              |  "practiceLearningObjectiveTitle": "lo-title1",
              |  "practiceSkills": [{
              |               "uuid": "skill-uuid-2",
              |               "code": "skill-code-2",
              |               "name": "skill-name-2"
              |             }],
              |  "practiceSaScore": 35.0,
              |  "practiceItemLearningObjectiveId": "lo-id2",
              |  "practiceItemLearningObjectiveCode": "lo-code-2",
              |  "practiceItemLearningObjectiveTitle": "lo-title-2",
              |  "practiceItemSkills": [{
              |               "uuid": "skill-uuid-3",
              |               "code": "skill-code-3",
              |               "name": "skill-name-3"
              |             }],
              |  "eventDateDw": "20180908",
              |  "outsideOfSchool" : false,
              |  "practiceAcademicYearId": "5118c0e2-0c2e-472f-a35a-337ce5779497",
              |  "instructionalPlanId": "instructionalPlanId",
              |  "learningPathId": "learningPathId",
              |  "practiceClassId": "75af7199-5fb5-4537-95f9-caf4573573bf",
              |  "materialId": "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f",
              |  "materialType": "CORE"
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = PracticeItemContentSessionFinished,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "PracticeItemContentSessionFinished",
              |  "occurredOn": "2018-09-08T12:57:00.0",
              |  "practiceId": "practice-id1",
              |  "id": "content-id1",
              |  "title": "content-title1",
              |  "lessonType": "content-lesson-type1",
              |  "location": "content-lesson-location1",
              |  "score": 25.0,
              |  "status": "IN_PROGRESS",
              |  "practiceLearnerId": "student-id1",
              |  "practiceSubjectId": "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a",
              |  "practiceSubjectName": "subject-name1",
              |  "practiceSchoolId": "school-id1",
              |  "practiceGradeId": "grade-id1",
              |  "practiceGrade": 8,
              |  "practiceSectionId": "7df52e2e-a394-4d22-be58-83497631d256",
              |  "practiceLearningObjectiveId": "lo-id1",
              |  "practiceLearningObjectiveCode": "lo-code1",
              |  "practiceLearningObjectiveTitle": "lo-title1",
              |  "practiceSkills": [{
              |               "uuid": "skill-uuid-2",
              |               "code": "skill-code-2",
              |               "name": "skill-name-2"
              |             }],
              |  "practiceSaScore": 35.0,
              |  "practiceItemLearningObjectiveId": "lo-id2",
              |  "practiceItemLearningObjectiveCode": "lo-code-2",
              |  "practiceItemLearningObjectiveTitle": "lo-title-2",
              |  "practiceItemSkills": [{
              |               "uuid": "skill-uuid-3",
              |               "code": "skill-code-3",
              |               "name": "skill-name-3"
              |             }],
              |  "eventDateDw": "20180908",
              |  "outsideOfSchool" : true,
              |  "practiceAcademicYearId": "5118c0e2-0c2e-472f-a35a-337ce5779497",
              |  "instructionalPlanId": "instructionalPlanId",
              |  "learningPathId": "learningPathId",
              |  "practiceClassId": "75af7199-5fb5-4537-95f9-caf4573573bf",
              |  "materialId": "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f",
              |  "materialType": "CORE"
              |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          var start = System.nanoTime()
          val rsSinks = sinks.filter(_.name == RedshiftPracticeSessionSink)
          assert(rsSinks.size == 6)

          val parquetSinks = sinks.filter(_.name.startsWith("parquet")).filterNot(_.name.contains("delta-staging"))
          parquetSinks.foreach(p => {
            assert(p.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
            assert[String](p.output, "eventdate", "2018-09-08")
          })

          val redshiftSessionStartDf = rsSinks.head.output
          assert(redshiftSessionStartDf.columns.toSet === expectedRedshiftColsPSession)
          val redshiftSessionStartRow = redshiftSessionStartDf.first()
          assertRow[Double](redshiftSessionStartRow, "practice_session_sa_score", 35.0)
          assertRow[Double](redshiftSessionStartRow, "practice_session_score", -1.0)
          assertRow[String](redshiftSessionStartRow, "practice_session_id", "practice-id1")
          assertRow[String](redshiftSessionStartRow, "student_uuid", "student-id1")
          assertRow[String](redshiftSessionStartRow, "lo_uuid", "lo-id1")
          assertTimestampRow(redshiftSessionStartRow, "practice_session_created_time", "2018-09-08 12:35:00.0")
          assertRow[String](redshiftSessionStartRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](redshiftSessionStartRow, "practice_session_event_type", 1)
          assertRow[Boolean](redshiftSessionStartRow, "practice_session_is_start", true)
          assertRow[Boolean](redshiftSessionStartRow, "practice_session_is_start_event_processed", false)
          assertRow[Boolean](redshiftSessionStartRow, "practice_session_outside_of_school", false)
          assertRow[Int](redshiftSessionStartRow, "practice_session_stars", DefaultStars)
          assertRow[String](redshiftSessionStartRow, "academic_year_uuid", "6074385a-7328-4f8e-92c2-9ee0d51e36d8")
          assertRow[String](redshiftSessionStartRow, "class_uuid", "a6749ea5-6657-4816-9bba-def8ac525515")
          assertRow[String](redshiftSessionStartRow, "section_uuid", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](redshiftSessionStartRow, "subject_uuid", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")
          assertRow[String](redshiftSessionStartRow, "practice_session_material_id", "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f")
          assertRow[String](redshiftSessionStartRow, "practice_session_material_type", "CORE")


          val redshiftSessionFinishedDf = rsSinks(1).output
          assert(redshiftSessionFinishedDf.columns.toSet === expectedRedshiftColsPSession)
          val redshiftSessionFinishedRow = redshiftSessionFinishedDf.first()
          assertRow[Double](redshiftSessionFinishedRow, "practice_session_sa_score", 35.0)
          assertRow[Double](redshiftSessionFinishedRow, "practice_session_score", 40.0)
          assertRow[String](redshiftSessionFinishedRow, "practice_session_id", "practice-id1")
          assertRow[String](redshiftSessionFinishedRow, "student_uuid", "student-id1")
          assertRow[String](redshiftSessionFinishedRow, "tenant_uuid", "tenant-id")
          assertRow[String](redshiftSessionFinishedRow, "lo_uuid", "lo-id1")
          assertTimestampRow(redshiftSessionFinishedRow, "practice_session_created_time", "2018-09-08 13:35:00.0")
          assertRow[String](redshiftSessionFinishedRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](redshiftSessionFinishedRow, "practice_session_event_type", 1)
          assertRow[Boolean](redshiftSessionFinishedRow, "practice_session_is_start", false)
          assertRow[Boolean](redshiftSessionFinishedRow, "practice_session_is_start_event_processed", false)
          assertRow[Boolean](redshiftSessionFinishedRow, "practice_session_outside_of_school", true)
          assertRow[Int](redshiftSessionFinishedRow, "practice_session_stars", 1)
          assertRow[String](redshiftSessionFinishedRow, "academic_year_uuid", "6074385a-7328-4f8e-92c2-9ee0d51e36d8")
          assertRow[String](redshiftSessionFinishedRow, "class_uuid", "a6749ea5-6657-4816-9bba-def8ac525515")
          assertRow[String](redshiftSessionFinishedRow, "section_uuid", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](redshiftSessionFinishedRow, "subject_uuid", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")
          assertRow[String](redshiftSessionFinishedRow, "practice_session_material_id", "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f")
          assertRow[String](redshiftSessionFinishedRow, "practice_session_material_type", "CORE")

          val deltaSessionsDf = sinks
            .filter(
              s =>
                s.name == DeltaPracticeSessionSink &&
                  s.eventType == s"${FactPracticeSessionEntity}_$practiceSessionType")
            .head
            .output
          assert(deltaSessionsDf.columns.toSet === expectedDeltaColsPSession)
          val deltaSessionsRow = deltaSessionsDf.first()
          assertRow[Double](deltaSessionsRow, "practice_session_sa_score", 35.0)
          assertRow[Double](deltaSessionsRow, "practice_session_score", 40.0)
          assertRow[String](deltaSessionsRow, "practice_session_id", "practice-id1")
          assertRow[String](deltaSessionsRow, "practice_session_student_id", "student-id1")
          assertRow[String](deltaSessionsRow, "practice_session_tenant_id", "tenant-id")
          assertRow[String](deltaSessionsRow, "practice_session_lo_id", "lo-id1")
          assertTimestampRow(deltaSessionsRow, "practice_session_start_time", "2018-09-08 12:35:00.0")
          assertTimestampRow(deltaSessionsRow, "practice_session_end_time", "2018-09-08 13:35:00.0")
          assertRow[Long](deltaSessionsRow, "practice_session_time_spent", 3600)
          assertRow[String](deltaSessionsRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](deltaSessionsRow, "practice_session_event_type", 1)
          assertRow[Boolean](deltaSessionsRow, "practice_session_is_start", false)
          assertRow[Boolean](deltaSessionsRow, "practice_session_outside_of_school", true)
          assertRow[Int](deltaSessionsRow, "practice_session_stars", 1)
          assertRow[String](deltaSessionsRow, "practice_session_academic_year_id", "6074385a-7328-4f8e-92c2-9ee0d51e36d8")
          assertRow[String](deltaSessionsRow, "practice_session_class_id", "a6749ea5-6657-4816-9bba-def8ac525515")
          assertRow[String](deltaSessionsRow, "practice_session_section_id", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](deltaSessionsRow, "practice_session_subject_id", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")
          assertRow[Boolean](deltaSessionsRow, "practice_session_is_start_event_processed", false)
          assertRow[String](deltaSessionsRow, "eventdate", "2018-09-08")
          assertRow[String](deltaSessionsRow, "practice_session_item_content_title", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_content_lesson_type", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_content_location", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_lo_id", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_step_id", null)
          assertRow[String](deltaSessionsRow, "practice_session_material_id", "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f")
          assertRow[String](deltaSessionsRow, "practice_session_material_type", "CORE")


          val redshiftItemSessionStartDf = rsSinks(2).output
          assert(redshiftItemSessionStartDf.columns.toSet === expectedRedshiftColsPItemSession)
          val redshiftItemSessionStartRow = redshiftItemSessionStartDf.first()
          assertRow[Double](redshiftItemSessionStartRow, "practice_session_sa_score", 35.0)
          assertRow[Double](redshiftItemSessionStartRow, "practice_session_score", -1.0)
          assertRow[String](redshiftItemSessionStartRow, "practice_session_id", "practice-id1")
          assertRow[String](redshiftItemSessionStartRow, "student_uuid", "student-id1")
          assertRow[String](redshiftItemSessionStartRow, "practice_session_item_lo_uuid", "lo-id2")
          assertTimestampRow(redshiftItemSessionStartRow, "practice_session_created_time", "2018-09-08 12:36:00.0")
          assertRow[String](redshiftItemSessionStartRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](redshiftItemSessionStartRow, "practice_session_event_type", 2)
          assertRow[Boolean](redshiftItemSessionStartRow, "practice_session_is_start", true)
          assertRow[Boolean](redshiftItemSessionStartRow, "practice_session_is_start_event_processed", false)
          assertRow[Boolean](redshiftItemSessionStartRow, "practice_session_outside_of_school", true)
          assertRow[Int](redshiftItemSessionStartRow, "practice_session_stars", DefaultStars)
          assertRow[String](redshiftItemSessionStartRow, "academic_year_uuid", "f9d32bde-40ce-43ca-874d-f7e0065f495a")
          assertRow[String](redshiftItemSessionStartRow, "class_uuid", "c4aae384-befc-443a-82de-196753f2c88a")
          assertRow[String](redshiftItemSessionStartRow, "section_uuid", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](redshiftItemSessionStartRow, "subject_uuid", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")
          assertRow[String](redshiftItemSessionStartRow, "practice_session_material_id", "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f")
          assertRow[String](redshiftItemSessionStartRow, "practice_session_material_type", "CORE")

          val redshiftItemSessionFinishedDf = rsSinks(3).output
          assert(redshiftItemSessionFinishedDf.columns.toSet === expectedRedshiftColsPItemSession)
          val redshiftItemSessionFinishedRow = redshiftItemSessionFinishedDf.first()
          assertRow[Double](redshiftItemSessionFinishedRow, "practice_session_sa_score", 35.0)
          assertRow[Double](redshiftItemSessionFinishedRow, "practice_session_score", 10.0)
          assertRow[String](redshiftItemSessionFinishedRow, "practice_session_id", "practice-id1")
          assertRow[String](redshiftItemSessionFinishedRow, "student_uuid", "student-id1")
          assertRow[String](redshiftItemSessionFinishedRow, "practice_session_item_lo_uuid", "lo-id2")
          assertTimestampRow(redshiftItemSessionFinishedRow, "practice_session_created_time", "2018-09-08 12:40:00.0")
          assertRow[String](redshiftItemSessionFinishedRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](redshiftItemSessionFinishedRow, "practice_session_event_type", 2)
          assertRow[Boolean](redshiftItemSessionFinishedRow, "practice_session_is_start", false)
          assertRow[Boolean](redshiftItemSessionFinishedRow, "practice_session_is_start_event_processed", false)
          assertRow[Boolean](redshiftItemSessionFinishedRow, "practice_session_outside_of_school", true)
          assertRow[Int](redshiftItemSessionFinishedRow, "practice_session_stars", DefaultStars)
          assertRow[String](redshiftItemSessionFinishedRow, "academic_year_uuid", "f9d32bde-40ce-43ca-874d-f7e0065f495a")
          assertRow[String](redshiftItemSessionFinishedRow, "class_uuid", "c4aae384-befc-443a-82de-196753f2c88a")
          assertRow[String](redshiftItemSessionFinishedRow, "section_uuid", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](redshiftItemSessionFinishedRow, "subject_uuid", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")
          assertRow[String](redshiftItemSessionFinishedRow, "practice_session_material_id", "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f")
          assertRow[String](redshiftItemSessionFinishedRow, "practice_session_material_type", "CORE")

          val deltaItemSessionsDf = sinks
            .filter(
              s =>
                s.name == DeltaPracticeSessionSink &&
                  s.eventType == s"${FactPracticeSessionEntity}_$practiceItemSessionType")
            .head
            .output
          assert(deltaItemSessionsDf.columns.toSet === expectedDeltaColsPItemSession)
          val deltaItemSessionsRow = deltaItemSessionsDf.first()
          assertRow[Double](deltaItemSessionsRow, "practice_session_sa_score", 35.0)
          assertRow[Double](deltaItemSessionsRow, "practice_session_score", 10.0)
          assertRow[String](deltaItemSessionsRow, "practice_session_id", "practice-id1")
          assertRow[String](deltaItemSessionsRow, "practice_session_student_id", "student-id1")
          assertRow[String](deltaItemSessionsRow, "practice_session_item_lo_id", "lo-id2")
          assertTimestampRow(deltaItemSessionsRow, "practice_session_start_time", "2018-09-08 12:36:00.0")
          assertTimestampRow(deltaItemSessionsRow, "practice_session_end_time", "2018-09-08 12:40:00.0")
          assertRow[Long](deltaItemSessionsRow, "practice_session_time_spent", 240)
          assertRow[String](deltaItemSessionsRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](deltaItemSessionsRow, "practice_session_event_type", 2)
          assertRow[Boolean](deltaItemSessionsRow, "practice_session_is_start", false)
          assertRow[Boolean](deltaItemSessionsRow, "practice_session_outside_of_school", true)
          assertRow[Int](deltaItemSessionsRow, "practice_session_stars", DefaultStars)
          assertRow[String](deltaItemSessionsRow, "practice_session_academic_year_id", "f9d32bde-40ce-43ca-874d-f7e0065f495a")
          assertRow[String](deltaItemSessionsRow, "practice_session_class_id", "c4aae384-befc-443a-82de-196753f2c88a")
          assertRow[String](deltaItemSessionsRow, "practice_session_section_id", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](deltaItemSessionsRow, "practice_session_subject_id", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")
          assertRow[Boolean](deltaItemSessionsRow, "practice_session_is_start_event_processed", false)
          assertRow[String](deltaItemSessionsRow, "eventdate", "2018-09-08")
          assertRow[String](deltaSessionsRow, "practice_session_item_content_title", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_content_lesson_type", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_content_location", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_step_id", null)
          assertRow[String](deltaSessionsRow, "practice_session_material_id", "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f")
          assertRow[String](deltaSessionsRow, "practice_session_material_type", "CORE")

          val redshiftItemContentSessionStartDf = rsSinks(4).output
          assert(redshiftItemContentSessionStartDf.columns.toSet === expectedRedshiftColsPItemContentSession)
          val redshiftItemContentSessionStartRow = redshiftItemContentSessionStartDf.first()
          assertRow[Double](redshiftItemContentSessionStartRow, "practice_session_sa_score", 35.0)
          assertRow[Double](redshiftItemContentSessionStartRow, "practice_session_score", -1.0)
          assertRow[String](redshiftItemContentSessionStartRow, "practice_session_id", "practice-id1")
          assertRow[String](redshiftItemContentSessionStartRow, "student_uuid", "student-id1")
          assertRow[String](redshiftItemContentSessionStartRow, "practice_session_item_lo_uuid", "lo-id2")
          assertRow[String](redshiftItemContentSessionStartRow, "practice_session_item_step_id", "content-id1")
          assertTimestampRow(redshiftItemContentSessionStartRow, "practice_session_created_time", "2018-09-08 12:37:00.0")
          assertRow[String](redshiftItemContentSessionStartRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](redshiftItemContentSessionStartRow, "practice_session_event_type", 3)
          assertRow[Boolean](redshiftItemContentSessionStartRow, "practice_session_is_start", true)
          assertRow[Boolean](redshiftItemContentSessionStartRow, "practice_session_is_start_event_processed", false)
          assertRow[Boolean](redshiftItemContentSessionStartRow, "practice_session_outside_of_school", false)
          assertRow[Int](redshiftItemContentSessionStartRow, "practice_session_stars", DefaultStars)
          assertRow[String](redshiftItemContentSessionStartRow, "academic_year_uuid", "5118c0e2-0c2e-472f-a35a-337ce5779497")
          assertRow[String](redshiftItemContentSessionStartRow, "class_uuid", "75af7199-5fb5-4537-95f9-caf4573573bf")
          assertRow[String](redshiftItemContentSessionStartRow, "section_uuid", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](redshiftItemContentSessionStartRow, "subject_uuid", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")
          assertRow[String](redshiftItemContentSessionStartRow, "practice_session_material_id", "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f")
          assertRow[String](redshiftItemContentSessionStartRow, "practice_session_material_type", "CORE")

          val redshiftItemContentSessionFinishedDf = rsSinks(5).output
          assert(redshiftItemContentSessionFinishedDf.columns.toSet === expectedRedshiftColsPItemContentSession)
          val redshiftItemContentSessionFinishedRow = redshiftItemContentSessionFinishedDf.first()
          assertRow[Double](redshiftItemContentSessionFinishedRow, "practice_session_sa_score", 35.0)
          assertRow[Double](redshiftItemContentSessionFinishedRow, "practice_session_score", 25.0)
          assertRow[String](redshiftItemContentSessionFinishedRow, "practice_session_id", "practice-id1")
          assertRow[String](redshiftItemContentSessionFinishedRow, "practice_session_item_lo_uuid", "lo-id2")
          assertRow[String](redshiftItemContentSessionFinishedRow, "practice_session_item_step_id", "content-id1")
          assertTimestampRow(redshiftItemContentSessionFinishedRow, "practice_session_created_time", "2018-09-08 12:57:00.0")
          assertRow[Int](redshiftItemContentSessionFinishedRow, "practice_session_event_type", 3)
          assertRow[Boolean](redshiftItemContentSessionFinishedRow, "practice_session_is_start", false)
          assertRow[Boolean](redshiftItemContentSessionFinishedRow, "practice_session_is_start_event_processed", false)
          assertRow[Boolean](redshiftItemContentSessionFinishedRow, "practice_session_outside_of_school", true)
          assertRow[Int](redshiftItemContentSessionFinishedRow, "practice_session_stars", DefaultStars)
          assertRow[String](redshiftItemContentSessionFinishedRow, "academic_year_uuid", "5118c0e2-0c2e-472f-a35a-337ce5779497")
          assertRow[String](redshiftItemContentSessionFinishedRow, "class_uuid", "75af7199-5fb5-4537-95f9-caf4573573bf")
          assertRow[String](redshiftItemContentSessionFinishedRow, "section_uuid", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](redshiftItemContentSessionFinishedRow, "subject_uuid", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")
          assertRow[String](redshiftItemContentSessionFinishedRow, "practice_session_material_id", "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f")
          assertRow[String](redshiftItemContentSessionFinishedRow, "practice_session_material_type", "CORE")

          val deltaItemContentSessiosDf = sinks
            .filter(
              s =>
                s.name == DeltaPracticeSessionSink &&
                  s.eventType == s"${FactPracticeSessionEntity}_$practiceItemContentSessionType")
            .head
            .output
          assert(deltaItemContentSessiosDf.columns.toSet === expectedDeltaColsPItemContentSession)
          val deltaItemContentSessiosRow = deltaItemContentSessiosDf.first()
          assertRow[Double](deltaItemContentSessiosRow, "practice_session_sa_score", 35.0)
          assertRow[Double](deltaItemContentSessiosRow, "practice_session_score", 25.0)
          assertRow[String](deltaItemContentSessiosRow, "practice_session_id", "practice-id1")
          assertRow[String](deltaItemContentSessiosRow, "practice_session_item_lo_id", "lo-id2")
          assertRow[String](deltaItemContentSessiosRow, "practice_session_item_step_id", "content-id1")
          assertTimestampRow(deltaItemContentSessiosRow, "practice_session_start_time", "2018-09-08 12:37:00.0")
          assertTimestampRow(deltaItemContentSessiosRow, "practice_session_end_time", "2018-09-08 12:57:00.0")
          assertRow[Long](deltaItemContentSessiosRow, "practice_session_time_spent", 1200)
          assertRow[Int](deltaItemContentSessiosRow, "practice_session_event_type", 3)
          assertRow[Boolean](deltaItemContentSessiosRow, "practice_session_is_start", false)
          assertRow[Boolean](deltaItemContentSessiosRow, "practice_session_outside_of_school", true)
          assertRow[Int](deltaItemContentSessiosRow, "practice_session_stars", DefaultStars)
          assertRow[String](deltaItemContentSessiosRow, "practice_session_academic_year_id", "5118c0e2-0c2e-472f-a35a-337ce5779497")
          assertRow[String](deltaItemContentSessiosRow, "practice_session_class_id", "75af7199-5fb5-4537-95f9-caf4573573bf")
          assertRow[String](deltaItemContentSessiosRow, "practice_session_section_id", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](deltaItemContentSessiosRow, "practice_session_subject_id", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")
          assertRow[Boolean](deltaItemContentSessiosRow, "practice_session_is_start_event_processed", false)
          assertRow[String](deltaItemContentSessiosRow, "eventdate", "2018-09-08")
          assertRow[String](deltaSessionsRow, "practice_session_item_content_title", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_content_lesson_type", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_content_location", null)
          assertRow[String](deltaSessionsRow, "practice_session_material_id", "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f")
          assertRow[String](deltaSessionsRow, "practice_session_material_type", "CORE")
        }
      )
    }
  }

  test("transform practice session end events with null successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = PracticeSessionStarted,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "PracticeSessionStarted",
              |  "occurredOn": "2018-09-08T12:35:00.0",
              |  "practiceId": "practice-id1",
              |  "learnerId": "student-id1",
              |  "subjectId": "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a",
              |  "subjectName": "subject-name1",
              |  "schoolId": "school-id1",
              |  "gradeId": "grade-id1",
              |  "grade": 8,
              |  "sectionId": "7df52e2e-a394-4d22-be58-83497631d256",
              |  "subjectCode": "subject-code1",
              |  "learningObjectiveId": "lo-id1",
              |  "learningObjectiveCode": "lo-code1",
              |  "learningObjectiveTitle": "lo-title1",
              |  "skills": [{
              |               "uuid": "skill-uuid-1",
              |               "code": "skill-code-1",
              |               "name": "skill-name-1"
              |             }],
              |  "saScore" : 35.0,
              |  "created": "2018-09-08T12:35:00.0",
              |  "updated": "2018-09-08T12:35:00.0",
              |  "status": "IN_PROGRESS",
              |  "outsideOfSchool" : false,
              |  "academicYearId": "60a2603d-1547-49f1-906c-85179248efec",
              |  "eventDateDw": "20180908",
              |  "instructionalPlanId": "instructionalPlanId",
              |  "learningPathId": "learningPathId",
              |  "classId": "a6749ea5-6657-4816-9bba-def8ac525515",
              |  "materialId": null,
              |  "materialType": null
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = PracticeSessionFinished,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "PracticeSessionFinished",
              |  "occurredOn": "2018-09-08T13:35:00.0",
              |  "practiceId": "practice-id1",
              |  "learnerId": "student-id1",
              |  "subjectId": "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a",
              |  "subjectName": "subject-name1",
              |  "schoolId": "school-id1",
              |  "gradeId": "grade-id1",
              |  "grade": 8,
              |  "sectionId": "7df52e2e-a394-4d22-be58-83497631d256",
              |  "subjectCode": "subject-code1",
              |  "learningObjectiveId": "lo-id1",
              |  "learningObjectiveCode": "lo-code1",
              |  "learningObjectiveTitle": "lo-title1",
              |  "skills": [{
              |               "uuid": "skill-uuid-1",
              |               "code": "skill-code-1",
              |               "name": "skill-name-1"
              |             }],
              |  "saScore" : 35.0,
              |  "stars": null,
              |  "created": "2018-09-08T12:35:00.0",
              |  "updated": "2018-09-08T13:35:00.0",
              |  "status": "COMPLETED",
              |  "score": 40.0,
              |  "outsideOfSchool" : true,
              |  "academicYearId": "60a2603d-1547-49f1-906c-85179248efec",
              |  "eventDateDw": "20180908",
              |  "instructionalPlanId": "instructionalPlanId",
              |  "learningPathId": "learningPathId",
              |  "classId": "a6749ea5-6657-4816-9bba-def8ac525515",
              |  "materialId": null,
              |  "materialType": null
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = PracticeItemSessionStarted,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "PracticeItemSessionStarted",
              |  "occurredOn": "2018-09-08T12:36:00.0",
              |  "practiceId": "practice-id1",
              |  "learningObjectiveId": "lo-id2",
              |  "learningObjectiveCode": "lo-code-2",
              |  "learningObjectiveTitle": "lo-title-2",
              |  "skills": [{
              |               "uuid": "skill-uuid-1",
              |               "code": "skill-code-1",
              |               "name": "skill-name-1"
              |             }],
              |  "status": "IN_PROGRESS",
              |  "practiceLearnerId": "student-id1",
              |  "practiceSubjectId": "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a",
              |  "practiceSubjectName": "subject-name1",
              |  "practiceSchoolId": "school-id1",
              |  "practiceGradeId": "grade-id1",
              |  "practiceGrade": 8,
              |  "practiceSectionId": "7df52e2e-a394-4d22-be58-83497631d256",
              |  "practiceLearningObjectiveId": "lo-id1",
              |  "practiceLearningObjectiveCode": "lo-code1",
              |  "practiceLearningObjectiveTitle": "lo-title1",
              |  "practiceSkills": [{
              |               "uuid": "skill-uuid-2",
              |               "code": "skill-code-2",
              |               "name": "skill-name-2"
              |             }],
              |  "practiceSaScore": 35.0,
              |  "outsideOfSchool" : true,
              |  "practiceAcademicYearId": "98f50c3c-be1a-4429-abc6-4a64aaa2a001",
              |  "eventDateDw": "20180908",
              |  "instructionalPlanId": "instructionalPlanId",
              |  "learningPathId": "learningPathId",
              |  "practiceClassId": "75af7199-5fb5-4537-95f9-caf4573573bf",
              |  "materialId": null,
              |  "materialType": null
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = PracticeItemSessionFinished,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "PracticeItemSessionFinished",
              |  "occurredOn": "2018-09-08T12:40:00.0",
              |  "practiceId": "practice-id1",
              |  "learningObjectiveId": "lo-id2",
              |  "learningObjectiveCode": "lo-code-2",
              |  "learningObjectiveTitle": "lo-title-2",
              |  "skills": [{
              |               "uuid": "skill-uuid-1",
              |               "code": "skill-code-1",
              |               "name": "skill-name-1"
              |             }],
              |  "status": "IN_PROGRESS",
              |  "score": 10.0,
              |  "practiceLearnerId": "student-id1",
              |  "practiceSubjectId": "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a",
              |  "practiceSubjectName": "subject-name1",
              |  "practiceSchoolId": "school-id1",
              |  "practiceGradeId": "grade-id1",
              |  "practiceGrade": 8,
              |  "practiceSectionId": "7df52e2e-a394-4d22-be58-83497631d256",
              |  "practiceLearningObjectiveId": "lo-id1",
              |  "practiceLearningObjectiveCode": "lo-code1",
              |  "practiceLearningObjectiveTitle": "lo-title1",
              |  "practiceSkills": [{
              |               "uuid": "skill-uuid-2",
              |               "code": "skill-code-2",
              |               "name": "skill-name-2"
              |             }],
              |  "practiceSaScore": 35.0,
              |  "outsideOfSchool" : true,
              |  "practiceAcademicYearId": "98f50c3c-be1a-4429-abc6-4a64aaa2a001",
              |  "eventDateDw": "20180908",
              |  "instructionalPlanId": "instructionalPlanId",
              |  "learningPathId": "learningPathId",
              |  "practiceClassId": "75af7199-5fb5-4537-95f9-caf4573573bf",
              |  "materialId": null,
              |  "materialType": null
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = PracticeItemContentSessionStarted,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "PracticeItemContentSessionStarted",
              |  "occurredOn": "2018-09-08T12:37:00.0",
              |  "practiceId": "practice-id1",
              |  "id": "content-id1",
              |  "title": "content-title1",
              |  "lessonType": "content-lesson-type1",
              |  "location": "content-lesson-location1",
              |  "status": "IN_PROGRESS",
              |  "practiceLearnerId": "student-id1",
              |  "practiceSubjectId": "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a",
              |  "practiceSubjectName": "subject-name1",
              |  "practiceSchoolId": "school-id1",
              |  "practiceGradeId": "grade-id1",
              |  "practiceGrade": 8,
              |  "practiceSectionId": "7df52e2e-a394-4d22-be58-83497631d256",
              |  "practiceLearningObjectiveId": "lo-id1",
              |  "practiceLearningObjectiveCode": "lo-code1",
              |  "practiceLearningObjectiveTitle": "lo-title1",
              |  "practiceSkills": [{
              |               "uuid": "skill-uuid-2",
              |               "code": "skill-code-2",
              |               "name": "skill-name-2"
              |             }],
              |  "practiceSaScore": 35.0,
              |  "practiceItemLearningObjectiveId": "lo-id2",
              |  "practiceItemLearningObjectiveCode": "lo-code-2",
              |  "practiceItemLearningObjectiveTitle": "lo-title-2",
              |  "practiceItemSkills": [{
              |               "uuid": "skill-uuid-3",
              |               "code": "skill-code-3",
              |               "name": "skill-name-3"
              |             }],
              |  "eventDateDw": "20180908",
              |  "practiceAcademicYearId": "03cb533d-f46a-436a-9892-bbf816d7c4fb",
              |  "outsideOfSchool" : false,
              |  "instructionalPlanId": "instructionalPlanId",
              |  "learningPathId": "learningPathId",
              |  "practiceClassId": "75af7199-5fb5-4537-95f9-caf4573573bf",
              |  "materialId": null,
              |  "materialType": null
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = PracticeItemContentSessionFinished,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "PracticeItemContentSessionFinished",
              |  "occurredOn": "2018-09-08T12:57:00.0",
              |  "practiceId": "practice-id1",
              |  "id": "content-id1",
              |  "title": "content-title1",
              |  "lessonType": "content-lesson-type1",
              |  "location": "content-lesson-location1",
              |  "score": 25.0,
              |  "status": "IN_PROGRESS",
              |  "practiceLearnerId": "student-id1",
              |  "practiceSubjectId": "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a",
              |  "practiceSubjectName": "subject-name1",
              |  "practiceSchoolId": "school-id1",
              |  "practiceGradeId": "grade-id1",
              |  "practiceGrade": 8,
              |  "practiceSectionId": "7df52e2e-a394-4d22-be58-83497631d256",
              |  "practiceLearningObjectiveId": "lo-id1",
              |  "practiceLearningObjectiveCode": "lo-code1",
              |  "practiceLearningObjectiveTitle": "lo-title1",
              |  "practiceSkills": [{
              |               "uuid": "skill-uuid-2",
              |               "code": "skill-code-2",
              |               "name": "skill-name-2"
              |             }],
              |  "practiceSaScore": 35.0,
              |  "practiceItemLearningObjectiveId": "lo-id2",
              |  "practiceItemLearningObjectiveCode": "lo-code-2",
              |  "practiceItemLearningObjectiveTitle": "lo-title-2",
              |  "practiceItemSkills": [{
              |               "uuid": "skill-uuid-3",
              |               "code": "skill-code-3",
              |               "name": "skill-name-3"
              |             }],
              |  "eventDateDw": "20180908",
              |  "outsideOfSchool" : true,
              |  "practiceAcademicYearId": "03cb533d-f46a-436a-9892-bbf816d7c4fb",
              |  "instructionalPlanId": "instructionalPlanId",
              |  "learningPathId": "learningPathId",
              |  "practiceClassId": "75af7199-5fb5-4537-95f9-caf4573573bf",
              |  "materialId": null,
              |  "materialType": null
              |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val rsSinks = sinks.filter(_.name == RedshiftPracticeSessionSink)
          assert(rsSinks.size == 6)

          val parquetSinks = sinks.filter(_.name.startsWith("parquet")).filterNot(_.name.contains("delta-staging"))
          parquetSinks.foreach(p => {
            assert(p.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
            assert[String](p.output, "eventdate", "2018-09-08")
          })

          val redshiftSessionStartDf = rsSinks.head.output
          assert(redshiftSessionStartDf.columns.toSet === expectedRedshiftColsPSession)
          val redshiftSessionStartRow = redshiftSessionStartDf.first()
          assertRow[Double](redshiftSessionStartRow, "practice_session_sa_score", 35.0)
          assertRow[Double](redshiftSessionStartRow, "practice_session_score", -1.0)
          assertRow[String](redshiftSessionStartRow, "practice_session_id", "practice-id1")
          assertRow[String](redshiftSessionStartRow, "student_uuid", "student-id1")
          assertRow[String](redshiftSessionStartRow, "lo_uuid", "lo-id1")
          assertTimestampRow(redshiftSessionStartRow, "practice_session_created_time", "2018-09-08 12:35:00.0")
          assertRow[String](redshiftSessionStartRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](redshiftSessionStartRow, "practice_session_event_type", 1)
          assertRow[Boolean](redshiftSessionStartRow, "practice_session_is_start", true)
          assertRow[Boolean](redshiftSessionStartRow, "practice_session_is_start_event_processed", false)
          assertRow[Boolean](redshiftSessionStartRow, "practice_session_outside_of_school", false)
          assertRow[Int](redshiftSessionStartRow, "practice_session_stars", DefaultStars)
          assertRow[String](redshiftSessionStartRow, "academic_year_uuid", "60a2603d-1547-49f1-906c-85179248efec")
          assertRow[String](redshiftSessionStartRow, "class_uuid", "a6749ea5-6657-4816-9bba-def8ac525515")
          assertRow[String](redshiftSessionStartRow, "section_uuid", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](redshiftSessionStartRow, "subject_uuid", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")
          assertRow[String](redshiftSessionStartRow, "practice_session_material_id", null)
          assertRow[String](redshiftSessionStartRow, "practice_session_material_type", null)

          val redshiftSessionFinishedDf = rsSinks(1).output
          assert(redshiftSessionFinishedDf.columns.toSet === expectedRedshiftColsPSession)
          val redshiftSessionFinishedRow = redshiftSessionFinishedDf.first()
          assertRow[Double](redshiftSessionFinishedRow, "practice_session_sa_score", 35.0)
          assertRow[Double](redshiftSessionFinishedRow, "practice_session_score", 40.0)
          assertRow[String](redshiftSessionFinishedRow, "practice_session_id", "practice-id1")
          assertRow[String](redshiftSessionFinishedRow, "student_uuid", "student-id1")
          assertRow[String](redshiftSessionFinishedRow, "tenant_uuid", "tenant-id")
          assertRow[String](redshiftSessionFinishedRow, "lo_uuid", "lo-id1")
          assertTimestampRow(redshiftSessionFinishedRow, "practice_session_created_time", "2018-09-08 13:35:00.0")
          assertRow[String](redshiftSessionFinishedRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](redshiftSessionFinishedRow, "practice_session_event_type", 1)
          assertRow[Boolean](redshiftSessionFinishedRow, "practice_session_is_start", false)
          assertRow[Boolean](redshiftSessionFinishedRow, "practice_session_is_start_event_processed", false)
          assertRow[Boolean](redshiftSessionFinishedRow, "practice_session_outside_of_school", true)
          assertRow[String](redshiftSessionFinishedRow, "practice_session_stars", "-1")
          assertRow[String](redshiftSessionFinishedRow, "academic_year_uuid", "60a2603d-1547-49f1-906c-85179248efec")
          assertRow[String](redshiftSessionFinishedRow, "class_uuid", "a6749ea5-6657-4816-9bba-def8ac525515")
          assertRow[String](redshiftSessionFinishedRow, "section_uuid", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](redshiftSessionFinishedRow, "subject_uuid", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")

          val deltaSessionsDf = sinks
            .filter(
              s =>
                s.name == DeltaPracticeSessionSink &&
                  s.eventType == s"${FactPracticeSessionEntity}_$practiceSessionType")
            .head
            .output
          assert(deltaSessionsDf.columns.toSet === expectedDeltaColsPSession)
          val deltaSessionsRow = deltaSessionsDf.first()
          assertRow[Double](deltaSessionsRow, "practice_session_sa_score", 35.0)
          assertRow[Double](deltaSessionsRow, "practice_session_score", 40.0)
          assertRow[String](deltaSessionsRow, "practice_session_id", "practice-id1")
          assertRow[String](deltaSessionsRow, "practice_session_student_id", "student-id1")
          assertRow[String](deltaSessionsRow, "practice_session_tenant_id", "tenant-id")
          assertRow[String](deltaSessionsRow, "practice_session_lo_id", "lo-id1")
          assertTimestampRow(deltaSessionsRow, "practice_session_start_time", "2018-09-08 12:35:00.0")
          assertTimestampRow(deltaSessionsRow, "practice_session_end_time", "2018-09-08 13:35:00.0")
          assertRow[Long](deltaSessionsRow, "practice_session_time_spent", 3600)
          assertRow[String](deltaSessionsRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](deltaSessionsRow, "practice_session_event_type", 1)
          assertRow[Boolean](deltaSessionsRow, "practice_session_is_start", false)
          assertRow[Boolean](deltaSessionsRow, "practice_session_outside_of_school", true)
          assertRow[String](deltaSessionsRow, "practice_session_stars", "-1")
          assertRow[String](deltaSessionsRow, "practice_session_academic_year_id", "60a2603d-1547-49f1-906c-85179248efec")
          assertRow[String](deltaSessionsRow, "practice_session_class_id", "a6749ea5-6657-4816-9bba-def8ac525515")
          assertRow[String](deltaSessionsRow, "practice_session_section_id", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](deltaSessionsRow, "practice_session_subject_id", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")
          assertRow[Boolean](deltaSessionsRow, "practice_session_is_start_event_processed", false)
          assertRow[String](deltaSessionsRow, "eventdate", "2018-09-08")
          assertRow[String](deltaSessionsRow, "practice_session_item_content_title", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_content_lesson_type", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_content_location", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_lo_id", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_step_id", null)

          val redshiftItemSessionStartDf = rsSinks(2).output
          assert(redshiftItemSessionStartDf.columns.toSet === expectedRedshiftColsPItemSession)
          assert(redshiftItemSessionStartDf.count == 1)
          val redshiftItemSessionStartRow = redshiftItemSessionStartDf.first()
          assertRow[Double](redshiftItemSessionStartRow, "practice_session_sa_score", 35.0)
          assertRow[Double](redshiftItemSessionStartRow, "practice_session_score", -1.0)
          assertRow[String](redshiftItemSessionStartRow, "practice_session_id", "practice-id1")
          assertRow[String](redshiftItemSessionStartRow, "student_uuid", "student-id1")
          assertRow[String](redshiftItemSessionStartRow, "practice_session_item_lo_uuid", "lo-id2")
          assertTimestampRow(redshiftItemSessionStartRow, "practice_session_created_time", "2018-09-08 12:36:00.0")
          assertRow[String](redshiftItemSessionStartRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](redshiftItemSessionStartRow, "practice_session_event_type", 2)
          assertRow[Boolean](redshiftItemSessionStartRow, "practice_session_is_start", true)
          assertRow[Boolean](redshiftItemSessionStartRow, "practice_session_is_start_event_processed", false)
          assertRow[Boolean](redshiftItemSessionStartRow, "practice_session_outside_of_school", true)
          assertRow[Int](redshiftItemSessionStartRow, "practice_session_stars", DefaultStars)
          assertRow[String](redshiftItemSessionStartRow, "academic_year_uuid", "98f50c3c-be1a-4429-abc6-4a64aaa2a001")
          assertRow[String](redshiftItemSessionStartRow, "class_uuid", "75af7199-5fb5-4537-95f9-caf4573573bf")
          assertRow[String](redshiftItemSessionStartRow, "section_uuid", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](redshiftItemSessionStartRow, "subject_uuid", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")

          val redshiftItemSessionEndDf = rsSinks(3).output
          assert(redshiftItemSessionEndDf.columns.toSet === expectedRedshiftColsPItemSession)
          val redshiftItemSessionEndRow = redshiftItemSessionEndDf.first()
          assertRow[Double](redshiftItemSessionEndRow, "practice_session_sa_score", 35.0)
          assertRow[Double](redshiftItemSessionEndRow, "practice_session_score", 10.0)
          assertRow[String](redshiftItemSessionEndRow, "practice_session_id", "practice-id1")
          assertRow[String](redshiftItemSessionEndRow, "student_uuid", "student-id1")
          assertRow[String](redshiftItemSessionEndRow, "practice_session_item_lo_uuid", "lo-id2")
          assertTimestampRow(redshiftItemSessionEndRow, "practice_session_created_time", "2018-09-08 12:40:00.0")
          assertRow[String](redshiftItemSessionEndRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](redshiftItemSessionEndRow, "practice_session_event_type", 2)
          assertRow[Boolean](redshiftItemSessionEndRow, "practice_session_is_start", false)
          assertRow[Boolean](redshiftItemSessionEndRow, "practice_session_is_start_event_processed", false)
          assertRow[Boolean](redshiftItemSessionEndRow, "practice_session_outside_of_school", true)
          assertRow[Int](redshiftItemSessionEndRow, "practice_session_stars", DefaultStars)
          assertRow[String](redshiftItemSessionEndRow, "academic_year_uuid", "98f50c3c-be1a-4429-abc6-4a64aaa2a001")
          assertRow[String](redshiftItemSessionStartRow, "class_uuid", "75af7199-5fb5-4537-95f9-caf4573573bf")
          assertRow[String](redshiftItemSessionStartRow, "section_uuid", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](redshiftItemSessionStartRow, "subject_uuid", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")

          val deltaItemSessionDf = sinks
            .filter(
              s =>
                s.name == DeltaPracticeSessionSink &&
                  s.eventType == s"${FactPracticeSessionEntity}_$practiceItemSessionType")
            .head
            .output
          assert(deltaItemSessionDf.columns.toSet === expectedDeltaColsPItemSession)
          val deltaItemSessionRow = deltaItemSessionDf.first()
          assertRow[Double](deltaItemSessionRow, "practice_session_sa_score", 35.0)
          assertRow[Double](deltaItemSessionRow, "practice_session_score", 10.0)
          assertRow[String](deltaItemSessionRow, "practice_session_id", "practice-id1")
          assertRow[String](deltaItemSessionRow, "practice_session_student_id", "student-id1")
          assertRow[String](deltaItemSessionRow, "practice_session_item_lo_id", "lo-id2")
          assertTimestampRow(deltaItemSessionRow, "practice_session_start_time", "2018-09-08 12:36:00.0")
          assertTimestampRow(deltaItemSessionRow, "practice_session_end_time", "2018-09-08 12:40:00.0")
          assertRow[Long](deltaItemSessionRow, "practice_session_time_spent", 240)
          assertRow[String](deltaItemSessionRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](deltaItemSessionRow, "practice_session_event_type", 2)
          assertRow[Boolean](deltaItemSessionRow, "practice_session_is_start", false)
          assertRow[Boolean](deltaItemSessionRow, "practice_session_outside_of_school", true)
          assertRow[Int](deltaItemSessionRow, "practice_session_stars", DefaultStars)
          assertRow[String](deltaItemSessionRow, "practice_session_academic_year_id", "98f50c3c-be1a-4429-abc6-4a64aaa2a001")
          assertRow[String](deltaItemSessionRow, "practice_session_class_id", "75af7199-5fb5-4537-95f9-caf4573573bf")
          assertRow[String](deltaItemSessionRow, "practice_session_section_id", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](deltaItemSessionRow, "practice_session_subject_id", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")
          assertRow[Boolean](deltaItemSessionRow, "practice_session_is_start_event_processed", false)
          assertRow[String](deltaItemSessionRow, "eventdate", "2018-09-08")
          assertRow[String](deltaSessionsRow, "practice_session_item_content_title", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_content_lesson_type", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_content_location", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_step_id", null)

          val redshiftItemContentSessionStartDf = rsSinks(4).output
          assert(redshiftItemContentSessionStartDf.columns.toSet === expectedRedshiftColsPItemContentSession)
          val redshiftItemContentSessionStartRow = redshiftItemContentSessionStartDf.first()
          assertRow[Double](redshiftItemContentSessionStartRow, "practice_session_sa_score", 35.0)
          assertRow[Double](redshiftItemContentSessionStartRow, "practice_session_score", -1.0)
          assertRow[String](redshiftItemContentSessionStartRow, "practice_session_id", "practice-id1")
          assertRow[String](redshiftItemContentSessionStartRow, "student_uuid", "student-id1")
          assertRow[String](redshiftItemContentSessionStartRow, "practice_session_item_lo_uuid", "lo-id2")
          assertRow[String](redshiftItemContentSessionStartRow, "practice_session_item_step_id", "content-id1")
          assertTimestampRow(redshiftItemContentSessionStartRow, "practice_session_created_time", "2018-09-08 12:37:00.0")
          assertRow[String](redshiftItemContentSessionStartRow, "practice_session_date_dw_id", "20180908")
          assertRow[Int](redshiftItemContentSessionStartRow, "practice_session_event_type", 3)
          assertRow[Boolean](redshiftItemContentSessionStartRow, "practice_session_is_start", true)
          assertRow[Boolean](redshiftItemContentSessionStartRow, "practice_session_is_start_event_processed", false)
          assertRow[Boolean](redshiftItemContentSessionStartRow, "practice_session_outside_of_school", false)
          assertRow[Int](redshiftItemContentSessionStartRow, "practice_session_stars", DefaultStars)
          assertRow[String](redshiftItemContentSessionStartRow, "academic_year_uuid", "03cb533d-f46a-436a-9892-bbf816d7c4fb")
          assertRow[String](redshiftItemContentSessionStartRow, "class_uuid", "75af7199-5fb5-4537-95f9-caf4573573bf")
          assertRow[String](redshiftItemContentSessionStartRow, "section_uuid", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](redshiftItemContentSessionStartRow, "subject_uuid", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")

          val redshiftItemContentSessionEndDf = rsSinks(5).output
          assert(redshiftItemContentSessionEndDf.columns.toSet === expectedRedshiftColsPItemContentSession)
          val redshiftItemContentSessionEndRow = redshiftItemContentSessionEndDf.first()
          assertRow[Double](redshiftItemContentSessionEndRow, "practice_session_sa_score", 35.0)
          assertRow[Double](redshiftItemContentSessionEndRow, "practice_session_score", 25.0)
          assertRow[String](redshiftItemContentSessionEndRow, "practice_session_id", "practice-id1")
          assertRow[String](redshiftItemContentSessionEndRow, "student_uuid", "student-id1")
          assertRow[String](redshiftItemContentSessionEndRow, "practice_session_item_lo_uuid", "lo-id2")
          assertRow[String](redshiftItemContentSessionEndRow, "practice_session_item_step_id", "content-id1")
          assertTimestampRow(redshiftItemContentSessionEndRow, "practice_session_created_time", "2018-09-08 12:57:00.0")
          assertRow[Int](redshiftItemContentSessionEndRow, "practice_session_event_type", 3)
          assertRow[Boolean](redshiftItemContentSessionEndRow, "practice_session_is_start", false)
          assertRow[Boolean](redshiftItemContentSessionEndRow, "practice_session_is_start_event_processed", false)
          assertRow[Boolean](redshiftItemContentSessionEndRow, "practice_session_outside_of_school", true)
          assertRow[Int](redshiftItemContentSessionEndRow, "practice_session_stars", DefaultStars)
          assertRow[String](redshiftItemContentSessionEndRow, "academic_year_uuid", "03cb533d-f46a-436a-9892-bbf816d7c4fb")
          assertRow[String](redshiftItemContentSessionEndRow, "class_uuid", "75af7199-5fb5-4537-95f9-caf4573573bf")
          assertRow[String](redshiftItemContentSessionEndRow, "section_uuid", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](redshiftItemContentSessionEndRow, "subject_uuid", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")

          val deltaItemContentSessionDf = sinks
            .filter(
              s =>
                s.name == DeltaPracticeSessionSink &&
                  s.eventType == s"${FactPracticeSessionEntity}_$practiceItemContentSessionType")
            .head
            .output
          assert(deltaItemContentSessionDf.columns.toSet === expectedDeltaColsPItemContentSession)
          val deltaItemContentSessionRow = deltaItemContentSessionDf.first()
          assertRow[Double](deltaItemContentSessionRow, "practice_session_sa_score", 35.0)
          assertRow[Double](deltaItemContentSessionRow, "practice_session_score", 25.0)
          assertRow[String](deltaItemContentSessionRow, "practice_session_id", "practice-id1")
          assertRow[String](deltaItemContentSessionRow, "practice_session_student_id", "student-id1")
          assertRow[String](deltaItemContentSessionRow, "practice_session_item_lo_id", "lo-id2")
          assertRow[String](deltaItemContentSessionRow, "practice_session_item_step_id", "content-id1")
          assertTimestampRow(deltaItemContentSessionRow, "practice_session_start_time", "2018-09-08 12:37:00.0")
          assertTimestampRow(deltaItemContentSessionRow, "practice_session_end_time", "2018-09-08 12:57:00.0")
          assertRow[Long](deltaItemContentSessionRow, "practice_session_time_spent", 1200)
          assertRow[Int](deltaItemContentSessionRow, "practice_session_event_type", 3)
          assertRow[Boolean](deltaItemContentSessionRow, "practice_session_is_start", false)
          assertRow[Boolean](deltaItemContentSessionRow, "practice_session_outside_of_school", true)
          assertRow[Int](deltaItemContentSessionRow, "practice_session_stars", DefaultStars)
          assertRow[String](deltaItemContentSessionRow, "practice_session_academic_year_id", "03cb533d-f46a-436a-9892-bbf816d7c4fb")
          assertRow[String](deltaItemContentSessionRow, "practice_session_class_id", "75af7199-5fb5-4537-95f9-caf4573573bf")
          assertRow[String](deltaItemContentSessionRow, "practice_session_section_id", "7df52e2e-a394-4d22-be58-83497631d256")
          assertRow[String](deltaItemContentSessionRow, "practice_session_subject_id", "b1dbc9db-2cbe-48a7-9a0b-3a6de6119a5a")
          assertRow[Boolean](deltaItemContentSessionRow, "practice_session_is_start_event_processed", false)
          assertRow[String](deltaItemContentSessionRow, "eventdate", "2018-09-08")
          assertRow[String](deltaSessionsRow, "practice_session_item_content_title", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_content_lesson_type", null)
          assertRow[String](deltaSessionsRow, "practice_session_item_content_location", null)
        }
      )
    }
  }

}
