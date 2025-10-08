package com.alefeducation.facts

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Helpers._

class PracticeSpec extends SparkSuite {

  trait Setup {
    implicit val transformer = new PracticeTransformer(PracticeService, spark)
  }

  val expectedColumns = Set(
    "practice_created_time",
    "practice_dw_created_time",
    "practice_date_dw_id",
    "practice_id",
    "lo_uuid",
    "student_uuid",
    "subject_uuid",
    "grade_uuid",
    "tenant_uuid",
    "school_uuid",
    "section_uuid",
    "skill_uuid",
    "practice_sa_score",
    "item_lo_uuid",
    "item_skill_uuid",
    "practice_item_step_id",
    "practice_item_content_title",
    "practice_item_content_lesson_type",
    "practice_item_content_location",
    "academic_year_uuid",
    "practice_instructional_plan_id",
    "practice_learning_path_id",
    "class_uuid",
    "practice_material_id",
    "practice_material_type"
  )

  val expectedDeltaColumns = Set(
    "practice_tenant_id",
    "practice_id",
    "practice_student_id",
    "practice_school_id",
    "practice_grade_id",
    "practice_subject_id",
    "practice_section_id",
    "practice_lo_id",
    "practice_skill_id",
    "practice_sa_score",
    "practice_item_lo_id",
    "practice_item_skill_id",
    "practice_item_step_id",
    "practice_item_content_title",
    "practice_item_content_lesson_type",
    "practice_item_content_location",
    "practice_academic_year_id",
    "practice_instructional_plan_id",
    "practice_learning_path_id",
    "practice_class_id",
    "practice_created_time",
    "practice_dw_created_time",
    "practice_date_dw_id",
    "eventdate",
    "practice_material_id",
    "practice_material_type"
  )

  test("transform practice created events successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetPracticeSource,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "PracticeCreatedEvent",
              |  "id":"practiceId",
              |  "occurredOn": "2018-09-08 02:38:00.0",
              |  "schoolId": "school-id",
              |  "learnerId": "student-id",
              |  "gradeId": "school-grade-id",
              |  "sectionId": "c4aae384-befc-443a-82de-196753f2c88a",
              |  "subjectId": "75af7199-5fb5-4537-95f9-caf4573573bf",
              |  "learningObjectiveId": "learning-objective-id",
              |  "skills": [{
              |               "uuid": "skill-uuid-1",
              |               "code": "skill-code-1",
              |               "name": "skill-name-1"
              |             }],
              |  "saScore": 24.5,
              |  "items": [
              |            {
              |              "learningObjectiveId": "item-lo",
              |              "skills": [
              |                           {
              |                           "uuid": "skill-uuid-3",
              |                           "code": "skill-code-3",
              |                           "name": "skill-name-3"
              |                           },
              |                           {
              |                           "uuid": "skill-uuid-4",
              |                           "code": "skill-code-4",
              |                           "name": "skill-name-4"
              |                           }
              |                     ],
              |              "contents": [
              |                            {
              |                              "id": "con1",
              |                              "title": "content-title-1",
              |                              "lessonType": "lesson-type-1",
              |                              "location": "http://location1"
              |                             },
              |                             {
              |                              "id": "con2",
              |                              "title": "content-title-2",
              |                              "lessonType": "lesson-type-2",
              |                              "location": "http://location2"
              |                             }
              |                            ]
              |             }
              |           ],
              |  "academicYearId": "5394b28f-65b4-427e-83f6-b5e9264c392d",
              |  "eventDateDw": "20181029",
              |  "instructionalPlanId": "instructionalPlanId",
              |  "learningPathId": "learningPathId",
              |  "classId": "a6749ea5-6657-4816-9bba-def8ac525515",
              |  "materialId": "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f",
              |  "materialType": "CORE"
              |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 3)
          val df = sinks.filter(_.name == RedshiftPracticeSink).head.output

          assert(df.count === 4)
          assert(df.columns.toSet === expectedColumns)

          val df1 = df.filter("item_skill_uuid = 'skill-uuid-3' and practice_item_step_id = 'con1'")
          assert[String](df1, "school_uuid", "school-id")
          assert[String](df1, "student_uuid", "student-id")
          assert[String](df1, "skill_uuid", "skill-uuid-1")
          assert[Double](df1, "practice_sa_score", 24.5)
          assert[String](df1, "item_lo_uuid", "item-lo")
          assert[String](df1, "practice_item_content_location", "http://location1")
          assert[String](df1, "practice_date_dw_id", "20181029")
          assert[String](df1, "academic_year_uuid", "5394b28f-65b4-427e-83f6-b5e9264c392d")
          assert[String](df1, "class_uuid", "a6749ea5-6657-4816-9bba-def8ac525515")
          assert[String](df1, "subject_uuid", "75af7199-5fb5-4537-95f9-caf4573573bf")
          assert[String](df1, "section_uuid", "c4aae384-befc-443a-82de-196753f2c88a")
          assert[String](df1, "practice_material_id", "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f")
          assert[String](df1, "practice_material_type", "CORE")

          val df2 = df.filter("item_skill_uuid = 'skill-uuid-3' and practice_item_step_id = 'con2'")
          assert[String](df2, "school_uuid", "school-id")
          assert[String](df2, "student_uuid", "student-id")
          assert[String](df2, "skill_uuid", "skill-uuid-1")
          assert[Double](df2, "practice_sa_score", 24.5)
          assert[String](df2, "item_lo_uuid", "item-lo")
          assert[String](df2, "practice_item_content_location", "http://location2")
          assert[String](df2, "practice_date_dw_id", "20181029")
          assert[String](df2, "academic_year_uuid", "5394b28f-65b4-427e-83f6-b5e9264c392d")
          assert[String](df2, "class_uuid", "a6749ea5-6657-4816-9bba-def8ac525515")
          assert[String](df2, "subject_uuid", "75af7199-5fb5-4537-95f9-caf4573573bf")
          assert[String](df2, "section_uuid", "c4aae384-befc-443a-82de-196753f2c88a")
          assert[String](df2, "practice_material_id", "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f")
          assert[String](df2, "practice_material_type", "CORE")

          val df3 = df.filter("item_skill_uuid = 'skill-uuid-4' and practice_item_step_id = 'con2'")
          assert[String](df3, "school_uuid", "school-id")
          assert[String](df3, "student_uuid", "student-id")
          assert[String](df3, "skill_uuid", "skill-uuid-1")
          assert[Double](df3, "practice_sa_score", 24.5)
          assert[String](df3, "item_lo_uuid", "item-lo")
          assert[String](df3, "practice_item_content_location", "http://location2")
          assert[String](df3, "practice_date_dw_id", "20181029")
          assert[String](df3, "academic_year_uuid", "5394b28f-65b4-427e-83f6-b5e9264c392d")
          assert[String](df3, "class_uuid", "a6749ea5-6657-4816-9bba-def8ac525515")
          assert[String](df3, "subject_uuid", "75af7199-5fb5-4537-95f9-caf4573573bf")
          assert[String](df3, "section_uuid", "c4aae384-befc-443a-82de-196753f2c88a")
          assert[String](df3, "practice_material_id", "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f")
          assert[String](df3, "practice_material_type", "CORE")

          val df4 = df.filter("item_skill_uuid = 'skill-uuid-4' and practice_item_step_id = 'con1'")
          assert[String](df4, "school_uuid", "school-id")
          assert[String](df4, "student_uuid", "student-id")
          assert[String](df4, "skill_uuid", "skill-uuid-1")
          assert[Double](df4, "practice_sa_score", 24.5)
          assert[String](df4, "item_lo_uuid", "item-lo")
          assert[String](df4, "practice_item_content_location", "http://location1")
          assert[String](df4, "practice_date_dw_id", "20181029")
          assert[String](df4, "academic_year_uuid", "5394b28f-65b4-427e-83f6-b5e9264c392d")
          assert[String](df4, "class_uuid", "a6749ea5-6657-4816-9bba-def8ac525515")
          assert[String](df4, "subject_uuid", "75af7199-5fb5-4537-95f9-caf4573573bf")
          assert[String](df4, "section_uuid", "c4aae384-befc-443a-82de-196753f2c88a")
          assert[String](df4, "practice_material_id", "1cc2c43a-2ef3-11ef-a40a-f623e7efc61f")
          assert[String](df4, "practice_material_type", "CORE")

          val deltaDf = sinks.filter(_.name == DeltaPracticeSink).head.output
          assert(deltaDf.columns.toSet === expectedDeltaColumns)
        }
      )
    }
  }
}
