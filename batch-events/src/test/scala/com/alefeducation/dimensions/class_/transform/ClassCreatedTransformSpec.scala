package com.alefeducation.dimensions.class_.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.class_.transform.ClassLatestEventsTransform.classLatestEventsTransformSink
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class ClassCreatedTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "rel_class_dw_id",
    "class_created_time",
    "class_dw_created_time",
    "class_updated_time",
    "class_dw_updated_time",
    "class_deleted_time",
    "class_id",
    "class_title",
    "class_school_id",
    "class_grade_id",
    "class_section_id",
    "class_academic_year_id",
    "class_gen_subject",
    "class_curriculum_id",
    "class_curriculum_grade_id",
    "class_curriculum_subject_id",
    "class_content_academic_year",
    "class_curriculum_instructional_plan_id",
    "class_material_id",
    "class_material_type",
    "class_tutor_dhabi_enabled",
    "class_language_direction",
    "class_online",
    "class_practice",
    "class_course_status",
    "class_source_id",
    "class_status",
    "class_active_until",
    "class_category_id",
    "class_academic_calendar_id"
  )

  test("Class Created should be transformed") {
    val classLatestEvents =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ClassCreatedEvent",
        |  "classId": "class1",
        |  "title": "testClass",
        |  "schoolId": "school1",
        |  "gradeId": "grade1",
        |  "sectionId": "section1",
        |  "academicYearId": "ay1",
        |  "academicCalendarId": "ac1",
        |  "subjectCategory": "Art",
        |  "material": {
        |    "curriculum": 392027,
        |    "grade": 363684,
        |    "subject": 658224,
        |    "year": 2020,
        |    "instructionalPlanId": "f92c4dda-0ad9-464b-a005-67d581ada1ed",
        |    "materialId": "test_material_id",
        |    "materialType": "test_material_type"
        |  },
        |  "settings": {
        |    "tutorDhabiEnabled": true,
        |    "languageDirection": "LTR",
        |    "online": true,
        |    "practice": true
        |  },
        |  "teachers": [
        |    "70a24bfe-27c2-4600-b1f5-1f9db2a38e9f",
        |    "753005c6-24d2-4b21-9335-60ce8e15fee0"
        |  ],
        |  "status": "ACTIVE",
        |  "sourceId": "909",
        |  "occurredOn": "2019-01-08 02:40:00.0",
        |  "categoryId": "a92c4dda-0ad9-464b-a005-67d581ad01ed",
        |  "organisationGlobal": "6"
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ClassDeletedEvent",
        |  "classId": "class1",
        |  "title": null,
        |  "schoolId": null,
        |  "gradeId": null,
        |  "sectionId": null,
        |  "academicYearId": null,
        |  "subjectCategory": null,
        |  "material": null,
        |  "settings": null,
        |  "teachers": null,
        |  "status": null,
        |  "sourceId": null,
        |  "occurredOn": "2019-01-08 02:40:00.0",
        |  "categoryId": null,
        |  "organisationGlobal": null
        |}
        |]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val transformer = new ClassCreatedTransform(sprk, service)

    when(
      service.readOptional(classLatestEventsTransformSink, sprk)
    ).thenReturn(
      Some(
        spark.read.json(Seq(classLatestEvents).toDS())
      )
    )
    when(
      service.getStartIdUpdateStatus("dim_class")
    ).thenReturn(
      1
    )

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    assert[String](df, "class_id", "class1")
    assert[String](df, "class_title", "testClass")
    assert[String](df, "class_school_id", "school1")
    assert[Long](df, "rel_class_dw_id", 1)
    assert[String](df, "class_active_until", null)
    assert[Long](df, "class_curriculum_id", 392027)
    assert[Long](df, "class_curriculum_grade_id", 363684)
    assert[Long](df, "class_curriculum_subject_id", 658224)
    assert[String](df, "class_course_status", "ACTIVE")
    assert[String](df, "class_created_time", "2019-01-08 02:40:00.0")
  }

}
