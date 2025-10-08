package com.alefeducation.dimensions.class_.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.class_.transform.ClassLatestEventsTransform.classLatestEventsTransformSink
import com.alefeducation.models.ClassModel.DimClass
import com.alefeducation.util.Helpers.{ParquetClassDeletedSource, ParquetClassModifiedSource}
import com.alefeducation.util.Resources
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class ClassModifiedTransformSpec extends SparkSuite {
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

  test("Class Updated should be transformed") {
    val classLatest =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ClassUpdatedEvent",
        |  "classId": "class1",
        |  "title": "testClass2",
        |  "schoolId": "school1",
        |  "gradeId": "grade1",
        |  "sectionId": "section1",
        |  "academicYearId": "ay1",
        |  "academicCalendarId": "ac1",
        |  "subjectCategory": "Art2",
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
        |  "occurredOn": "2019-01-12 00:00:00.0",
        |  "categoryId": "a92c4dda-0ad9-464b-a005-67d581ad01ed",
        |  "organisationGlobal": "6"
        |}
        |]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val transformer = new ClassModifiedTransform(sprk, service)

    when(
      service.readOptional(classLatestEventsTransformSink, sprk)
    ).thenReturn(
      Some(
        spark.read.json(Seq(classLatest).toDS())
      )
    )
    when(
      service.readOptional(ParquetClassModifiedSource, sprk, extraProps = List(("mergeSchema", "true")))
    ).thenReturn(
      None
    )
    when(
      service.readOptional(ParquetClassDeletedSource, sprk)
    ).thenReturn(
      None
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
    assert[String](df, "class_title", "testClass2")
    assert[String](df, "class_school_id", "school1")
    assert[Long](df, "rel_class_dw_id", 1)
    assert[String](df, "class_active_until", null)
    assert[Long](df, "class_curriculum_id", 392027)
    assert[Long](df, "class_curriculum_grade_id", 363684)
    assert[Long](df, "class_curriculum_subject_id", 658224)
    assert[String](df, "class_course_status", "ACTIVE")
    assert[Int](df, "class_status", 1)
    assert[String](df, "class_created_time", "2019-01-12 00:00:00.0")
  }

  test("Class Deleted should be transformed when created data is in redshift") {
    val classLatest =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ClassDeletedEvent",
        |  "classId": "class1",
        |  "academicCalendarId": null,
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
        |  "occurredOn": "2019-01-12 00:00:00.0",
        |  "categoryId": null,
        |  "organisationGlobal": null
        |}
        |]
      """.stripMargin

    val classDeleted =
      """
        |[
        |{
        | "tenantId": "tenant-id",
        | "eventType": "ClassDeletedEvent",
        | "classId": "class1",
        | "occurredOn": "2019-01-12 00:00:00.0"
        |}
        |]""".stripMargin

    val classRedshift =
      """
        |[
        |{
        | "class_id": "class1",
        | "class_title": "testClassRedsh",
        | "class_school_id": "schoolId",
        | "class_grade_id": "gradeId",
        | "class_section_id": "sectionId",
        | "class_academic_year_id": "ayId",
        | "class_academic_calendar_id": "acId",
        | "class_gen_subject": "subj",
        | "class_curriculum_id": 228,
        | "class_curriculum_grade_id": 228,
        | "class_curriculum_subject_id": 228,
        | "class_content_academic_year": 228,
        | "class_tutor_dhabi_enabled": true,
        | "class_language_direction": "there",
        | "class_online": true,
        | "class_practice": true,
        | "class_course_status": "ACTIVE",
        | "class_source_id": "sourceId",
        | "class_curriculum_instructional_plan_id": "ipId",
        | "class_category_id": "categoryId",
        | "class_material_id": "materialId",
        | "class_material_type": "materialType",
        | "class_created_time": "2019-01-10 00:00:00.0",
        | "class_status": 1
        |}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val transformer = new ClassModifiedTransform(sprk, service)

    when(
      service.readOptional(classLatestEventsTransformSink, sprk)
    ).thenReturn(
      Some(
        spark.read.json(Seq(classLatest).toDS())
      )
    )
    when(
      service.readOptional(ParquetClassModifiedSource, sprk, extraProps = List(("mergeSchema", "true")))
    ).thenReturn(
      None
    )
    when(
      service.readOptional(ParquetClassDeletedSource, sprk)
    ).thenReturn(
      Some(
        spark.read.json(Seq(classDeleted).toDS())
      )
    )
    when(
      service.getStartIdUpdateStatus("dim_class")
    ).thenReturn(
      1
    )
    when(
      service.readFromRedshiftQuery[DimClass](
        s"select * from ${Resources.redshiftStageSchema()}.rel_class where class_id in ('class1') and class_active_until is null"
      )
    ).thenReturn(
      Seq.empty[DimClass].toDF()
    )
    when(
      service.readFromRedshiftQuery[DimClass](
        s"select * from ${Resources.redshiftSchema()}.dim_class where class_id in ('class1') and class_active_until is null"
      )
    ).thenReturn(
      spark.read.json(Seq(classRedshift).toDS())
    )

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    assert[String](df, "class_id", "class1")
    assert[String](df, "class_title", "testClassRedsh")
    assert[String](df, "class_school_id", "schoolId")
    assert[Long](df, "rel_class_dw_id", 1)
    assert[String](df, "class_active_until", null)
    assert[Long](df, "class_curriculum_id", 228)
    assert[Long](df, "class_curriculum_grade_id", 228)
    assert[Long](df, "class_curriculum_subject_id", 228)
    assert[String](df, "class_course_status", "ACTIVE")
    assert[Int](df, "class_status", 4)
    assert[String](df, "class_created_time", "2019-01-12 00:00:00.0")
  }

  test("Class Deleted should be transformed when created data is in incoming event") {
    val classModified =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ClassCreatedEvent",
        |  "classId": "class1",
        |  "title": "testClass",
        |  "schoolId": "schoolId",
        |  "gradeId": "grade1",
        |  "sectionId": "section1",
        |  "academicYearId": "ay1",
        |  "academicCalendarId": "ac1",
        |  "subjectCategory": "Art",
        |  "material": {
        |    "curriculum": 228,
        |    "grade": 228,
        |    "subject": 228,
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
        |  "eventType": "ClassUpdatedEvent",
        |  "classId": "class2",
        |  "title": "testClassUpd",
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
        |  "occurredOn": "2019-01-10 00:00:00.0",
        |  "categoryId": "a92c4dda-0ad9-464b-a005-67d581ad01ed",
        |  "organisationGlobal": "6"
        |}
        |]
      """.stripMargin

    val classLatest =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ClassUpdatedEvent",
        |  "classId": "class2",
        |  "title": "testClassUpd",
        |  "schoolId": "school1",
        |  "gradeId": "grade1",
        |  "sectionId": "section1",
        |  "academicYearId": "ay1",
        |  "academicCalendarId": "ac1",
        |  "subjectCategory": "Art2",
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
        |  "occurredOn": "2019-01-12 00:00:00.0",
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
        |  "academicCalendarId": null,
        |  "subjectCategory": null,
        |  "material": null,
        |  "settings": null,
        |  "teachers": null,
        |  "status": null,
        |  "sourceId": null,
        |  "occurredOn": "2019-01-12 00:01:00.0",
        |  "categoryId": null,
        |  "organisationGlobal": null
        |}
        |]
      """.stripMargin

    val classDeleted =
      """
        |[
        |{
        | "tenantId": "tenant-id",
        | "eventType": "ClassDeletedEvent",
        | "classId": "class1",
        | "occurredOn": "2019-01-12 00:00:00.0"
        |}
        |]""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val transformer = new ClassModifiedTransform(sprk, service)

    when(
      service.readOptional(classLatestEventsTransformSink, sprk)
    ).thenReturn(
      Some(
        spark.read.json(Seq(classLatest).toDS())
      )
    )
    when(
      service.readOptional(ParquetClassModifiedSource, sprk, extraProps = List(("mergeSchema", "true")))
    ).thenReturn(
      Some(
        spark.read.json(Seq(classModified).toDS())
      )
    )
    when(
      service.readOptional(ParquetClassDeletedSource, sprk)
    ).thenReturn(
      Some(
        spark.read.json(Seq(classDeleted).toDS())
      )
    )
    when(
      service.getStartIdUpdateStatus("dim_class")
    ).thenReturn(
      1
    )
    when(
      service.readFromRedshiftQuery[DimClass](
        s"select * from ${Resources.redshiftStageSchema()}.rel_class where class_id in ('class1') and class_active_until is null"
      )
    ).thenReturn(
      Seq.empty[DimClass].toDF()
    )
    when(
      service.readFromRedshiftQuery[DimClass](
        s"select * from ${Resources.redshiftSchema()}.dim_class where class_id in ('class1') and class_active_until is null"
      )
    ).thenReturn(
      Seq.empty[DimClass].toDF()
    )

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    val updated = df.filter($"class_id" === "class2")
    assert[String](updated, "class_title", "testClassUpd")
    assert[String](updated, "class_school_id", "school1")
    assert[Long](updated, "rel_class_dw_id", 1)
    assert[String](updated, "class_active_until", null)
    assert[Long](updated, "class_curriculum_id", 392027)
    assert[Long](updated, "class_curriculum_grade_id", 363684)
    assert[Long](updated, "class_curriculum_subject_id", 658224)
    assert[String](updated, "class_course_status", "ACTIVE")
    assert[Int](updated, "class_status", 1)
    assert[String](updated, "class_created_time", "2019-01-12 00:00:00.0")

    val deleted = df.filter($"class_id" === "class1")
    assert[String](deleted, "class_title", "testClass")
    assert[String](deleted, "class_school_id", "schoolId")
    assert[Long](deleted, "rel_class_dw_id", 2)
    assert[String](deleted, "class_active_until", null)
    assert[Long](deleted, "class_curriculum_id", 228)
    assert[Long](deleted, "class_curriculum_grade_id", 228)
    assert[Long](deleted, "class_curriculum_subject_id", 228)
    assert[String](deleted, "class_course_status", "ACTIVE")
    assert[Int](deleted, "class_status", 4)
    assert[String](deleted, "class_created_time", "2019-01-12 00:01:00.0")
  }

  test("Class Deleted should be transformed when created data is in redshift and in updated event") {
    val classModified =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ClassUpdatedEvent",
        |  "classId": "class2",
        |  "title": "testClassUpd",
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
        |  "occurredOn": "2019-01-10 00:00:00.0",
        |  "categoryId": "a92c4dda-0ad9-464b-a005-67d581ad01ed",
        |  "organisationGlobal": "6"
        |}
        |]
      """.stripMargin

    val classLatest =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "ClassDeletedEvent",
        |  "classId": "class2",
        |  "title": null,
        |  "schoolId": null,
        |  "gradeId": null,
        |  "sectionId": null,
        |  "academicYearId": null,
        |  "academicCalendarId": null,
        |  "subjectCategory": null,
        |  "material": null,
        |  "settings": null,
        |  "teachers": null,
        |  "status": null,
        |  "sourceId": null,
        |  "occurredOn": "2019-01-12 00:01:00.0",
        |  "categoryId": null,
        |  "organisationGlobal": null
        |}
        |]
      """.stripMargin

    val classDeleted =
      """
        |[
        |{
        | "tenantId": "tenant-id",
        | "eventType": "ClassDeletedEvent",
        | "classId": "class1",
        | "occurredOn": "2019-01-12 00:01:00.0"
        |}
        |]""".stripMargin

    val classRedshift =
      """
        |[
        |{
        | "class_id": "class1",
        | "class_title": "testClassRedsh",
        | "class_school_id": "schoolId",
        | "class_grade_id": "gradeId",
        | "class_section_id": "sectionId",
        | "class_academic_year_id": "ayId",
        | "class_academic_calendar_id": "acId",
        | "class_gen_subject": "subj",
        | "class_curriculum_id": 228,
        | "class_curriculum_grade_id": 228,
        | "class_curriculum_subject_id": 228,
        | "class_content_academic_year": 228,
        | "class_tutor_dhabi_enabled": true,
        | "class_language_direction": "there",
        | "class_online": true,
        | "class_practice": true,
        | "class_course_status": "ACTIVE",
        | "class_source_id": "sourceId",
        | "class_curriculum_instructional_plan_id": "ipId",
        | "class_category_id": "categoryId",
        | "class_material_id": "materialId",
        | "class_material_type": "materialType",
        | "class_created_time": "2019-01-10 00:00:00.0",
        | "class_status": 1
        |}
        |]
        |""".stripMargin


    val sprk = spark
    import sprk.implicits._

    val transformer = new ClassModifiedTransform(sprk, service)

    when(
      service.readOptional(classLatestEventsTransformSink, sprk)
    ).thenReturn(
      Some(
        spark.read.json(Seq(classLatest).toDS())
      )
    )
    when(
      service.readOptional(ParquetClassModifiedSource, sprk, extraProps = List(("mergeSchema", "true")))
    ).thenReturn(
      Some(
        spark.read.json(Seq(classModified).toDS())
      )
    )
    when(
      service.readOptional(ParquetClassDeletedSource, sprk)
    ).thenReturn(
      Some(
        spark.read.json(Seq(classDeleted).toDS())
      )
    )
    when(
      service.getStartIdUpdateStatus("dim_class")
    ).thenReturn(
      1
    )
    when(
      service.readFromRedshiftQuery[DimClass](
        s"select * from ${Resources.redshiftStageSchema()}.rel_class where class_id in ('class1') and class_active_until is null"
      )
    ).thenReturn(
      Seq.empty[DimClass].toDF()
    )
    when(
      service.readFromRedshiftQuery[DimClass](
        s"select * from ${Resources.redshiftSchema()}.dim_class where class_id in ('class1') and class_active_until is null"
      )
    ).thenReturn(
      spark.read.json(Seq(classRedshift).toDS())
    )


    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    assert[String](df, "class_id", "class2")
    assert[String](df, "class_title", "testClassUpd")
    assert[String](df, "class_school_id", "school1")
    assert[Long](df, "rel_class_dw_id", 1)
    assert[String](df, "class_active_until", null)
    assert[Long](df, "class_curriculum_id", 392027)
    assert[Long](df, "class_curriculum_grade_id", 363684)
    assert[Long](df, "class_curriculum_subject_id", 658224)
    assert[String](df, "class_course_status", "ACTIVE")
    assert[Int](df, "class_status", 4)
    assert[String](df, "class_created_time", "2019-01-12 00:01:00.0")
  }
}
