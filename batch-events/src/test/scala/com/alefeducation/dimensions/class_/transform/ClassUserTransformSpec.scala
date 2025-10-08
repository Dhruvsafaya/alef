package com.alefeducation.dimensions.class_.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Helpers.{ParquetClassModifiedSource, ParquetStudentClassAssociationSource}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class ClassUserTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "eventType",
    "role_uuid",
    "class_user_active_until",
    "class_user_status",
    "user_uuid",
    "class_uuid",
    "class_user_created_time",
    "class_user_dw_created_time",
    "class_user_updated_time",
    "class_user_deleted_time",
    "class_user_dw_updated_time",
    "class_user_attach_status",
    "rel_class_user_dw_id",
  )

  test("Class User should be transformed") {
    val classStudentAssociated =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "StudentEnrolledInClassEvent",
        |  "classId": "class1",
        |  "userId": "user1",
        |  "occurredOn": "2019-01-12 00:00:00.0"
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "StudentUnenrolledFromClassEvent",
        |  "classId": "class1",
        |  "userId": "user1",
        |  "occurredOn": "2019-01-13 00:00:00.0"
        |}
        |]
        |""".stripMargin

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
        |  ],
        |  "status": "ACTIVE",
        |  "sourceId": "909",
        |  "occurredOn": "2019-01-10 00:00:00.0",
        |  "categoryId": "a92c4dda-0ad9-464b-a005-67d581ad01ed",
        |  "organisationGlobal": "6"
        |}
        |]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val transformer = new ClassUserTransform(sprk, service)

    when(
      service.readOptional(ParquetStudentClassAssociationSource, sprk)
    ).thenReturn(
      Some(
        spark.read.json(Seq(classStudentAssociated).toDS())
      )
    )
    when(
      service.readOptional(ParquetClassModifiedSource, sprk)
    ).thenReturn(
      Some(
        spark.read.json(Seq(classModified).toDS())
      )
    )
    when(
      service.getStartIdUpdateStatus("dim_class_user")
    ).thenReturn(
      1
    )

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 4)

    val studentUnenrolled = df.filter($"eventType" === "StudentUnenrolledFromClassEvent")
    assert[String](studentUnenrolled, "role_uuid", "STUDENT")
    assert[Int](studentUnenrolled, "class_user_status", 1)
    assert[String](studentUnenrolled, "user_uuid", "user1")
    assert[String](studentUnenrolled, "class_uuid", "class1")
    assert[String](studentUnenrolled, "class_user_created_time", "2019-01-13 00:00:00.0")
    assert[Int](studentUnenrolled, "class_user_attach_status", 2)
    assert[Int](studentUnenrolled, "rel_class_user_dw_id", 4)

    val classUpdated = df.filter($"class_uuid" === "class2")
    assert[String](classUpdated, "role_uuid", "TEACHER")
    assert[Int](classUpdated, "class_user_status", 1)
    assert[String](classUpdated, "user_uuid", null)
    assert[String](classUpdated, "class_user_created_time", "2019-01-10 00:00:00.0")
    assert[Int](classUpdated, "class_user_attach_status", 2)
    assert[Int](classUpdated, "rel_class_user_dw_id", 3)
  }
}
