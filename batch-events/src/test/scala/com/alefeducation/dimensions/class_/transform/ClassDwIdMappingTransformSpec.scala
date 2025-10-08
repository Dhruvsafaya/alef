package com.alefeducation.dimensions.class_.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Helpers.ParquetClassModifiedSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class ClassDwIdMappingTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set("id", "entity_created_time", "entity_dw_created_time", "entity_type")

  test("Class Dw Id Mapping should be transformed") {
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

    val transformer = new ClassDwIdMappingTransform(sprk, service)

    when(
      service.readOptional(ParquetClassModifiedSource, sprk)
    ).thenReturn(
      Some(
        spark.read.json(Seq(classModified).toDS())
      )
    )
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    assert[String](df, "id", "class1")
    assert[String](df, "entity_created_time", "2019-01-08 02:40:00.0")
    assert[String](df, "entity_type", "class")
  }
}
