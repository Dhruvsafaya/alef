package com.alefeducation.dimensions.class_.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Helpers.{ParquetClassDeletedSource, ParquetClassModifiedSource}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.IntegerType
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class ClassLatestEventsTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "tenantId",
    "eventType",
    "classId",
    "title",
    "schoolId",
    "gradeId",
    "sectionId",
    "academicYearId",
    "academicCalendarId",
    "subjectCategory",
    "material",
    "settings",
    "teachers",
    "status",
    "sourceId",
    "occurredOn",
    "categoryId",
    "organisationGlobal"
  )

  test("Class Modified and Deleted should be transformed to latest") {
    val classModified =
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
        |    "curriculum": "392027",
        |    "grade": "363684",
        |    "subject": "658224",
        |    "year": 2020,
        |    "instructionalPlanId": "f92c4dda-0ad9-464b-a005-67d581ada1ed",
        |    "materialId": "test_material_id",
        |    "materialType": "test_material_type"
        |  },
        |  "settings": {
        |    "tutorDhabiEnabled": true,
        |    "languageDirection": "LTR",
        |    "online": true,
        |    "practice": true,
        |    "studentProgressView": null
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
        |  "eventType": "ClassCreatedEvent",
        |  "classId": "class2",
        |  "title": "testClass",
        |  "schoolId": "school1",
        |  "gradeId": "grade1",
        |  "sectionId": "section1",
        |  "academicYearId": "ay1",
        |  "subjectCategory": "Art",
        |  "material": {
        |    "curriculum": "392027",
        |    "grade": "363684",
        |    "subject": "658224",
        |    "year": 2020,
        |    "instructionalPlanId": "f92c4dda-0ad9-464b-a005-67d581ada1ed",
        |    "materialId": "test_material_id",
        |    "materialType": "test_material_type"
        |  },
        |  "settings": {
        |    "tutorDhabiEnabled": true,
        |    "languageDirection": "LTR",
        |    "online": true,
        |    "practice": true,
        |    "studentProgressView": null
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
        |
        |]
      """.stripMargin

    val classDeleted =
      """
        |[
        |{
        | "tenantId": "tenant-id",
        | "eventType": "ClassDeletedEvent", 
        | "classId": "class1", 
        | "occurredOn": "2019-01-09 02:42:00.0"
        |}
        |]""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val transformer = new ClassLatestEventsTransform(sprk, service)

    when(
      service.readOptional(ParquetClassModifiedSource, sprk, extraProps = List(("mergeSchema", "true")))
    ).thenReturn(
      Some(
        spark.read
          .json(Seq(classModified).toDS())
          .withColumn(
            "material",
            struct(
              $"material.curriculum",
              $"material.grade",
              $"material.subject",
              $"material.year".cast(IntegerType).alias("year"),
              $"material.instructionalPlanId",
              $"material.materialId",
              $"material.materialType"
            )
          )
          .withColumn(
            "settings",
            struct("settings.tutorDhabiEnabled", "settings.languageDirection", "settings.online", "settings.practice", "settings.studentProgressView")
          )
      )
    )
    when(
      service.readOptional(ParquetClassDeletedSource, sprk)
    ).thenReturn(
      Some(
        spark.read.json(Seq(classDeleted).toDS())
      )
    )

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    val deleted = df.filter($"eventType" === "ClassDeletedEvent")
    assert[String](deleted, "classId", "class1")
    assert[String](deleted, "occurredOn", "2019-01-09 02:42:00.0")
    assert[String](deleted, "title", null)
    assert[String](deleted, "material", null)
  }

  test("Class Modified should be transformed to latest") {
    val classModified =
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
        |    "curriculum": "392027",
        |    "grade": "363684",
        |    "subject": "658224",
        |    "year": 2020,
        |    "instructionalPlanId": "f92c4dda-0ad9-464b-a005-67d581ada1ed",
        |    "materialId": "test_material_id",
        |    "materialType": "test_material_type"
        |  },
        |  "settings": {
        |    "tutorDhabiEnabled": true,
        |    "languageDirection": "LTR",
        |    "online": true,
        |    "practice": true,
        |    "studentProgressView": null
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
        |  "eventType": "ClassCreatedEvent",
        |  "classId": "class1",
        |  "title": "testClass",
        |  "schoolId": "school1",
        |  "gradeId": "grade1",
        |  "sectionId": "section1",
        |  "academicYearId": "ay1",
        |  "subjectCategory": "Art2",
        |  "material": {
        |    "curriculum": "392027",
        |    "grade": "363684",
        |    "subject": "658224",
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
        |
        |]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val transformer = new ClassLatestEventsTransform(sprk, service)

    when(
      service.readOptional(ParquetClassModifiedSource, sprk, extraProps = List(("mergeSchema", "true")))
    ).thenReturn(
      Some(
        spark.read.json(Seq(classModified).toDS())
          .withColumn(
            "material",
            struct(
              $"material.curriculum",
              $"material.grade",
              $"material.subject",
              $"material.year".cast(IntegerType).alias("year"),
              $"material.instructionalPlanId",
              $"material.materialId",
              $"material.materialType"
            )
          )
          .withColumn(
            "settings",
            struct("settings.tutorDhabiEnabled", "settings.languageDirection", "settings.online", "settings.practice")
          )
      )
    )
    when(
      service.readOptional(ParquetClassDeletedSource, sprk)
    ).thenReturn(
      None
    )

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    assert[String](df, "classId", "class1")
    assert[String](df, "occurredOn", "2019-01-10 00:00:00.0")
    assert[String](df, "title", "testClass")
    assert[String](df, "subjectCategory", "Art2")
  }

  test("Class Deleted should be transformed to latest") {
    val classDeleted =
      """
        |[
        |{
        | "tenantId": "tenant-id",
        | "eventType": "ClassDeletedEvent",
        | "classId": "class1",
        | "occurredOn": "2019-01-10 00:00:00.0"
        |}
        |]""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val transformer = new ClassLatestEventsTransform(sprk, service)

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

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    val deleted = df.filter($"eventType" === "ClassDeletedEvent")
    assert[String](deleted, "classId", "class1")
    assert[String](deleted, "occurredOn", "2019-01-10 00:00:00.0")
    assert[String](deleted, "title", null)
    assert[String](deleted, "material", null)
  }

}
