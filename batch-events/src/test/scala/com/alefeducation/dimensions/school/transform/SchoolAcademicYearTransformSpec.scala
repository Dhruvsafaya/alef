package com.alefeducation.dimensions.school.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.school.transform.SchoolAcademicYearTransform.{
  SchoolAcademicYearRollOverCompletedSource,
  SchoolAcademicYearSwitchedSource,
  SchoolMutatedSource
}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class SchoolAcademicYearTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("SchoolAcademicYearTransform should transform when created, switched and rolled over is present") {
    val schoolMutatedValue =
      """
        |[
        |{
        |  "eventType": "SchoolCreatedEvent",
        |  "uuid": "schoolId",
        |  "name": "school-name-1",
        |  "addressId": "address-id",
        |  "addressLine": "masdar city",
        |  "addressPostBox": "110022",
        |  "addressCity": "Abu Dhabi",
        |  "addressCountry": "UAE",
        |  "latitude": 25.5,
        |  "longitude": 23.4,
        |  "currentAcademicYearId": "ay1",
        |  "organisation": "organisation-id-1",
        |  "timeZone": "Asia/Dubai",
        |  "firstTeachingDay": "Sunday",
        |  "occurredOn": "1970-07-14 02:40:00.0",
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "composition": "BOYS",
        |  "alias":"0AY11",
        |  "sourceId":"source-1",
        |  "contentRepositoryId": "cr1",
        |  "organisationGlobal": "globus",
        |  "content_repository_dw_id": 3,
        |  "entity_type": "content-repository",
        |  "content_repository_id": "cr1",
        |  "organization_code": "oc1"
        |}
        |]
        """.stripMargin

    val switchedValue =
      """
        |[
        |{
        |  "eventType": "SchoolAcademicYearSwitched",
        |  "currentAcademicYearId": "ay2",
        |  "currentAcademicYearType": "SCHOOL",
        |  "currentAcademicYearStartDate": 1571052193,
        |  "currentAcademicYearEndDate": 1571052193,
        |  "currentAcademicYearStatus": "CURRENT",
        |  "oldAcademicYearId": "ay1",
        |  "oldAcademicYearType": "ORGANISATION",
        |  "oldAcademicYearStartDate": 1571052193,
        |  "oldAcademicYearEndDate": 1571052193,
        |  "organization": "MOE",
        |  "schoolId": "schoolId",
        |  "updatedBy": "1a8d4564-c694-4b02-a5ff-4333f6b498d1",
        |  "occurredOn": "1970-07-14 05:00:00.0"
        |}
        |]
        |""".stripMargin

    val rolledOverValue =
      """
        |[
        |{
        |  "eventType": "AcademicYearRollOverCompleted",
        |  "id": "ay3",
        |  "schoolId": "schoolId",
        |  "previousId": "ay2",
        |  "occurredOn": "1970-07-14 10:00:00.0"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "saya_dw_id",
      "saya_school_id",
      "saya_academic_year_id",
      "saya_previous_academic_year_id",
      "saya_type",
      "saya_status",
      "saya_created_time",
      "saya_updated_time",
      "saya_dw_created_time",
      "saya_dw_updated_time",
    )

    val sprk = spark
    import sprk.implicits._

    val mutated = spark.read.json(Seq(schoolMutatedValue).toDS())
    when(service.readOptional(SchoolMutatedSource, sprk)).thenReturn(Some(mutated))

    val switched = spark.read.json(Seq(switchedValue).toDS())
    when(service.readOptional(SchoolAcademicYearSwitchedSource, sprk)).thenReturn(Some(switched))

    val rolledOver = spark.read.json(Seq(rolledOverValue).toDS())
    when(service.readOptional(SchoolAcademicYearRollOverCompletedSource, sprk)).thenReturn(Some(rolledOver))

    when(service.getStartIdUpdateStatus("dim_school_academic_year_association")).thenReturn(1)

    val transformer = new SchoolAcademicYearTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.dataFrame

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 3)

    val publishedDf = df.filter($"saya_dw_id" === 1)
    assert[String](publishedDf, "saya_school_id", "schoolId")
    assert[String](publishedDf, "saya_academic_year_id", "ay1")
    assert[String](publishedDf, "saya_previous_academic_year_id", null)
    assert[String](publishedDf, "saya_type", "CREATE")
    assert[Int](publishedDf, "saya_status", 2)
    assert[String](publishedDf, "saya_created_time", "1970-07-14 02:40:00.0")
    assert[String](publishedDf, "saya_updated_time", "1970-07-14 10:00:00.0")

    val switchedDf = df.filter($"saya_dw_id" === 2)
    assert[String](switchedDf, "saya_school_id", "schoolId")
    assert[String](switchedDf, "saya_academic_year_id", "ay2")
    assert[String](switchedDf, "saya_previous_academic_year_id", "ay1")
    assert[String](switchedDf, "saya_type", "SWITCH")
    assert[Int](switchedDf, "saya_status", 2)
    assert[String](switchedDf, "saya_created_time", "1970-07-14 05:00:00.0")
    assert[String](switchedDf, "saya_updated_time", "1970-07-14 10:00:00.0")

    val rollOverDf = df.filter($"saya_dw_id" === 3)
    assert[String](rollOverDf, "saya_school_id", "schoolId")
    assert[String](rollOverDf, "saya_academic_year_id", "ay3")
    assert[String](rollOverDf, "saya_previous_academic_year_id", "ay2")
    assert[String](rollOverDf, "saya_type", "ROLLOVER")
    assert[Int](rollOverDf, "saya_status", 1)
    assert[String](rollOverDf, "saya_created_time", "1970-07-14 10:00:00.0")
    assert[String](rollOverDf, "saya_updated_time", null)
  }

  test("SchoolAcademicYearTransform should transform when created and rolled over is present") {
    val schoolMutatedValue =
      """
        |[
        |{
        |  "eventType": "SchoolCreatedEvent",
        |  "uuid": "schoolId",
        |  "name": "school-name-1",
        |  "addressId": "address-id",
        |  "addressLine": "masdar city",
        |  "addressPostBox": "110022",
        |  "addressCity": "Abu Dhabi",
        |  "addressCountry": "UAE",
        |  "latitude": 25.5,
        |  "longitude": 23.4,
        |  "currentAcademicYearId": "ay1",
        |  "organisation": "organisation-id-1",
        |  "timeZone": "Asia/Dubai",
        |  "firstTeachingDay": "Sunday",
        |  "occurredOn": "1970-07-14 02:40:00.0",
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "composition": "BOYS",
        |  "alias":"0AY11",
        |  "sourceId":"source-1",
        |  "contentRepositoryId": "cr1",
        |  "organisationGlobal": "globus",
        |  "content_repository_dw_id": 3,
        |  "entity_type": "content-repository",
        |  "content_repository_id": "cr1",
        |  "organization_code": "oc1"
        |}
        |]
        """.stripMargin

    val rolledOverValue =
      """
        |[
        |{
        |  "eventType": "AcademicYearRollOverCompleted",
        |  "id": "ay3",
        |  "schoolId": "schoolId",
        |  "previousId": "ay2",
        |  "occurredOn": "1970-07-14 10:00:00.0"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "saya_dw_id",
      "saya_school_id",
      "saya_academic_year_id",
      "saya_previous_academic_year_id",
      "saya_type",
      "saya_status",
      "saya_created_time",
      "saya_updated_time",
      "saya_dw_created_time",
      "saya_dw_updated_time",
    )

    val sprk = spark
    import sprk.implicits._

    val mutated = spark.read.json(Seq(schoolMutatedValue).toDS())
    when(service.readOptional(SchoolMutatedSource, sprk)).thenReturn(Some(mutated))

    when(service.readOptional(SchoolAcademicYearSwitchedSource, sprk)).thenReturn(None)

    val rolledOver = spark.read.json(Seq(rolledOverValue).toDS())
    when(service.readOptional(SchoolAcademicYearRollOverCompletedSource, sprk)).thenReturn(Some(rolledOver))

    when(service.getStartIdUpdateStatus("dim_school_academic_year_association")).thenReturn(1)

    val transformer = new SchoolAcademicYearTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.dataFrame

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    val publishedDf = df.filter($"saya_dw_id" === 1)
    assert[String](publishedDf, "saya_school_id", "schoolId")
    assert[String](publishedDf, "saya_academic_year_id", "ay1")
    assert[String](publishedDf, "saya_previous_academic_year_id", null)
    assert[String](publishedDf, "saya_type", "CREATE")
    assert[Int](publishedDf, "saya_status", 2)
    assert[String](publishedDf, "saya_created_time", "1970-07-14 02:40:00.0")
    assert[String](publishedDf, "saya_updated_time", "1970-07-14 10:00:00.0")

    val rollOverDf = df.filter($"saya_dw_id" === 2)
    assert[String](rollOverDf, "saya_school_id", "schoolId")
    assert[String](rollOverDf, "saya_academic_year_id", "ay3")
    assert[String](rollOverDf, "saya_previous_academic_year_id", "ay2")
    assert[String](rollOverDf, "saya_type", "ROLLOVER")
    assert[Int](rollOverDf, "saya_status", 1)
    assert[String](rollOverDf, "saya_created_time", "1970-07-14 10:00:00.0")
    assert[String](rollOverDf, "saya_updated_time", null)
  }

  test("SchoolAcademicYearTransform should transform when rolled over is present") {
    val rolledOverValue =
      """
        |[
        |{
        |  "eventType": "AcademicYearRollOverCompleted",
        |  "id": "ay3",
        |  "schoolId": "schoolId",
        |  "previousId": "ay2",
        |  "occurredOn": "1970-07-14 10:00:00.0"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "saya_dw_id",
      "saya_school_id",
      "saya_academic_year_id",
      "saya_previous_academic_year_id",
      "saya_type",
      "saya_status",
      "saya_created_time",
      "saya_updated_time",
      "saya_dw_created_time",
      "saya_dw_updated_time",
    )

    val sprk = spark
    import sprk.implicits._

    when(service.readOptional(SchoolMutatedSource, sprk)).thenReturn(None)

    when(service.readOptional(SchoolAcademicYearSwitchedSource, sprk)).thenReturn(None)

    val rolledOver = spark.read.json(Seq(rolledOverValue).toDS())
    when(service.readOptional(SchoolAcademicYearRollOverCompletedSource, sprk)).thenReturn(Some(rolledOver))

    when(service.getStartIdUpdateStatus("dim_school_academic_year_association")).thenReturn(1)

    val transformer = new SchoolAcademicYearTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.dataFrame

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    val rollOverDf = df.filter($"saya_dw_id" === 1)
    assert[String](rollOverDf, "saya_school_id", "schoolId")
    assert[String](rollOverDf, "saya_academic_year_id", "ay3")
    assert[String](rollOverDf, "saya_previous_academic_year_id", "ay2")
    assert[String](rollOverDf, "saya_type", "ROLLOVER")
    assert[Int](rollOverDf, "saya_status", 1)
    assert[String](rollOverDf, "saya_created_time", "1970-07-14 10:00:00.0")
    assert[String](rollOverDf, "saya_updated_time", null)
  }

  test("SchoolAcademicYearTransform should transform when created is present") {
    val schoolMutatedValue =
      """
        |[
        |{
        |  "eventType": "SchoolCreatedEvent",
        |  "uuid": "schoolId",
        |  "name": "school-name-1",
        |  "addressId": "address-id",
        |  "addressLine": "masdar city",
        |  "addressPostBox": "110022",
        |  "addressCity": "Abu Dhabi",
        |  "addressCountry": "UAE",
        |  "latitude": 25.5,
        |  "longitude": 23.4,
        |  "currentAcademicYearId": "ay1",
        |  "organisation": "organisation-id-1",
        |  "timeZone": "Asia/Dubai",
        |  "firstTeachingDay": "Sunday",
        |  "occurredOn": "1970-07-14 02:40:00.0",
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "composition": "BOYS",
        |  "alias":"0AY11",
        |  "sourceId":"source-1",
        |  "contentRepositoryId": "cr1",
        |  "organisationGlobal": "globus",
        |  "content_repository_dw_id": 3,
        |  "entity_type": "content-repository",
        |  "content_repository_id": "cr1",
        |  "organization_code": "oc1"
        |}
        |]
        """.stripMargin

    val expectedColumns = Set(
      "saya_dw_id",
      "saya_school_id",
      "saya_academic_year_id",
      "saya_previous_academic_year_id",
      "saya_type",
      "saya_status",
      "saya_created_time",
      "saya_updated_time",
      "saya_dw_created_time",
      "saya_dw_updated_time",
    )

    val sprk = spark
    import sprk.implicits._


    val mutated = spark.read.json(Seq(schoolMutatedValue).toDS())
    when(service.readOptional(SchoolMutatedSource, sprk)).thenReturn(Some(mutated))

    when(service.readOptional(SchoolAcademicYearSwitchedSource, sprk)).thenReturn(None)

    when(service.readOptional(SchoolAcademicYearRollOverCompletedSource, sprk)).thenReturn(None)

    when(service.getStartIdUpdateStatus("dim_school_academic_year_association")).thenReturn(1)

    val transformer = new SchoolAcademicYearTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.dataFrame

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    val publishedDf = df.filter($"saya_dw_id" === 1)
    assert[String](publishedDf, "saya_school_id", "schoolId")
    assert[String](publishedDf, "saya_academic_year_id", "ay1")
    assert[String](publishedDf, "saya_previous_academic_year_id", null)
    assert[String](publishedDf, "saya_type", "CREATE")
    assert[Int](publishedDf, "saya_status", 1)
    assert[String](publishedDf, "saya_created_time", "1970-07-14 02:40:00.0")
    assert[String](publishedDf, "saya_updated_time", null)
  }

  test("SchoolAcademicYearTransform should not transform when no data present") {
    val sprk = spark

    when(service.readOptional(SchoolMutatedSource, sprk)).thenReturn(None)

    when(service.readOptional(SchoolAcademicYearSwitchedSource, sprk)).thenReturn(None)

    when(service.readOptional(SchoolAcademicYearRollOverCompletedSource, sprk)).thenReturn(None)

    when(service.getStartIdUpdateStatus("dim_school_academic_year_association")).thenReturn(1)

    val transformer = new SchoolAcademicYearTransform(sprk, service)
    val sinks = transformer.transform()

    assert(sinks.isEmpty)
  }

}
