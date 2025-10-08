package com.alefeducation.dimensions.school.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.schema.admin.{ContentRepositoryRedshift, DwIdMapping, OrganizationRedshift}
import com.alefeducation.util.Helpers.{ParquetSchoolSource, RedshiftContentRepositorySource, RedshiftDwIdMappingSource, RedshiftOrganizationSource}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class SchoolOrganizationTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("school organization should transform") {
    val schoolMutatedValue =
      """
        |[
        |{
        |  "eventType": "SchoolCreatedEvent",
        |  "uuid": "school-id",
        |  "name": "school-name-1",
        |  "addressId": "address-id",
        |  "addressLine": "masdar city",
        |  "addressPostBox": "110022",
        |  "addressCity": "Abu Dhabi",
        |  "addressCountry": "UAE",
        |  "latitude": 25.5,
        |  "longitude": 23.4,
        |  "organisation": "organisation-id-1",
        |  "timeZone": "Asia/Dubai",
        |  "firstTeachingDay": "Sunday",
        |  "occurredOn": "1970-07-14 02:40:00.0",
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "composition": "BOYS",
        |  "alias":"0AY11",
        |  "sourceId":"source-1",
        |  "contentRepositoryId": "cr1",
        |  "organisationGlobal": "globus"
        |},
        |{
        |  "eventType": "SchoolUpdatedEvent",
        |  "uuid": "school-id",
        |  "name": "school-name-11",
        |  "addressId": "address-id",
        |  "addressLine": "masdar city",
        |  "addressPostBox": "110022",
        |  "addressCity": "Abu Dhabi",
        |  "addressCountry": "UAE",
        |  "latitude": 25.5,
        |  "longitude": 23.4,
        |  "organisation": "organisation-id-1",
        |  "timeZone": "Asia/Dubai",
        |  "firstTeachingDay": "Sunday",
        |  "occurredOn": "1971-01-01 00:00:00.0",
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "composition": "BOYS",
        |  "alias":"0AY11",
        |  "sourceId":"source-1",
        |  "contentRepositoryId": "cr1",
        |  "organisationGlobal": "og1"
        |}
        |]
        """.stripMargin

    val dwIdMappingValue =
      """
        |[
        |{
        |  "content_repository_dw_id": 3,
        |  "content_repository_id": "cr1"
        |}
        |]
        |""".stripMargin

    val dimOrganizationValue =
      """
        |[
        |{
        |  "organization_dw_id": 5,
        |  "organization_code": "og1"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "addressCity",
      "addressCountry",
      "addressId",
      "addressLine",
      "addressPostBox",
      "alias",
      "composition",
      "contentRepositoryId",
      "eventType",
      "firstTeachingDay",
      "latitude",
      "longitude",
      "name",
      "occurredOn",
      "organisation",
      "organisationGlobal",
      "sourceId",
      "tenantId",
      "timeZone",
      "uuid",
      "content_repository_dw_id",
      "content_repository_id",
      "organization_code",
      "organization_dw_id",
      "_is_complete"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new SchoolOrganizationTransform(sprk, service)

    val schoolMutatedInput = spark.read.json(Seq(schoolMutatedValue).toDS())
    when(service.readOptional(ParquetSchoolSource, sprk)).thenReturn(Some(schoolMutatedInput))

    val dwIdMappingInput = spark.read.json(Seq(dwIdMappingValue).toDS())
    when(service.readFromRedshift[ContentRepositoryRedshift](RedshiftContentRepositorySource)).thenReturn(dwIdMappingInput)

    val dimOrganizationInput = spark.read.json(Seq(dimOrganizationValue).toDS())
    when(service.readFromRedshift[OrganizationRedshift](RedshiftOrganizationSource)).thenReturn(dimOrganizationInput)

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    val created = df.filter($"eventType" === "SchoolCreatedEvent")
    assert[String](created, "addressCity", "Abu Dhabi")
    assert[String](created, "addressCountry", "UAE")
    assert[String](created, "name", "school-name-1")
    assert[String](created, "content_repository_id", "cr1")
    assert[Int](created, "content_repository_dw_id", 3)
    assert[String](created, "organisationGlobal", "globus")
    assert[String](created, "organization_dw_id", null)
    assert[Boolean](created, "_is_complete", false)

    val updated = df.filter($"eventType" === "SchoolUpdatedEvent")
    assert[String](updated, "addressCity", "Abu Dhabi")
    assert[String](updated, "addressCountry", "UAE")
    assert[String](updated, "name", "school-name-11")
    assert[String](created, "content_repository_id", "cr1")
    assert[Int](created, "content_repository_dw_id", 3)
    assert[String](updated, "organisationGlobal", "og1")
    assert[Int](updated, "organization_dw_id", 5)
    assert[Boolean](updated, "_is_complete", true)

  }
}
