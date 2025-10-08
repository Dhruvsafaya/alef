package com.alefeducation.dimensions.school.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.school.transform.SchoolOrganizationTransform.SchoolOrganizationTransformSink
import com.alefeducation.dimensions.school.transform.SchoolUpdateCombinedTransform.{SchoolDeletedTransformSink, SchoolStatusUpdatedTransformSink, SchoolUpdatedTransformSink}
import com.alefeducation.util.Helpers.ParquetSchoolStatusSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class SchoolUpdateCombinedTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("school updated should transform") {
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
        |  "organisationGlobal": "globus",
        |  "content_repository_dw_id": 3,
        |  "entity_type": "content-repository",
        |  "content_repository_id": "cr1",
        |  "organization_code": "oc1",
        |  "organization_dw_id": 5,
        |  "_is_complete": true
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
        |  "organisationGlobal": "og1",
        |  "content_repository_dw_id": 3,
        |  "entity_type": "content-repository",
        |  "content_repository_id": "cr1",
        |  "organization_code": "oc1",
        |  "organization_dw_id": 5,
        |  "_is_complete": true
        |}
        |]
        """.stripMargin

    val expectedColumns = Set(
      "school_name",
      "school_latitude",
      "school_address_line",
      "school_id",
      "school_city_name",
      "school_country_name",
      "school_content_repository_id",
      "school_longitude",
      "school_first_day",
      "school_alias",
      "school_alias",
      "school_content_repository_dw_id",
      "school_organization_code",
      "school_organization_dw_id",
      "school_post_box",
      "school_tenant_id",
      "school_composition",
      "school_source_id",
      "school_timezone",
      "school_created_time",
      "school_dw_created_time",
      "school_dw_created_time",
      "school_updated_time",
      "school_deleted_time",
      "school_dw_updated_time",
      "_is_complete"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new SchoolUpdateCombinedTransform(sprk, service)

    val schoolMutatedInput = spark.read.json(Seq(schoolMutatedValue).toDS())
    when(service.readOptional(SchoolOrganizationTransformSink, sprk)).thenReturn(Some(schoolMutatedInput))

    when(service.readOptional(ParquetSchoolStatusSource, sprk)).thenReturn(None)

    val sinks = transformer.transform()

    val nonEmptySinks = sinks.filter(_.nonEmpty)

    assert(nonEmptySinks.length == 1)

    val df = nonEmptySinks.filter(_.get.sinkName == SchoolUpdatedTransformSink).head.get.dataFrame

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    assert[String](df, "school_city_name", "Abu Dhabi")
    assert[String](df, "school_country_name", "UAE")
    assert[String](df, "school_name", "school-name-11")
    assert[String](df, "school_content_repository_id", "cr1")
    assert[Int](df, "school_content_repository_dw_id", 3)
    assert[String](df, "school_organization_code", "og1")
    assert[Int](df, "school_organization_dw_id", 5)
    assert[String](df, "school_created_time", "1971-01-01 00:00:00.0")
    assert[String](df, "school_updated_time", null)
    assert[Boolean](df, "_is_complete", true)
  }

  test("school status updated should transform") {
    val schoolStatusValue =
      """
        |[
        |{
        |  "schoolId": "school-1",
        |  "eventType": "SchoolActivatedEvent",
        |  "occurredOn": "1970-07-14 02:40:00.0",
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
        |},
        |{
        |  "schoolId": "school-1",
        |  "eventType": "SchoolDeactivatedEvent",
        |  "occurredOn": "1970-07-14 03:00:00.0",
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
        |}
        |]
        """.stripMargin

    val expectedColumns = Set(
      "school_id",
      "school_status",
      "school_created_time",
      "school_dw_created_time",
      "school_updated_time",
      "school_deleted_time",
      "school_dw_updated_time"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new SchoolUpdateCombinedTransform(sprk, service)

    val schoolStatusInput = spark.read.json(Seq(schoolStatusValue).toDS())
    when(service.readOptional(ParquetSchoolStatusSource, sprk)).thenReturn(Some(schoolStatusInput))

    when(service.readOptional(SchoolOrganizationTransformSink, sprk)).thenReturn(None)

    val sinks = transformer.transform()
    val nonEmptySinks = sinks.filter(_.nonEmpty)

    assert(nonEmptySinks.length == 1)

    val df = nonEmptySinks.filter(_.get.sinkName == SchoolStatusUpdatedTransformSink).head.get.dataFrame

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    assert[String](df, "school_id", "school-1")
    assert[Int](df, "school_status", 3)
    assert[String](df, "school_created_time", "1970-07-14 03:00:00.0")
    assert[String](df, "school_updated_time", null)
  }

  test("school deleted should transform") {
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
        |  "organisationGlobal": "globus",
        |  "content_repository_dw_id": 3,
        |  "entity_type": "content-repository",
        |  "content_repository_id": "cr1",
        |  "organization_code": "oc1",
        |  "organization_dw_id": 5,
        |  "_is_complete": true
        |},
        |{
        |  "eventType": "SchoolDeletedEvent",
        |  "uuid": "school-id",
        |  "name": "school-name-111",
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
        |  "organisationGlobal": "og1",
        |  "content_repository_dw_id": 3,
        |  "entity_type": "content-repository",
        |  "content_repository_id": "cr1",
        |  "organization_code": "oc1",
        |  "organization_dw_id": 5,
        |  "_is_complete": true
        |}
        |]
        """.stripMargin

    val expectedColumns = Set(
      "school_status",
      "school_name",
      "school_latitude",
      "school_address_line",
      "school_id",
      "school_city_name",
      "school_country_name",
      "school_content_repository_id",
      "school_longitude",
      "school_first_day",
      "school_alias",
      "school_alias",
      "school_content_repository_dw_id",
      "school_organization_code",
      "school_organization_dw_id",
      "school_post_box",
      "school_tenant_id",
      "school_composition",
      "school_source_id",
      "school_timezone",
      "school_created_time",
      "school_dw_created_time",
      "school_dw_created_time",
      "school_updated_time",
      "school_deleted_time",
      "school_dw_updated_time",
      "_is_complete"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new SchoolUpdateCombinedTransform(sprk, service)

    val schoolMutatedInput = spark.read.json(Seq(schoolMutatedValue).toDS())
    when(service.readOptional(SchoolOrganizationTransformSink, sprk)).thenReturn(Some(schoolMutatedInput))

    when(service.readOptional(ParquetSchoolStatusSource, sprk)).thenReturn(None)

    val sinks = transformer.transform()
    val nonEmptySinks = sinks.filter(_.nonEmpty)

    assert(nonEmptySinks.length == 1)

    val df = nonEmptySinks.filter(_.get.sinkName == SchoolDeletedTransformSink).head.get.dataFrame

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    assert[String](df, "school_city_name", "Abu Dhabi")
    assert[String](df, "school_country_name", "UAE")
    assert[String](df, "school_name", "school-name-111")
    assert[String](df, "school_content_repository_id", "cr1")
    assert[Int](df, "school_content_repository_dw_id", 3)
    assert[String](df, "school_organization_code", "og1")
    assert[Int](df, "school_organization_dw_id", 5)
    assert[String](df, "school_created_time", "1971-01-01 00:00:00.0")
    assert[String](df, "school_updated_time", null)
    assert[String](df, "school_deleted_time", "1971-01-01 00:00:00.0")
    assert[Boolean](df, "_is_complete", true)
  }
}
