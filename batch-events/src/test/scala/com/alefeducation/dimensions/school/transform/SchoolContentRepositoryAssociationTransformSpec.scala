package com.alefeducation.dimensions.school.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.school.transform.SchoolContentRepositoryAssociationTransform.{Key, SchoolContentRepositoryAssEntity, SchoolContentRepositoryAssociationService}
import com.alefeducation.util.DataFrameEqualityUtils.assertSmallDatasetEquality
import com.alefeducation.util.Resources.getSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Timestamp

class SchoolContentRepositoryAssociationTransformSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("school created should transform") {
    val schoolMutatedValue =
      """
        |[
        |{
        |  "eventType": "SchoolCreatedEvent",
        |  "uuid": "94892f1e-3f2a-4789-8349-310575cb9e9c",
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
        |  "occurredOn": "2024-07-14 02:40:00.0",
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "composition": "BOYS",
        |  "alias":"0AY11",
        |  "sourceId":"source-1",
        |  "contentRepositoryId": "cr1",
        |  "organisationGlobal": "globus",
        |  "contentRepositoryIds": [
        |    "ebb5c3a3-e9af-4273-aa8f-39005c48d3f3",
        |    "c745b01e-6fb3-4e17-9702-13cdcafa5eef"
        |  ]
        |},
        |{
        |  "eventType": "SchoolUpdatedEvent",
        |  "uuid": "faa19c2a-dafc-4ef1-ad84-5a1918f521c5",
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
        |  "occurredOn": "2024-08-01 03:00:00.0",
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "composition": "BOYS",
        |  "alias":"0AY11",
        |  "sourceId":"source-1",
        |  "contentRepositoryId": "cr1",
        |  "organisationGlobal": "og1",
        |  "contentRepositoryIds": [
        |    "ebb5c3a3-e9af-4273-aa8f-39005c48d3f3",
        |    "c745b01e-6fb3-4e17-9702-13cdcafa5eef"
        |  ]
        |},
        |{
        |  "eventType": "SchoolUpdatedEvent",
        |  "uuid": "faa19c2a-dafc-4ef1-ad84-5a1918f521c5",
        |  "name": "school-name-11",
        |  "addressId": "address-id",
        |  "addressLine": "masdar city",
        |  "addressPostBox": "110022",
        |  "addressCity": "Abu Dhabi",
        |  "addressCountry": "UAE",
        |  "latitude": 25.5,
        |  "longitude": 23.4,
        |  "organisation": "organisation-id-2",
        |  "timeZone": "Asia/Dubai",
        |  "firstTeachingDay": "Sunday",
        |  "occurredOn": "2024-08-02 03:00:00.0",
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "composition": "BOYS",
        |  "alias":"0AY11",
        |  "sourceId":"source-1",
        |  "contentRepositoryId": "cr1",
        |  "organisationGlobal": "og1",
        |  "contentRepositoryIds": [
        |    "ebb5c3a3-e9af-4273-aa8f-39005c48d3f3",
        |    "4b137d0f-7c98-4c7c-b78a-b91d97d90717"
        |  ]
        |}
        |]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val expectedColumns = List(
      "scra_status",
      "scra_school_id",
      "scra_content_repository_id",
      "scra_active_until",
      "scra_created_time",
      "scra_dw_created_time",
      "scra_dw_id"
    )

    val sourceName = getSource(SchoolContentRepositoryAssociationService).head

    val schoolMutatedInput = spark.read.json(Seq(schoolMutatedValue).toDS())
    when(service.readOptional(sourceName, sprk)).thenReturn(Some(schoolMutatedInput))

    when(service.getStartIdUpdateStatus(Key)).thenReturn(5001)

    val transformer = new SchoolContentRepositoryAssociationTransform(sprk, service, SchoolContentRepositoryAssociationService)

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns.toSet)

    val expectedDF = List(
      (1, "94892f1e-3f2a-4789-8349-310575cb9e9c", "ebb5c3a3-e9af-4273-aa8f-39005c48d3f3", null, Timestamp.valueOf("2024-07-14 02:40:00.0"), Timestamp.valueOf("2024-08-15 02:40:00.0"), 5001),
      (1, "94892f1e-3f2a-4789-8349-310575cb9e9c", "c745b01e-6fb3-4e17-9702-13cdcafa5eef", null, Timestamp.valueOf("2024-07-14 02:40:00.0"), Timestamp.valueOf("2024-08-15 02:40:00.0"), 5002),
      (2, "faa19c2a-dafc-4ef1-ad84-5a1918f521c5", "ebb5c3a3-e9af-4273-aa8f-39005c48d3f3", Timestamp.valueOf("2024-08-02 03:00:00"), Timestamp.valueOf("2024-08-01 03:00:00"), Timestamp.valueOf("2024-08-15 02:40:00.0"), 5003),
      (2, "faa19c2a-dafc-4ef1-ad84-5a1918f521c5", "c745b01e-6fb3-4e17-9702-13cdcafa5eef", Timestamp.valueOf("2024-08-02 03:00:00"), Timestamp.valueOf("2024-08-01 03:00:00"), Timestamp.valueOf("2024-08-15 02:40:00.0"), 5004),
      (1, "faa19c2a-dafc-4ef1-ad84-5a1918f521c5", "ebb5c3a3-e9af-4273-aa8f-39005c48d3f3", null, Timestamp.valueOf("2024-08-02 03:00:00"), Timestamp.valueOf("2024-08-15 02:40:00.0"), 5005),
      (1, "faa19c2a-dafc-4ef1-ad84-5a1918f521c5", "4b137d0f-7c98-4c7c-b78a-b91d97d90717", null, Timestamp.valueOf("2024-08-02 03:00:00"), Timestamp.valueOf("2024-08-15 02:40:00.0"), 5006)
    ).toDF(expectedColumns: _*)

    assertSmallDatasetEquality(SchoolContentRepositoryAssEntity, df, expectedDF)
  }
}
