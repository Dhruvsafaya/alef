package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class TeacherTestDeliverySettingsTest extends SparkSuite with BaseDimensionSpec{
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("Should transform for dim teacher test delivery settings table") {

    val created_value =
      """
        |[{
        |	"eventType": "TestDeliveryCreatedIntegrationEvent",
        |	"id": "40ea6fab-fca5-40c0-8065-23b731aff9ed",
        | "testId": "80bc17d5-95c3-442b-af21-5b69fc8d5a93",
        | "contextFrameClassId": "58e158bf-020f-45a5-bd94-732cb5c2a91c",
        | "contextDomainId": "5ba14edb-464e-4cb9-b5e9-0da24713ac45",
        | "candidates": [
        |    "1665b58b-9eb4-42a1-92b3-e0588717fbf1",
        |    "17e4ab6b-da94-4970-81a4-bdfc2e5cae27"
        | ],
        | "deliverySettings": {
        |    "title": "cc",
        |    "startTime": "2024-01-30T08:03:22.985",
        |    "endTime": null,
        |    "allowLateSubmission": true,
        |    "stars": 5,
        |    "randomized": false
        | },
        | "status": "UPCOMING",
        | "allPossibleCandidates": [
        |    "e3c2968c-8481-41de-a6b3-d68652c3e922",
        |    "6cbfbd95-a113-4fdb-87ab-636ebc160d54"
        | ],
        | "occurredOn": "2023-04-27 07:39:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}]
      """.stripMargin
    val updated_value =
      """
        |{
        |	"eventType": "TestDeliveryArchivedIntegrationEvent",
        | "id": "40ea6fab-fca5-40c0-8065-23b731aff9ed",
        | "status": "DISCARDED",
        | "occurredOn": "2023-04-27 07:39:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_teacher_test_delivery_settings")).thenReturn(1)

    val createdDf = spark.read.json(Seq(created_value).toDS())
    val updatedDf = spark.read.json(Seq(updated_value).toDS())
    when(service.readOptional("parquet-teacher-test-delivery-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(createdDf))
    when(service.readOptional("parquet-teacher-test-delivery-archived-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(updatedDf))
    when(service.readOptional("delta-teacher-test-delivery-sink",  sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "teacher-test-delivery-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    val fst = df.first()

    assert(df.count === 1)
    assert(fst.getAs[String]("ttds_delivery_status") == "DISCARDED")
    assert(fst.getAs[Int]("ttds_status") == 4)
  }

}
