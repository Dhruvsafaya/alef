package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class AvatarLayerDimensionTransformTest extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  private val Entity = "avatar_layer"
  val MutatedExpectedFields = Set(
      s"${Entity}_dw_id",
      s"${Entity}_created_time",
      s"${Entity}_dw_created_time",
      s"${Entity}_updated_time",
      s"${Entity}_dw_updated_time",
      s"${Entity}_deleted_time",
      s"${Entity}_id",
      s"${Entity}_config_type",
      s"${Entity}_type",
      s"${Entity}_key",
      s"${Entity}_value",
      s"${Entity}_genders",
      s"${Entity}_cost",
      s"${Entity}_tenants",
      s"${Entity}_tags",
      s"${Entity}_excluded_organizations",
      s"${Entity}_is_enabled",
      s"${Entity}_is_deleted",
      s"${Entity}_valid_from",
      s"${Entity}_valid_till",
      s"${Entity}_order",
      s"${Entity}_status"
  )

  test("Should transform created, updated and deleted comes in same batch") {

    val created_value = """
                          |[{
                          |	"eventType": "AvatarLayerCreatedEvent",
                          |	"id": "2abbb969-e361-4aca-ba8d-146054cc2f24",
                          |	"configType": "color",
                          |	"type": "STANDARD",
                          |	"key": "hairs",
                          |	"value": "#B25922",
                          |	"genders": [
                          |		"MALE",
                          |		"FEMALE",
                          |		"UNKNOWN"
                          |	],
                          |	"cost": 0,
                          |	"tenants": ["MOE"],
                          |	"tags": ["someTag"],
                          |	"excludedOrganizations": ["org1"],
                          |	"isEnabled": true,
                          |	"isDeleted": false,
                          |	"validFrom": "2024-04-23T10:35:36.418102203",
                          |	"validTill": "2024-04-29T20:00:00",
                          |	"order": 8,
                          |	"eventId": "18a01073-ec5b-4376-bcba-a1a185564bd4",
                          | "occurredOn": "2023-04-27 07:39:32.000",
                          |	"eventDateDw": 20230427
                          |}]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_avatar_layer")).thenReturn(1)

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-marketplace-avatar-layer-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("delta-marketplace-avatar-layer-sink", sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "ccl-marketplace-avatar-layer-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 1)

    assert[Int](df, s"${Entity}_dw_id", 1)
    assert[String](df, s"${Entity}_id", "2abbb969-e361-4aca-ba8d-146054cc2f24")
    assert[String](df, s"${Entity}_config_type", "color")
    assert[String](df, s"${Entity}_type", "STANDARD")
    assert[String](df, s"${Entity}_key", "hairs")
    assert[String](df, s"${Entity}_type", "STANDARD")
    assert[Boolean](df, s"${Entity}_is_enabled", true)
    assert[Int](df, s"${Entity}_status", 1)

  }
}
