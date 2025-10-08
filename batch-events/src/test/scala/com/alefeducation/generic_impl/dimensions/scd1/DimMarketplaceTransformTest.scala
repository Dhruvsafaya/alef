package com.alefeducation.generic_impl.dimensions.scd1

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.generic_impl.GenericSCD1Transformation
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class DimMarketplaceTransformTest extends SparkSuite with BaseDimensionSpec{
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val MutatedExpectedFields = Set(
    s"created_time",
    s"updated_time",
    s"deleted_time",
    s"dw_created_time",
    s"dw_updated_time",
    s"dw_id",
    s"id",
    s"_trace_id",
    s"event_type",
    s"tenant_id",
    s"impact_type",
    s"star_cost",
    s"quota",
    s"status"
  )

  test("Should transform created event") {

    val created_value =
      """
        |[{
        |"eventType": "TreeImpactConfigUpdatedEvent",
        | "_trace_id": "trace-id-1",
        | "_app_tenant": "tenant-id-1",
        |	"id": "844d1e74-ec01-4a70-b79a-5822ae5d7663",
        |   "occurredOn": 1682581172000,
        |   "treeImpact": "BIODIVERSITY",
        |   "starCost": 1000,
        |   "quota": 500,
        |   "status":1
        |}]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("marketplace_config")).thenReturn(1)
    val EventDF = spark.read.json(Seq(created_value).toDS())

    when(service.readOptional("delta-marketplace-config-sink", sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "marketplace-config-mutated-transform")
    val df = transformer.transform(Map("alef_marketplace_plant_a_tree" -> Some(EventDF)), 1).get


    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 1)

    assert[Int](df, s"dw_id", 1)
    assert[String](df, s"_trace_id", "trace-id-1")
    assert[String](df, s"event_type", "TreeImpactConfigUpdatedEvent")
    assert[String](df, s"tenant_id", "tenant-id-1")
    assert[String](df, s"id", "844d1e74-ec01-4a70-b79a-5822ae5d7663")
    assert[String](df, s"impact_type", "BIODIVERSITY")
    assert[Int](df, s"star_cost", 1000)
    assert[Int](df, s"quota", 500)
    assertTimestamp(df, s"created_time", "2023-04-27 07:39:32.0")
    assertTimestamp(df, s"updated_time", null)
    assertTimestamp(df, s"deleted_time", null)
    assert[Int](df, s"status", 1)
  }
}
