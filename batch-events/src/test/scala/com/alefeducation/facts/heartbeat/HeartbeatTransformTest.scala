package com.alefeducation.facts.heartbeat

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.heartbeat.HeartbeatTransform.ParquetHeartbeatSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class HeartbeatTransformTest extends SparkSuite  {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "eventdate",
    "fuhe_date_dw_id",
    "fuhe_created_time",
    "fuhe_dw_created_time",
    "fuhe_user_id",
    "fuhe_role",
    "fuhe_tenant_id",
    "fuhe_school_id",
    "fuhe_channel"
  )

  test("should transform heartbeat event successfully") {
    val value = """
                  |[
                  |{
                  |"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
                  |"eventType":"ChatConversationOccurred",
                  |"loadtime":"2023-05-17T05:39:28.481Z",
                  |"role":"student",
                  |"uuid": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
                  |"schoolId": "",
                  |"occurredOn": "2023-04-27 10:01:38.373481108",
                  |"channel": "web",
                  |"eventDateDw": "20230427"
                  |}
                  |]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._

    val transformer = new HeartbeatTransform(sprk, service)

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetHeartbeatSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "fuhe_role", "student")
    assert[String](df, "fuhe_user_id", "fd605223-dbe9-426e-a8f4-67c76d6357c1")
    assert[String](df, "fuhe_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](df, "fuhe_channel", "web")
    assert[String](df, "fuhe_created_time", "2023-04-27 10:01:38.373481")
    assert[String](df, "fuhe_date_dw_id", "20230427")
  }
}
