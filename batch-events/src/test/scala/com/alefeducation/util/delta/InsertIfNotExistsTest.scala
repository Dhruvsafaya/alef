package com.alefeducation.util.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.DeltaInsertIfNotExistsSink
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.DataFrameEqualityUtils.assertSmallDatasetEquality
import com.alefeducation.util.Resources.getSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Timestamp

class InsertIfNotExistsTest extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns: List[String] = List(
    "user_dw_id",
    "user_id",
    "user_type",
    "user_created_time",
    "user_dw_created_time"
  )

  test("should prepare dataframe for insert if not exists") {
    val value =
      """
        |[
        |{
        |  "user_dw_id": 610081,
        |  "user_id": "0046a2cc-10c2-42b8-8bd0-1dd9705993e8",
        |  "user_type": "ADMIN",
        |  "user_created_time": "2020-09-30 09:53:39",
        |  "user_dw_created_time": "2020-09-30 11:08:07"
        |}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val serviceName = "delta-user-service"
    val sourceName = getSource(serviceName).head
    val inputDf = spark.read.json(Seq(value).toDS()).withColumn("user_created_time", col("user_created_time").cast(TimestampType))
    when(service.readOptional(sourceName, sprk)).thenReturn(Some(inputDf))

    val transform = new InsertIfNotExists(sprk, service, serviceName)

    val sink = transform.write()

    val dataSink = sink.head.asInstanceOf[DeltaInsertIfNotExistsSink]
    val df = dataSink.output

    assert(df.columns.toSet === expectedColumns.toSet)

    assert(dataSink.matchConditions === "delta.user_id = events.user_id and delta.user_id2 = events.user_id2")
    val expectedDF = List(
      (610081, "0046a2cc-10c2-42b8-8bd0-1dd9705993e8", "ADMIN", Timestamp.valueOf("2020-09-30 09:53:39"), Timestamp.valueOf("2020-09-30 11:08:07"))
    ).toDF(expectedColumns: _*)

    assertSmallDatasetEquality("user", df, expectedDF)
  }

}
