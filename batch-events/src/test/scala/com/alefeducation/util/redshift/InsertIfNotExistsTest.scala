package com.alefeducation.util.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.DataFrameEqualityUtils.assertSmallDatasetEquality
import com.alefeducation.util.Resources.getSource
import com.alefeducation.util.StringUtilities.replaceSpecChars
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

  test("should prepare dataframe for insert if not exists using post actions") {
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

    val serviceName = "redshift-rel-user-service"
    val sourceName = getSource(serviceName).head
    val inputDf = spark.read.json(Seq(value).toDS()).withColumn("user_created_time", col("user_created_time").cast(TimestampType))
    when(service.readOptional(sourceName, sprk)).thenReturn(Some(inputDf))

    val transform = new InsertIfNotExists(sprk, service, serviceName)

    val sink = transform.write()

    val dataSink = sink.head
    val df = dataSink.output

    val expectedQuery = """
                     |INSERT INTO rs_stage_schema.rel_user (user_created_time, user_dw_created_time, user_dw_id, user_id, user_type)
                     |  select user_created_time, user_dw_created_time, user_dw_id, user_id, user_type from rs_stage_schema.staging_rel_user_tmp t
                     |     WHERE NOT EXISTS (SELECT 1 FROM rs_stage_schema.rel_user WHERE user_id = t.user_id);
                     |DROP table rs_stage_schema.staging_rel_user_tmp;
                     |""".stripMargin

    val options = dataSink.options
    assert(options("dbtable") === "rs_stage_schema.staging_rel_user_tmp")
    assert(replaceSpecChars(options("postactions")) === replaceSpecChars(expectedQuery))

    assert(df.columns.toSet === expectedColumns.toSet)

    val expectedDF = List(
      (610081, "0046a2cc-10c2-42b8-8bd0-1dd9705993e8", "ADMIN", Timestamp.valueOf("2020-09-30 09:53:39"), Timestamp.valueOf("2020-09-30 11:08:07"))
    ).toDF(expectedColumns: _*)

    assertSmallDatasetEquality("user", df, expectedDF)
  }

  test("should prepare dataframe with cast fields that mentioned in config") {
    val expColumns = List("user_dw_id", "user_id", "user_date_dw_id")
    val value =
      """
        |[
        |{
        |  "user_dw_id": 610081,
        |  "user_id": "0046a2cc-10c2-42b8-8bd0-1dd9705993e8",
        |  "user_date_dw_id": "20250501"
        |}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val serviceName = "user-service"
    val sourceName = getSource(serviceName).head
    val inputDf = spark.read.json(Seq(value).toDS())
    when(service.readOptional(sourceName, sprk)).thenReturn(Some(inputDf))

    val transform = new InsertIfNotExists(sprk, service, serviceName)

    val sink = transform.write()

    val dataSink = sink.head
    val df = dataSink.output

    val expectedDF = List(
      (610081, "0046a2cc-10c2-42b8-8bd0-1dd9705993e8", 20250501)
    ).toDF(expColumns: _*)

    assertSmallDatasetEquality("user", df, expectedDF)
  }
}
