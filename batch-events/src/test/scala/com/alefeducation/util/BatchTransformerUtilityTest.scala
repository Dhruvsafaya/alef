package com.alefeducation.util

import com.alefeducation.bigdata.commons.testutils.{SparkTest, TestBaseSpec}
import com.alefeducation.util.DataFrameEqualityUtils.{createDfFromJson, assertSmallDatasetEquality => assertDF}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.scalatest.Ignore

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class BatchTransformerUtilityTest extends TestBaseSpec with SparkTest {

  import com.alefeducation.util.BatchTransformerUtility._
  import spark.implicits._

  describe("getLatestRecords") {
    it("Should get latest records from given records") {
      val record = Seq(("100", getDateTime("2021-06-13")), (("100", getDateTime("2021-06-12")))).toDF("class_id", "occurredOn")
      val latestRecord = getLatestRecords(record, "class_id", "occurredOn")
      assert(latestRecord.count == 1)
    }
  }

  describe("genDwId") {
    val json =
      """
        |[
        |{"col":"one","occurredOn":"2023-10-03 15:21:00","id":1},
        |{"col":"two","occurredOn":"2023-10-05 01:00:00","id":2},
        |{"col":"three","occurredOn":"2023-10-06 17:22:00","id":3}
        |]
      """.stripMargin

    val df = Seq(
      ("two", "2023-10-05 01:00:00"),
      ("one", "2023-10-03 15:21:00"),
      ("three", "2023-10-06 17:22:00")
    ).toDF("col", "occurredOn")

    val resDf = df.genDwId("id", 1L)

    assertDF("entity", resDf, createDfFromJson(spark, json))
  }

  describe("transformForSCDTypeII should mark old rows as historical") {
    val json =
      """
        |[
        |{"user_status":1,"user_event_type":"CreateUser","user_active_until":null,"user_id":1,"user_type":"user","user_created_time":"2017-07-14 02:40:00.000Z","user_dw_created_time":"2024-07-30T06:10:43.054Z"},
        |{"user_status":1,"user_event_type":"DisableUser","user_active_until":null,"user_id":2,"user_type":"teacher","user_created_time":"2017-07-15 03:46:00.000Z","user_dw_created_time":"2024-07-30T06:10:43.054Z"},
        |{"user_status":2,"user_event_type":"UpdateUser","user_active_until":"2017-07-15 03:46:00.000Z","user_id":2,"user_type":"teacher","user_created_time":"2017-07-15 03:45:00.000Z","user_dw_created_time":"2024-07-30 06:10:43.054Z"},
        |{"user_status":2,"user_event_type":"CreateUser","user_active_until":"2017-07-15 03:46:00.000Z","user_id":2,"user_type":"teacher","user_created_time":"2017-07-15 02:40:00.000Z","user_dw_created_time":"2024-07-30 06:10:43.054Z"}
        |]
      """.stripMargin

    val df: DataFrame = List(
      (1, "user", 1001, "2017-07-14 02:40:00.0", "CreateUser"),
      (2, "teacher", 1002, "2017-07-15 02:40:00.0", "CreateUser"),
      (2, "teacher", 1003, "2017-07-15 03:45:00.0", "UpdateUser"),
      (2, "teacher", 1004, "2017-07-15 03:46:00.0", "DisableUser"),
    ).toDF("id", "type", "test_id", "occurredOn", "eventType")

    val resDf = df.transformForSCDTypeII(
      Map(
        "user_status" -> "user_status",
        "user_active_until" -> "user_active_until",
        "eventType" -> "user_event_type",
        "type" -> "user_type",
        "occurredOn" -> "occurredOn",
        "id" -> "user_id"
      ),
      "user",
      List("id", "type"),
      "occurredOn"
    )

    val expDf = createDfFromJson(spark, json)
      .withColumn("user_active_until", col("user_active_until").cast(TimestampType))
      .withColumn("user_created_time", col("user_created_time").cast(TimestampType))

    assertDF("user", resDf, expDf)
  }

  // ignore because date_dw_id in delta should migrated from string to integer type
  ignore("appendFactDateColumns should append date_dw_id with Integer type") {
    val json =
      """
        |[
        |{"eventType":"CreateUser","id":1,"occurredOn":"2017-07-14 02:40:00.000Z","user_date_dw_id":20170714,"eventdate":"2017-07-14"},
        |{"eventType":"CreateUser","id":2,"occurredOn":"2017-07-15 02:40:00.000Z","user_date_dw_id":20170715,"eventdate":"2017-07-15"}
        |]
      """.stripMargin

    val df: DataFrame = List(
      (1, "2017-07-14 02:40:00.0", "CreateUser"),
      (2, "2017-07-15 02:40:00.0", "CreateUser"),
    ).toDF("id", "occurredOn", "eventType")
      .withColumn("occurredOn", col("occurredOn").cast(TimestampType))

    val resDf = df.appendFactDateColumns("user", isFact = true)

    val expDf = createDfFromJson(spark, json)
      .withColumn("occurredOn", col("occurredOn").cast(TimestampType))

    assertDF("user", resDf, expDf)
  }

  def getDateTime(dateStr: String): Long = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    LocalDate.parse(dateStr, formatter).toEpochDay
  }
}
