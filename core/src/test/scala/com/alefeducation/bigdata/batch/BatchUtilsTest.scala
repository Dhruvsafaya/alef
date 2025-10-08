package com.alefeducation.bigdata.batch

import com.alefeducation.bigdata.batch.BatchUtils._
import org.apache.spark.sql.SparkSession
import com.alefeducation.bigdata.commons.testutils.SparkSuite



import scala.collection.immutable.ListMap


class BatchUtilsTest extends SparkSuite {

  val entityPrefix = "test"

  test("Add Status based on Active until with entityPrefix provided") {

    val plannedValue =
      """
        |[
        |{
        | "test_id":"test_id_val",
        | "eventType": "TestEvent"
        |}
        |]
        |""".stripMargin

    val expectedCols = Set(
      "eventType", "test_id", "test_status"
    )

    val sprk: SparkSession = spark

    import sprk.implicits._

    val df = spark.read.json(Seq(plannedValue).toDS)
    val outputDF = addStatusColumn(sprk, df, entityPrefix)

    assert(outputDF.columns.toSet === expectedCols)
  }

  test("Add Status based on Active until with entityPrefix not provided") {

    val plannedValue =
      """
        |[
        |{
        | "test_id":"test_id_val",
        | "eventType": "TestEvent"
        |}
        |]
        |""".stripMargin

    val expectedCols = Set(
      "eventType", "test_id", "status"
    )

    val sprk: SparkSession = spark

    import sprk.implicits._

    val df = spark.read.json(Seq(plannedValue).toDS)
    val outputDF = addStatusColumn(sprk, df)

    assert(outputDF.columns.toSet === expectedCols)
  }

}
