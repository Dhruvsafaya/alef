package com.alefeducation.util

import com.alefeducation.bigdata.commons.testutils.{SparkTest, TestBaseSpec}
import org.apache.spark.sql.functions.lit

import java.sql.Timestamp

class FactTransformerTest extends TestBaseSpec with SparkTest {

  import spark.implicits._

  val sep2018Timestamp = Timestamp.valueOf("2018-09-08 02:40:00.0")
  val nov2021Timestamp = Timestamp.valueOf("2021-11-23 02:40:00.0")

  describe("appendGeneratedColumns") {
    it("should append date_dw_id, eventdate, created_time and dw_created_time to the records") {
      val factsDF = Seq(
        ("101", Timestamp.valueOf("2021-06-13 00:00:00")),
        ("100", Timestamp.valueOf("2021-06-12 00:00:00"))
      ).toDF("class_id", "occurredOn")

      val stubDateTimeProvider = stub[DateTimeProvider]
      (stubDateTimeProvider.toUtcTimestamp _).when("occurredOn").returns(lit(sep2018Timestamp))
      (stubDateTimeProvider.currentUtcTimestamp _).when().returns(lit(nov2021Timestamp))

      val factTransformer = new FactTransformer("class", stubDateTimeProvider)
      val factsWithGeneratedColumns = factTransformer.appendGeneratedColumns(factsDF)

      val expectedDF = Seq(
        ("101", Timestamp.valueOf("2021-06-13 00:00:00"), "20210613", sep2018Timestamp, nov2021Timestamp, "2021-06-13"),
        ("100", Timestamp.valueOf("2021-06-12 00:00:00"), "20210612", sep2018Timestamp, nov2021Timestamp, "2021-06-12")
      ).toDF("class_id", "occurredOn", "class_date_dw_id", "class_created_time", "class_dw_created_time", "eventdate")

      assertSmallDatasetEquality(factsWithGeneratedColumns, expectedDF, ignoreNullable = true)
    }

    it("should throw error when occurredOn column is missing") {
      val factsDF = Seq("101","100").toDF("class_id")

      val stubDateTimeProvider = stub[DateTimeProvider]
      (stubDateTimeProvider.toUtcTimestamp _).when("occurredOn").returns(lit(sep2018Timestamp))
      (stubDateTimeProvider.currentUtcTimestamp _).when().returns(lit(nov2021Timestamp))

      val factTransformer = new FactTransformer("class", stubDateTimeProvider)

      val exception = the[IllegalArgumentException] thrownBy factTransformer.appendGeneratedColumns(factsDF)

      exception.getMessage should include ("Column `occurredOn` is required to derive platform generated columns.")
    }
  }
}
