package com.alefeducation.bigdata.commons.testutils

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import java.util.TimeZone

trait SparkSessionWrapper {
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  System.setProperty("user.timezone", "UTC")

  lazy val spark: SparkSession = {
    val sparkConf = new SparkConf()
        .set("spark.sql.session.timeZone", "UTC")
        .set("_enableConnection", "false")
        .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .set("spark.sql.datetime.java8API.enabled", "false")
        .set("spark.sql.legacy.typeCoercion.datetimeToString.enabled", "true")
        .set("spark.sql.autoBroadcastJoinThreshold", "-1")
        .set("spark.sql.shuffle.partitions", "1")
        .set("spark.default.parallelism", "1")
        .set("spark.ui.enabled", "false")
        .set("spark.driver.bindAddress", "127.0.0.1")
        .set("spark.driver.host", "127.0.0.1")
        .set("spark.sql.adaptive.enabled", "false")
        .set("spark.sql.cbo.enabled", "false")
        .set("spark.driver.memory", "1g")
        .set("spark.sql.parquet.compression.codec", "snappy")
        .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
        .set("spark.sql.statistics.histogram.enabled", "false")
        .set("spark.sql.parquet.mergeSchema", "false")

    SparkSession
      .builder()
      .config(sparkConf)
      .master("local[*]")
      .appName("Spark Test")
      .getOrCreate()
  }

}

trait TestBaseSpec extends AnyFunSpec with Matchers with MockFactory with BeforeAndAfterAll

trait SparkTest extends SparkSessionWrapper with DatasetComparer {
  this: TestBaseSpec =>
}
