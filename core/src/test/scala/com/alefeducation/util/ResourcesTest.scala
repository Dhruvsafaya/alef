package com.alefeducation.util

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Resources.{Resource, env, getResourceByName, getS3Root}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.scalatest.matchers.should.Matchers


class ResourcesTest extends SparkSuite with Matchers {

  test("env should return test environment value") {
    val envValue = Resources.env()
    envValue shouldBe "test"
  }

  test("executionEnv should return execution environment") {
    val executionEnvValue = Resources.executionEnv()
    executionEnvValue shouldBe "local-test"
  }

  test("localS3Endpoint should return correct S3 endpoint") {
    val s3Endpoint = Resources.localS3Endpoint()
    s3Endpoint shouldBe "http://localhost:9000"
  }

  test("redshiftSchema should return correct schema") {
    val schema = Resources.redshiftSchema()
    schema shouldBe "alefdw"
  }

  test("getS3Root should return correct S3 root") {
    val s3Root = Resources.getS3Root()
    s3Root shouldBe "alef-data-platform"
  }

  test("getS3Path should return correct S3 path") {
    val s3Path = Resources.getS3Path()
    s3Path shouldBe "s3://test-bucket/data"
  }

  test("getDeltaSnapshotPartitions should return correct partition count") {
    val partitions = Resources.getDeltaSnapshotPartitions()
    partitions shouldBe 5
  }

  test("ALEF_39389_PATHWAY_CONTENT_REPO_IWH_TOGGLE should return true") {
    Resources.ALEF_39389_PATHWAY_CONTENT_REPO_IWH_TOGGLE shouldBe true
  }

  test("getStringList should return correct list of fields") {
    Resources.getList("service-name", "exclude-columns") shouldBe List("one", "two", "three")
  }

  test("should get payload stream") {
    val arrayStructData = Seq(
      Row("body1", "2023-01-21 00:00:00", List(Row("eventType", "ActivityCreated"))),
      Row("body2", "2023-01-21 00:00:00", List(Row("eventType", "ActivityUpdated")))
    )

    val arrayStructSchema = new StructType().add("value", StringType)
      .add("timestamp", StringType)
      .add("headers", ArrayType(new StructType()
        .add("key", StringType)
        .add("value", StringType)
      ))

    val df = spark.createDataFrame(spark.sparkContext
      .parallelize(arrayStructData), arrayStructSchema)

    val payloadStream = Resources.getPayloadStream(spark, df, useKafkaHeader = true)

    assert(payloadStream.columns === List("eventType", "body", "loadtime", "_headers"))
  }

  test("should load resource from config") {
    val either = getResourceByName("resource1")

    assert(either === Right(Resource("resource1", Map("one" -> "one1", "two" -> "two2"))))
  }

  test("should return left either with exception while try to load resource") {
    val either = getResourceByName("resource2")

    assert(either.isLeft)
    assert(either.left.map(_.getMessage).swap.getOrElse("") === "Key not found: 'resource2'.")
  }

  test("getResource should return list of resources") {
    val res = Resources.getResource("service-name", "sink")

    res.toList.sortBy(_.name) shouldBe List(
      Resource("sink-one",Map("name" -> "sink-one-value")),
      Resource("sink-two",Map("name" -> "sink-two-value"))
    )
  }

  test("getOffsetLogS3Path should return correct offset log S3 path") {
    val expected = "alef-data-platform/test/catalog/_internal/offset_log/"
    Resources.getOffsetLogS3Path shouldBe expected
  }

  test("getBatchExecutionLogTable should return correct batch execution log table name") {
    val expected = "test._internal.batch_execution_log"
    Resources.getBatchExecutionLogTable shouldBe expected
  }

  test("getBatchExecutionLogS3Path should return correct batch execution log S3 path") {
    val expected = "s3://test-bucket/data/test/catalog/_internal/batch_execution_log/"
    Resources.getBatchExecutionLogS3Path shouldBe expected
  }

  test("getBronzeTableNameFor should return correct bronze table name") {
    val expected = "test.bronze.bronze_table_name"
    Resources.getBronzeTableNameFor("bronze_table_name") shouldBe expected
  }
}
