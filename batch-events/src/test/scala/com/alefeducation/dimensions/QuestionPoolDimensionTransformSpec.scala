package com.alefeducation.dimensions

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.question_pool.QuestionPoolDimensionTransform
import com.alefeducation.dimensions.question_pool.QuestionPoolDimensionTransform.QuestionPoolMutatedSourceName
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class QuestionPoolDimensionTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val QuestionPoolMutatedExpectedFields = Set(
    "question_pool_id",
    "question_pool_name",
    "question_pool_triggered_by",
    "question_pool_question_code_prefix",
    "question_pool_app_status",
    "question_pool_status",
    "question_pool_created_time",
    "question_pool_updated_time",
    "question_pool_deleted_time",
    "question_pool_dw_created_time",
    "question_pool_dw_updated_time"
  )

  test("Transform QuestionPool Created Event") {
    val value =
      """
        |[
        |{"eventType":"PoolCreatedEvent","triggeredBy": "29","name":"test123","questionCodePrefix":"test123_","status":"DRAFT","createdBy":"29","updatedBy":"29","createdAt":1598360607238,"updatedAt":1598360607238,"occurredOn":"08-25-2020 1:03:27.238","poolId":"5f450c1f70b7d80001d3a128","partitionKey":"5f450c1f70b7d80001d3a128","id":"5085098c-7deb-41be-afa1-4225332ea52b"}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(QuestionPoolMutatedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new QuestionPoolDimensionTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.get.output

    assert(df.columns.toSet === QuestionPoolMutatedExpectedFields)

    assert[String](df, "question_pool_id", "5f450c1f70b7d80001d3a128")
    assert[String](df, "question_pool_name", "test123")
    assert[String](df, "question_pool_triggered_by", "29")
    assert[String](df, "question_pool_question_code_prefix", "test123_")
    assert[String](df, "question_pool_app_status", "DRAFT")
    assert[Int](df, "question_pool_status", 1)
  }

  test("Transform QuestionPoolUpdated Event") {
    val value =
      """
        |[
        |{"eventType":"PoolUpdatedEvent","triggeredBy": "29","name":"test123","questionCodePrefix":"test123_updated","status":"DRAFT","createdBy":"29","updatedBy":"29","createdAt":1598360607238,"updatedAt":1598360607238,"occurredOn":"08-25-2020 2:03:27.238","poolId":"5f450c1f70b7d80001d3a128","partitionKey":"5f450c1f70b7d80001d3a128","id":"5085098c-7deb-41be-afa1-4225332ea52b"}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(QuestionPoolMutatedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new QuestionPoolDimensionTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.get.output

    assert(df.columns.toSet === QuestionPoolMutatedExpectedFields)

    assert[String](df, "question_pool_id", "5f450c1f70b7d80001d3a128")
    assert[String](df, "question_pool_name", "test123")
    assert[String](df, "question_pool_triggered_by", "29")
    assert[String](df, "question_pool_question_code_prefix", "test123_updated")
    assert[String](df, "question_pool_app_status", "DRAFT")
    assert[Int](df, "question_pool_status", 1)
  }

  test("Transform QuestionPool Created/Updated Event") {
    val value =
      """
        |[
        |{"eventType":"PoolCreatedEvent","triggeredBy": "29","name":"test123","questionCodePrefix":"test123_","status":"DRAFT","createdBy":"29","updatedBy":"29","createdAt":1598360607238,"updatedAt":1598360607238,"occurredOn":"08-25-2020 1:03:27.238","poolId":"5f450c1f70b7d80001d3a128","partitionKey":"5f450c1f70b7d80001d3a128","id":"5085098c-7deb-41be-afa1-4225332ea52b"},
        |{"eventType":"PoolUpdatedEvent","triggeredBy": "29","name":"test123","questionCodePrefix":"test123_updated","status":"DRAFT","createdBy":"29","updatedBy":"29","createdAt":1598360607238,"updatedAt":1598360607238,"occurredOn":"08-25-2020 2:03:27.238","poolId":"5f450c1f70b7d80001d3a128","partitionKey":"5f450c1f70b7d80001d3a128","id":"5085098c-7deb-41be-afa1-4225332ea52b"}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(QuestionPoolMutatedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new QuestionPoolDimensionTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.get.output

    assert(df.columns.toSet === QuestionPoolMutatedExpectedFields)

    assert[String](df, "question_pool_id", "5f450c1f70b7d80001d3a128")
    assert[String](df, "question_pool_name", "test123")
    assert[String](df, "question_pool_triggered_by", "29")
    assert[String](df, "question_pool_question_code_prefix", "test123_updated")
    assert[String](df, "question_pool_app_status", "DRAFT")
    assert[Int](df, "question_pool_status", 1)
  }
}