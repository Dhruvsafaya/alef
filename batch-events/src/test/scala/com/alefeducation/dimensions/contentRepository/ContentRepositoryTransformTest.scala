package com.alefeducation.dimensions.contentRepository

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.contentRepository.transform.ContentRepositoryTransform
import com.alefeducation.dimensions.contentRepository.transform.ContentRepositoryTransform.{ContentRepositoryMutatedParquetSource, ContentRepositoryTransformedCreatedSink}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.collection.immutable


class ContentRepositoryTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns = List(
    "content_repository_id",
    "content_repository_name",
    "content_repository_organisation_owner",
    "content_repository_status",
    "content_repository_created_time",
    "content_repository_dw_created_time",
    "content_repository_dw_updated_time",
    "content_repository_updated_time",
  ).toSet

  test("should transform contentRepository created event") {
    val value =
      """
        |[
        |{
        |  "eventType": "ContentRepositoryCreatedEvent",
        |  "id": "93ef3952-e606-4be2-8a08-35b692a67219",
        |  "name": "testorg2sep21",
        |  "organisation": "Arabits",
        |  "occurredOn": "2021-06-23 05:33:24.921"
        |}
        |]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._
    val transformer = new ContentRepositoryTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ContentRepositoryMutatedParquetSource, sprk)).thenReturn(Some(inputDF))

    val sinks = transformer.transform()
    val df  = sinks.filter(_.get.name == ContentRepositoryTransformedCreatedSink).head.get.output

    assert(df)
    assert[String](df, "content_repository_organisation_owner", "Arabits")
  }

  test("should transform dwIdMapping DF") {
    val value =
      """
        |[
        |{
        |  "eventType": "ContentRepositoryCreatedEvent",
        |  "id": "93ef3952-e606-4be2-8a08-35b692a67219",
        |  "name": "testorg2sep21",
        |  "organisation": "Mora",
        |  "occurredOn": "2021-06-23 05:33:24.921"
        |}
        |]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._
    val transformer = new ContentRepositoryTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ContentRepositoryMutatedParquetSource, sprk)).thenReturn(Some(inputDF))
    val sinks = transformer.transform()
    val df  = sinks.filter(_.get.name == "content-repository-dw-id-mapping-sink").head.get.output
    assert[String](df, "id", "93ef3952-e606-4be2-8a08-35b692a67219")
    assert[String](df, "entity_type", "content-repository")
  }

  private def assert(df: DataFrame): Unit = {
    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "content_repository_id", "93ef3952-e606-4be2-8a08-35b692a67219")
    assert[String](df, "content_repository_name", "testorg2sep21")
    assert[Int](df, "content_repository_status", 1)
    assert[String](df, "content_repository_created_time", "2021-06-23 05:33:24.921")
  }
}
