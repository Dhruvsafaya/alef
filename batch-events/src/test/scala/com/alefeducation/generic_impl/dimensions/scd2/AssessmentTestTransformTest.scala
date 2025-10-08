package com.alefeducation.generic_impl.dimensions.scd2

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.generic_impl.GenericSCD2Transformation
import org.apache.spark.sql.SparkSession
import org.scalatestplus.mockito.MockitoSugar.mock

class AssessmentTestTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val updatedValue =
    """
      |[
      |{
      |  "_load_date": "20250423",
      |  "_load_time": 1745406566081,
      |  "_trace_id": "d10e7b9b-bdda-4467-8695-65eae2ee87e0",
      |  "_source_type": "ALEF-KAFKA",
      |  "_app_tenant": "e7e36084-1d3a-4785-a09e-b97fb4ed88ae",
      |  "_source_app": "SHARED",
      |  "_change_type": "insert",
      |  "_commit_version": 2,
      |  "_commit_timestamp": "2025-04-23T10:20:38.000Z",
      |  "eventType": "TestPublishedDataEvent",
      |  "testParts": "[{\"id\":\"b77fe25c-3ce8-4f4e-bbf5-de0130fe72cf\",\"versionId\":\"64684c6e-cfc8-4a0c-b359-7d007eb52e6c\"},{\"id\":\"tespart-1\",\"versionId\":\"testpart-version-2\"}]",
      |  "versionId": "cefba46f-c893-495e-8e23-b58f81fb443c",
      |  "occurredOn": 1745401263573,
      |  "version": 1,
      |  "id": "c3a85d39-1e1f-427a-b8f9-a6e1005f2463",
      |  "status": "PUBLISHED",
      |  "title": "Prabhakar test",
      |  "userType": "SYSTEM",
      |  "active": true
      |},
      |{
      |  "_load_date": "20250423",
      |  "_load_time": 1745406566081,
      |  "_trace_id": "d10e7b9b-bdda-4467-8695-65eae2ee87e0",
      |  "_source_type": "ALEF-KAFKA",
      |  "_app_tenant": "e7e36084-1d3a-4785-a09e-b97fb4ed88ae",
      |  "_source_app": "SHARED",
      |  "_change_type": "insert",
      |  "_commit_version": 2,
      |  "_commit_timestamp": "2025-04-23T10:20:38.000Z",
      |  "eventType": "TestPublishedDataEvent",
      |  "testParts": "[{\"id\":\"217fe25c-3ce8-4f4e-bbf5-de0130fe72cf\",\"versionId\":\"21684c6e-cfc8-4a0c-b359-7d007eb52e6c\"}]",
      |  "versionId": "newba46f-c893-495e-8e23-b58f81fb443c",
      |  "occurredOn": 1745401263774,
      |  "version": 2,
      |  "id": "c3a85d39-1e1f-427a-b8f9-a6e1005f2463",
      |  "status": "PUBLISHED",
      |  "title": "Prabhakar test 1",
      |  "userType": "SYSTEM",
      |  "active": true
      |}
      |]
      |""".stripMargin

  test("should construct test dimension dataframe when test events mutated event flows") {
    val expectedColumns = Set(
      "dw_id",
      "_trace_id",
      "created_time",
      "dw_created_time",
      "status",
      "active_until",
      "_trace_id",
      "tenant_id",
      "event_type",
      "id",
      "version",
      "version_id",
      "testpart_id",
      "testpart_version_id",
      "app_status",
      "user_type",
      "active",
      "title"
    )

    val sprk = spark
    import sprk.implicits._
    val transformer = new GenericSCD2Transformation("assessment-test-mutated-transform")

    val updatedDF = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map("bronze-assessment-test-source" -> Some(updatedDF)), 0).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 3)
    assert[Int](df, "dw_id", 0)
    assert[String](df, "id", "c3a85d39-1e1f-427a-b8f9-a6e1005f2463")
    assert[String](df, "_trace_id", "d10e7b9b-bdda-4467-8695-65eae2ee87e0")
    assert[String](df, "tenant_id", "e7e36084-1d3a-4785-a09e-b97fb4ed88ae")
    assert[String](df, "event_type", "TestPublishedDataEvent")
    assert[String](df, "version_id", "cefba46f-c893-495e-8e23-b58f81fb443c")
    assert[Int](df, "version", 1)
    assert[String](df, "app_status", "PUBLISHED")
    assert[String](df, "user_type", "SYSTEM")
    assert[Boolean](df, "active", true)
    assert[String](df, "title", "Prabhakar test")
    assert[Int](df, "status", 2)
  }

  test("should construct test dimension dataframe when no testparts") {
    val updatedValue =
      """
        |[
        |{
        |  "_load_date": "20250423",
        |  "_load_time": 1745406566081,
        |  "_trace_id": "d10e7b9b-bdda-4467-8695-65eae2ee87e0",
        |  "_source_type": "ALEF-KAFKA",
        |  "_app_tenant": "e7e36084-1d3a-4785-a09e-b97fb4ed88ae",
        |  "_source_app": "SHARED",
        |  "_change_type": "insert",
        |  "_commit_version": 2,
        |  "_commit_timestamp": "2025-04-23T10:20:38.000Z",
        |  "eventType": "TestPublishedDataEvent",
        |  "testParts": "[]",
        |  "versionId": "cefba46f-c893-495e-8e23-b58f81fb443c",
        |  "occurredOn": 1745401263573,
        |  "version": 1,
        |  "id": "c3a85d39-1e1f-427a-b8f9-a6e1005f2463",
        |  "status": "PUBLISHED",
        |  "title": "Prabhakar test",
        |  "userType": "SYSTEM",
        |  "active": true
        |}
        |]
        |""".stripMargin
    val expectedColumns = Set(
      "dw_id",
      "_trace_id",
      "created_time",
      "dw_created_time",
      "status",
      "active_until",
      "_trace_id",
      "tenant_id",
      "event_type",
      "id",
      "version",
      "version_id",
      "app_status",
      "user_type",
      "active",
      "title"
    )

    val sprk = spark
    import sprk.implicits._
    val transformer = new GenericSCD2Transformation("assessment-test-mutated-transform")

    val updatedDF = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map("bronze-assessment-test-source" -> Some(updatedDF)), 0).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    assert[Int](df, "dw_id", 0)
    assert[String](df, "id", "c3a85d39-1e1f-427a-b8f9-a6e1005f2463")
    assert[String](df, "_trace_id", "d10e7b9b-bdda-4467-8695-65eae2ee87e0")
    assert[String](df, "tenant_id", "e7e36084-1d3a-4785-a09e-b97fb4ed88ae")
    assert[String](df, "event_type", "TestPublishedDataEvent")
    assert[String](df, "version_id", "cefba46f-c893-495e-8e23-b58f81fb443c")
    assert[Int](df, "version", 1)
    assert[String](df, "app_status", "PUBLISHED")
    assert[String](df, "user_type", "SYSTEM")
    assert[Boolean](df, "active", true)
    assert[String](df, "title", "Prabhakar test")
    assert[Int](df, "status", 1)
  }
}
