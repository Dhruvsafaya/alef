package com.alefeducation.generic_impl.dimensions.scd2

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.generic_impl.GenericSCD2Transformation
import org.apache.spark.sql.SparkSession
import org.scalatestplus.mockito.MockitoSugar.mock

class AssessmentTestpartTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val updatedValue =
    """
      |[
      |{
      |	    "id": "b77fe25c-3ce8-4f4e-bbf5-de0130fe72cf",
      |     "_trace_id": "trace-id-1",
      |     "_app_tenant": "tenant-id",
      |	    "eventType": "TestPartPublishedDataEvent",
      |     "versionId": "64684c6e-cfc8-4a0c-b359-7d007eb52e6c",
      |	    "publishedOn": "2025-04-23T09:37:18.372189564",
      |	    "version": 1,
      |	    "status": "PUBLISHED",
      |	    "userId": "3c5bfccc-3a97-41e9-a0c1-6e30ca711959",
      |	    "userType": "USER",
      |	    "active": true,
      |	    "title": "Prabhakar Test April",
      |	    "type": "Benchmark Test",
      |	    "subject": "Arabic",
      |	    "language": "Arabic",
      |	    "selectionMode": "Linear",
      |	    "skill": "Listening",
      |	    "referenceCode": "prabhakar_02",
      |	    "occurredOn": 1745401038702
      |},
      |{
      |	    "id": "b77fe25c-3ce8-4f4e-bbf5-de0130fe72cf",
      |     "_trace_id": "trace-id-1",
      |     "_app_tenant": "tenant-id",
      |	    "eventType": "TestPartPublishedDataEvent",
      |     "versionId": "22684c6e-cfc8-4a0c-b359-7d007eb52e6c",
      |	    "publishedOn": "2025-04-23T09:37:18.372189564",
      |	    "version": 2,
      |	    "status": "PUBLISHED",
      |	    "userId": "3c5bfccc-3a97-41e9-a0c1-6e30ca711959",
      |	    "userType": "USER",
      |	    "active": true,
      |	    "title": "Prabhakar Test April",
      |	    "type": "Benchmark Test",
      |	    "subject": "Arabic",
      |	    "language": "Arabic",
      |	    "selectionMode": "Seletive",
      |	    "skill": "Listening",
      |	    "referenceCode": "prabhakar_02",
      |	    "occurredOn": 1745401038802
      |}
      |]
      |""".stripMargin

  test("should construct pathway target dimension dataframe when Pathway events mutated event flows") {
    val expectedColumns = Set(
      "dw_id",
      "id",
      "_trace_id",
      "created_time",
      "dw_created_time",
      "status",
      "active_until",
      "_trace_id",
      "tenant_id",
      "event_type",
      "version_id",
      "published_on",
      "id",
      "version",
      "app_status",
      "user_id",
      "user_type",
      "active",
      "title",
      "type",
      "subject",
      "language",
      "selection_mode",
      "skill",
      "reference_code"
    )

    val sprk = spark
    import sprk.implicits._
    val transformer = new GenericSCD2Transformation("assessment-testpart-mutated-transform")

    val updatedDF = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map("bronze-assessment-testpart-source" -> Some(updatedDF)), 0).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)
    assert[Int](df, "dw_id", 0)
    assert[String](df, "id", "b77fe25c-3ce8-4f4e-bbf5-de0130fe72cf")
    assert[String](df, "_trace_id", "trace-id-1")
    assert[String](df, "tenant_id", "tenant-id")
    assert[String](df, "event_type", "TestPartPublishedDataEvent")
    assert[String](df, "version_id", "64684c6e-cfc8-4a0c-b359-7d007eb52e6c")
    assert[String](df, "published_on", "2025-04-23 09:37:18.372189")
    assert[Int](df, "version", 1)
    assert[String](df, "app_status", "PUBLISHED")
    assert[String](df, "user_id", "3c5bfccc-3a97-41e9-a0c1-6e30ca711959")
    assert[String](df, "user_type", "USER")
    assert[Boolean](df, "active", true)
    assert[String](df, "title", "Prabhakar Test April")
    assert[String](df, "type", "Benchmark Test")
    assert[String](df, "subject", "Arabic")
    assert[String](df, "language", "Arabic")
    assert[String](df, "selection_mode", "Linear")
    assert[Int](df, "status", 2)
  }
}
