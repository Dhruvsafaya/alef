package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class GenericDimSCDTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]
  test("should construct attach adt dataframe when time field is in seconds") {
    val updatedValue =
      """
        |[
        |{
        | "eventType": "AttemptThresholdCreated",
        |	"id": "65dd8751d130a58ae9582819",
        | "academicYearId": "academic_year_id",
        |  "tenantId": "tenant-id-1",
        | "schoolId": "c7378da9-76fb-432c-8181-1eb970b3982f",
        | "schoolName": "Targaryen of King's Landing",
        | "attempts": [
        |   {
        |     "attemptNumber": 1,
        |     "startTime" : 1724965200,
        |     "endTime" : 1732913999,
        |     "attemptTitle": "Test 1"
        |   },
        |   {
        |     "attemptNumber": 2,
        |     "startTime" : 1724965200,
        |     "endTime" : 1732913999,
        |     "attemptTitle": "Test 2"
        |   }
        | ],
        | "status": "ACTIVE",
        | "numberOfAttempts": 2,
        |	"occurredOn": "2021-06-23 05:33:24.921"
        |},
        |{
        | "eventType": "AttemptThresholdUpdated",
        |	"id": "65dd8751d130a58ae9582819",
        | "academicYearId": "academic_year_id",
        | "tenantId": "tenant-id-1",
        | "schoolId": "c7378da9-76fb-432c-8181-1eb970b3982f",
        | "schoolName": "Targaryen of King's Landing",
        | "attempts": [
        |   {
        |     "attemptNumber": 1,
        |     "startTime" : 1724965200,
        |     "endTime" : 1732913999,
        |     "attemptTitle": "Test 1"
        |   },
        |   {
        |     "attemptNumber": 2,
        |     "startTime" : 1724965200,
        |     "endTime" : 1732913999,
        |     "attemptTitle": "Test 2"
        |   }
        | ],
        | "status": "ACTIVE",
        | "numberOfAttempts": 2,
        |	"occurredOn": "2021-06-23 05:33:25.921"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "aat_dw_id",
      "aat_created_time",
      "aat_dw_created_time",
      "aat_updated_time",
      "aat_dw_updated_time",
      "aat_id",
      "aat_status",
      "aat_tenant_id",
      "aat_academic_year_id",
      "aat_school_id",
      "aat_attempt_start_time",
      "aat_attempt_end_time",
      "aat_attempt_number",
      "aat_attempt_title",
      "aat_total_attempts",
      "aat_state"
    )

    val sprk = spark
    import sprk.implicits._
    val transformer = new GenericDimSCDTransform(sprk, service, "adt-attempt-threshold-mutated-transform")

    val updatedDF = spark.read.json(Seq(updatedValue).toDS())
    when(service.readOptional("parquet-adt-attempt-threshold-mutated-source", sprk)).thenReturn(Some(updatedDF))

    val sinks = transformer.transform()

    val df = sinks.filter(_.name == "transformed-adt-attempt-threshold-mutated-sink").head.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 4)
    assert[String](df, "aat_id", "65dd8751d130a58ae9582819")
    assert[String](df, "aat_tenant_id", "tenant-id-1")
    assert[String](df, "aat_school_id", "c7378da9-76fb-432c-8181-1eb970b3982f")
    assert[String](df, "aat_created_time", "2021-06-23 05:33:24.921")
    assert[String](df, "aat_updated_time", "2021-06-23 05:33:25.921")
    assert[String](df, "aat_attempt_start_time", "2024-08-29 21:00:00.0")
    assert[String](df, "aat_attempt_end_time", "2024-11-29 20:59:59.0")
    assert[String](df, "aat_state", "ACTIVE")
    assert[Int](df, "aat_status", 2)
  }

  test("should construct attach adt dataframe when time field is in millis") {
    val updatedValue =
      """
        |[
        |{
        | "eventType": "AttemptThresholdCreated",
        |	"id": "65dd8751d130a58ae9582819",
        | "academicYearId": "academic_year_id",
        |  "tenantId": "tenant-id-1",
        | "schoolId": "c7378da9-76fb-432c-8181-1eb970b3982f",
        | "schoolName": "Targaryen of King's Landing",
        | "attempts": [
        |   {
        |     "attemptNumber": 1,
        |     "startTime" : 1721692800213,
        |     "endTime" : 1721692800213,
        |     "attemptTitle": "Test 1"
        |   },
        |   {
        |     "attemptNumber": 2,
        |     "startTime" : 1732913999,
        |     "endTime" : 1732913999,
        |     "attemptTitle": "Test 2"
        |   }
        | ],
        | "status": "ACTIVE",
        | "numberOfAttempts": 2,
        |	"occurredOn": "2021-06-23 05:33:24.921"
        |},
        |{
        | "eventType": "AttemptThresholdUpdated",
        |	"id": "65dd8751d130a58ae9582819",
        | "academicYearId": "academic_year_id",
        | "tenantId": "tenant-id-1",
        | "schoolId": "c7378da9-76fb-432c-8181-1eb970b3982f",
        | "schoolName": "Targaryen of King's Landing",
        | "attempts": [
        |   {
        |     "attemptNumber": 1,
        |     "startTime" : 1724965200,
        |     "endTime" : 1732913999,
        |     "attemptTitle": "Test 1"
        |   },
        |   {
        |     "attemptNumber": 2,
        |     "startTime" : 1724965200,
        |     "endTime" : 1732913999,
        |     "attemptTitle": "Test 2"
        |   }
        | ],
        | "status": "ACTIVE",
        | "numberOfAttempts": 2,
        |	"occurredOn": "2021-06-23 05:33:25.921"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "aat_dw_id",
      "aat_created_time",
      "aat_dw_created_time",
      "aat_updated_time",
      "aat_dw_updated_time",
      "aat_id",
      "aat_status",
      "aat_tenant_id",
      "aat_academic_year_id",
      "aat_school_id",
      "aat_attempt_start_time",
      "aat_attempt_end_time",
      "aat_attempt_number",
      "aat_attempt_title",
      "aat_total_attempts",
      "aat_state"
    )

    val sprk = spark
    import sprk.implicits._
    val transformer = new GenericDimSCDTransform(sprk, service, "adt-attempt-threshold-mutated-transform")

    val updatedDF = spark.read.json(Seq(updatedValue).toDS())
    when(service.readOptional("parquet-adt-attempt-threshold-mutated-source", sprk)).thenReturn(Some(updatedDF))

    val sinks = transformer.transform()

    val df = sinks.filter(_.name == "transformed-adt-attempt-threshold-mutated-sink")(0).output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 4)
    assert[String](df, "aat_id", "65dd8751d130a58ae9582819")
    assert[String](df, "aat_tenant_id", "tenant-id-1")
    assert[String](df, "aat_school_id", "c7378da9-76fb-432c-8181-1eb970b3982f")
    assert[String](df, "aat_created_time", "2021-06-23 05:33:24.921")
    assert[String](df, "aat_updated_time", "2021-06-23 05:33:25.921")
    assert[String](df, "aat_attempt_start_time", "2024-07-23 00:00:00.213")
    assert[String](df, "aat_attempt_end_time", "2024-07-23 00:00:00.213")
    assert[String](df, "aat_state", "ACTIVE")
    assert[Int](df, "aat_status", 2)
  }

}
