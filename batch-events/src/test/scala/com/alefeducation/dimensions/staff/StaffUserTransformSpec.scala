package com.alefeducation.dimensions.staff

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.staff.StaffUserHelper._
import com.alefeducation.dimensions.staff.StaffUserTransform.StaffUserTransformService
import com.alefeducation.util.DataFrameEqualityUtils.assertSmallDatasetEquality
import com.alefeducation.util.Resources.getNestedString
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.BooleanType
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Timestamp
import java.util.TimeZone

class StaffUserTransformSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns: List[String] = List(
    "staff_user_avatar",
    "staff_user_id",
    "staff_user_onboarded",
    "staff_user_exclude_from_report",
    "staff_user_expirable",
    "staff_user_status",
    "staff_user_created_time",
    "staff_user_dw_created_time",
    "staff_user_active_until",
    "staff_user_event_type",
    "rel_staff_user_dw_id",
    "staff_user_enabled"
  )

  test("user created, deleted events") {
    val value =
      """
        |[
        |{
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "eventType": "UserCreatedEvent",
        |  "uuid": "be40f7a9-6fcb-4239-9fee-c9807f93f51b",
        |
        |  "enabled": true,
        |  "school": {"uuid":"school-id"},
        |  "avatar": null,
        |  "onboarded": true,
        |  "expirable":true,
        |  "role":"TDC",
        |  "roles":["TDC"],
        |  "permissions":["ALL_TDC_PERMISSIONS"],
        |  "occurredOn": "2024-01-08 02:40:00.0",
        |  "excludeFromReport": true
        |},
        |{
        |   "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |   "eventType":"UserCreatedEvent",
        |   "occurredOn": "2024-01-08 02:50:00.0",
        |   "uuid":"bfe6e7e1-a78c-4317-b0a0-8d37328e9486",
        |   "avatar":null,
        |   "onboarded":false,
        |   "enabled":true,
        |   "membershipsV3": [
        |    {
        |        "role": "ADMIN",
        |        "schoolId": "Uuid1",
        |        "organization": "DEMO"
        |    }
        |   ],
        |   "createdOn": 1643348619255,
        |   "updatedOn": 1643348619255,
        |   "expirable":true,
        |   "associationGrades": [3, 4],
        |   "excludeFromReport": false
        |}
        |]
        |""".stripMargin

    val deletedEvents =
      """
        |[
        |{
        |   "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |   "eventType":"UserDeletedEvent",
        |   "occurredOn": "2024-01-08 02:50:00.0",
        |   "uuid":"bf38ff2c-d255-445f-ae1e-86b10cff99ac"
        |}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val staffSourceName = getNestedString(StaffUserTransformService, StaffSourceName)
    val staffDeletedName = getNestedString(StaffUserTransformService, StaffDeletedSourceName)
    val inputDf = spark.read.json(Seq(value).toDS())
    val inputDeletedDf = spark.read.json(Seq(deletedEvents).toDS())
    when(service.readOptional(staffSourceName, sprk, extraProps = ExtraProps)).thenReturn(Some(inputDf))
    when(service.readOptional(staffDeletedName, sprk, extraProps = ExtraProps)).thenReturn(Some(inputDeletedDf))
    when(service.getStartIdUpdateStatus(StaffUserIdKey)).thenReturn(2001)

    val transform = new StaffUserTransform(sprk, service, StaffUserTransformService)

    val sink = transform.transform()

    val df = sink.head.output

    assert(df.columns.toSet === expectedColumns.toSet)

    val expectedDF = List(
      (null, "be40f7a9-6fcb-4239-9fee-c9807f93f51b", "true", "true", "true", 1, Timestamp.valueOf("2024-01-08 02:40:00"), Timestamp.valueOf("2024-06-26 11:39:46"), null, "UserCreatedEvent", 2001, true),
      (null, "bf38ff2c-d255-445f-ae1e-86b10cff99ac", "null", "null", "null", 4, Timestamp.valueOf("2024-01-08 02:50:00"), Timestamp.valueOf("2024-06-26 11:39:46"), null, "UserDeletedEvent", 2002, false),
      (null, "bfe6e7e1-a78c-4317-b0a0-8d37328e9486", "false", "false", "true", 1, Timestamp.valueOf("2024-01-08 02:50:00"), Timestamp.valueOf("2024-06-26 11:39:46"), null, "UserCreatedEvent", 2003, true)
    ).toDF(expectedColumns: _*)
      .withColumn("staff_user_onboarded", col("staff_user_onboarded").cast(BooleanType))
      .withColumn("staff_user_exclude_from_report", col("staff_user_exclude_from_report").cast(BooleanType))
      .withColumn("staff_user_expirable", col("staff_user_expirable").cast(BooleanType))

    assertSmallDatasetEquality(StaffUserEntity, df, expectedDF)
  }

  test("user updated event with user enabled") {

    val value =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "UserUpdatedEvent",
        |  "uuid": "7365e0e0-4d03-463a-b0e9-a62dba7d141e",
        |  "enabled": true,
        |  "school": {"uuid":"school-id"},
        |  "avatar": null,
        |  "onboarded": true,
        |  "expirable":true,
        |  "role":"TDC",
        |  "roles":["TDC"],
        |  "permissions":["ALL_TDC_PERMISSIONS"],
        |  "occurredOn": "2024-01-08 02:40:00.0",
        |  "excludeFromReport": false
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "UserEnabledEvent",
        |  "uuid": "7365e0e0-4d03-463a-b0e9-a62dba7d141e",
        |  "enabled": true,
        |  "school": {"uuid":"school-id"},
        |  "avatar": null,
        |  "onboarded": true,
        |  "expirable":true,
        |  "role":"TDC",
        |  "roles":["TDC"],
        |  "permissions":["ALL_TDC_PERMISSIONS"],
        |  "occurredOn": "2024-01-08 02:40:00.0",
        |  "excludeFromReport": false
        |},
        |{
        |   "eventType":"UserUpdatedEvent",
        |   "occurredOn":"2024-01-08 02:55:00.0",
        |   "uuid":"60a7375d-a6f7-4988-8f54-2a33ff85fe8e",
        |   "avatar":null,
        |   "onboarded":false,
        |   "enabled":true,
        |   "membershipsV3": [
        |    {
        |        "role": "TEACHER",
        |        "schoolId": "Uuid1",
        |        "organization": "DEMO"
        |    },
        |    {
        |        "role": "ADMIN",
        |        "schoolId": "Uuid2",
        |        "organization": "DEMO"
        |    }
        |   ],
        |   "createdOn": 1643348619255,
        |   "updatedOn": 1643348619290,
        |   "expirable":true,
        |   "associationGrades": null,
        |   "excludeFromReport": false
        |}
        |]
        |""".stripMargin


    val sprk = spark
    import sprk.implicits._

    val staffSourceName = getNestedString(StaffUserTransformService, StaffSourceName)
    val staffDeletedName = getNestedString(StaffUserTransformService, StaffDeletedSourceName)
    val inputDf = spark.read.json(Seq(value).toDS())
    when(service.readOptional(staffSourceName, sprk, extraProps = ExtraProps)).thenReturn(Some(inputDf))
    when(service.readOptional(staffDeletedName, sprk, extraProps = ExtraProps)).thenReturn(None)
    when(service.getStartIdUpdateStatus(StaffUserIdKey)).thenReturn(1001)

    val transform = new StaffUserTransform(sprk, service, StaffUserTransformService)

    val sink = transform.transform()

    val df = sink.head.output

    assert(df.columns.toSet === expectedColumns.toSet)

    val expectedDF = List(
      (null, "7365e0e0-4d03-463a-b0e9-a62dba7d141e", true, false, true, 1, Timestamp.valueOf("2024-01-08 02:40:00"), Timestamp.valueOf("2024-06-26 11:39:46"), null, "UserUpdatedEvent", 1001, true),
      (null, "60a7375d-a6f7-4988-8f54-2a33ff85fe8e", false, false, true, 1, Timestamp.valueOf("2024-01-08 02:55:00"), Timestamp.valueOf("2024-06-26 11:39:46"), null, "UserUpdatedEvent", 1002, true)
    ).toDF(expectedColumns: _*)

    assertSmallDatasetEquality(StaffUserEntity, df, expectedDF)
  }

  test("User updated events with both disabled and enabled") {

    val value =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "UserUpdatedEvent",
        |  "uuid": "7365e0e0-4d03-463a-b0e9-a62dba7d141e",
        |  "enabled": true,
        |  "school": {"uuid":"school-id"},
        |  "avatar": null,
        |  "onboarded": true,
        |  "expirable":true,
        |  "role":"TDC",
        |  "roles":["TDC"],
        |  "permissions":["ALL_TDC_PERMISSIONS"],
        |  "occurredOn": "2024-01-08 02:40:00.0",
        |  "excludeFromReport": false
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "UserEnabledEvent",
        |  "uuid": "7365e0e0-4d03-463a-b0e9-a62dba7d141e",
        |  "enabled": true,
        |  "school": {"uuid":"school-id"},
        |  "avatar": null,
        |  "onboarded": true,
        |  "expirable":true,
        |  "role":"TDC",
        |  "roles":["TDC"],
        |  "permissions":["ALL_TDC_PERMISSIONS"],
        |  "occurredOn": "2024-01-08 02:40:00.0",
        |  "excludeFromReport": false
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "UserUpdatedEvent",
        |  "uuid": "7365e0e0-4d03-463a-b0e9-a62dba7d141e",
        |  "enabled": false,
        |  "school": {"uuid":"school-id"},
        |  "avatar": null,
        |  "onboarded": true,
        |  "expirable":true,
        |  "role":"TDC",
        |  "roles":["TDC"],
        |  "permissions":["ALL_TDC_PERMISSIONS"],
        |  "occurredOn": "2024-01-09 02:40:00.0",
        |  "excludeFromReport": false
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "UserDisabledEvent",
        |  "uuid": "7365e0e0-4d03-463a-b0e9-a62dba7d141e",
        |  "enabled": false,
        |  "school": {"uuid":"school-id"},
        |  "avatar": null,
        |  "onboarded": true,
        |  "expirable":true,
        |  "role":"TDC",
        |  "roles":["TDC"],
        |  "permissions":["ALL_TDC_PERMISSIONS"],
        |  "occurredOn": "2024-01-09 02:40:00.0",
        |  "excludeFromReport": false
        |}
        |]
        |""".stripMargin


    val sprk = spark
    import sprk.implicits._

    val staffSourceName = getNestedString(StaffUserTransformService, StaffSourceName)
    val staffDeletedName = getNestedString(StaffUserTransformService, StaffDeletedSourceName)
    val inputDf = spark.read.json(Seq(value).toDS())
    when(service.readOptional(staffSourceName, sprk, extraProps = ExtraProps)).thenReturn(Some(inputDf))
    when(service.readOptional(staffDeletedName, sprk, extraProps = ExtraProps)).thenReturn(None)
    when(service.getStartIdUpdateStatus(StaffUserIdKey)).thenReturn(1001)

    val transform = new StaffUserTransform(sprk, service, StaffUserTransformService)

    val sink = transform.transform()

    val df = sink.head.output

    assert(df.columns.toSet === expectedColumns.toSet)

    val expectedDF = List(
      (null, "7365e0e0-4d03-463a-b0e9-a62dba7d141e", true, false, true, 2, Timestamp.valueOf("2024-01-08 02:40:00"), Timestamp.valueOf("2024-06-26 11:39:46"), Timestamp.valueOf("2024-01-09 02:40:00"), "UserUpdatedEvent", 1001, true),
      (null, "7365e0e0-4d03-463a-b0e9-a62dba7d141e", true, false, true, 1, Timestamp.valueOf("2024-01-09 02:40:00"), Timestamp.valueOf("2024-06-26 11:39:46"), null, "UserUpdatedEvent", 1002, false)
    ).toDF(expectedColumns: _*)

    assertSmallDatasetEquality(StaffUserEntity, df, expectedDF)
  }

  test("user updated event with student role only") {

    val value =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "UserUpdatedEvent",
        |  "uuid": "7365e0e0-4d03-463a-b0e9-a62dba7d141e",
        |  "enabled": true,
        |  "school": {"uuid":"school-id"},
        |  "avatar": null,
        |  "onboarded": true,
        |  "expirable":true,
        |  "role":"STUDENT",
        |  "roles":["STUDENT"],
        |  "permissions":["STUDY"],
        |  "occurredOn": "2024-01-08 02:40:00.0",
        |  "excludeFromReport": false
        |},
        |{
        |   "eventType":"UserUpdatedEvent",
        |   "occurredOn":"2024-01-08 02:55:00.0",
        |   "uuid":"60a7375d-a6f7-4988-8f54-2a33ff85fe8e",
        |   "avatar":null,
        |   "onboarded":false,
        |   "enabled":true,
        |   "membershipsV3": [
        |    {
        |        "role": "TEACHER",
        |        "schoolId": "Uuid1",
        |        "organization": "DEMO"
        |    },
        |    {
        |        "role": "STUDENT",
        |        "schoolId": "Uuid2",
        |        "organization": "DEMO"
        |    }
        |   ],
        |   "createdOn": 1643348619255,
        |   "updatedOn": 1643348619290,
        |   "expirable":true,
        |   "associationGrades": null,
        |   "excludeFromReport": false
        |},
        |{
        |   "eventType":"UserUpdatedEvent",
        |   "occurredOn":"2024-01-08 02:57:00.0",
        |   "uuid":"12906528-dada-45eb-bd8a-8556b007c25d",
        |   "avatar":null,
        |   "onboarded":false,
        |   "enabled":true,
        |   "membershipsV3": [
        |    {
        |        "role": "TEACHER",
        |        "schoolId": "Uuid1",
        |        "organization": "DEMO"
        |    },
        |    {
        |        "role": "ADMIN",
        |        "schoolId": "Uuid2",
        |        "organization": "DEMO"
        |    },
        |    {
        |        "role": "PRINCIPAL",
        |        "schoolId": "Uuid2",
        |        "organization": "DEMO"
        |    }
        |   ],
        |   "createdOn": 1643348619256,
        |   "updatedOn": 1643348619294,
        |   "expirable":true,
        |   "associationGrades": null,
        |   "excludeFromReport": false
        |}
        |]
        |""".stripMargin


    val sprk = spark
    import sprk.implicits._

    val staffSourceName = getNestedString(StaffUserTransformService, StaffSourceName)
    val staffDeletedName = getNestedString(StaffUserTransformService, StaffDeletedSourceName)
    val inputDf = spark.read.json(Seq(value).toDS())
    when(service.readOptional(staffSourceName, sprk, extraProps = ExtraProps)).thenReturn(Some(inputDf))
    when(service.readOptional(staffDeletedName, sprk, extraProps = ExtraProps)).thenReturn(None)
    when(service.getStartIdUpdateStatus(StaffUserIdKey)).thenReturn(1001)

    val transform = new StaffUserTransform(sprk, service, StaffUserTransformService)

    val sink = transform.transform()

    val df = sink.head.output

    assert(df.columns.toSet === expectedColumns.toSet)

    val expectedDF = List(
      (null, "12906528-dada-45eb-bd8a-8556b007c25d", false, false, true, 1, Timestamp.valueOf("2024-01-08 02:57:00"), Timestamp.valueOf("2024-06-26 11:39:46"), null, "UserUpdatedEvent", 1001, true)
    ).toDF(expectedColumns: _*)

    assertSmallDatasetEquality(StaffUserEntity, df, expectedDF)
  }

}
