package com.alefeducation.dimensions.staff

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.staff.StaffRelUserTransform.StaffRelUserTransformService
import com.alefeducation.util.DataFrameEqualityUtils.assertSmallDatasetEquality
import com.alefeducation.util.Resources.getSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Timestamp

class StaffRelUserTransformSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns: List[String] = List(
    "user_id",
    "user_type",
    "user_created_time",
    "user_dw_created_time",
  )

  test("should prepare dataframe for rel user table when both events with role and membershipsV3") {

    val value =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "UserCreatedEvent",
        |  "uuid": "be40f7a9-6fcb-4239-9fee-c9807f93f51b",
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
        |   "eventType":"UserCreatedEvent",
        |   "occurredOn": "2024-01-08 02:50:00.0",
        |   "uuid":"bfe6e7e1-a78c-4317-b0a0-8d37328e9486",
        |   "avatar":null,
        |   "onboarded":null,
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
        |},
        |{
        |   "eventType":"UserUpdatedEvent",
        |   "occurredOn":"2024-01-09 02:55:00.0",
        |   "uuid":"60a7375d-a6f7-4988-8f54-2a33ff85fe8e",
        |   "avatar":null,
        |   "onboarded":null,
        |   "enabled":true,
        |   "membershipsV3": [
        |    {
        |        "role": "TEACHER",
        |        "schoolId": "Uuid1",
        |        "organization": "DEMO"
        |    },
        |    {
        |        "role": "TDC",
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

    val sourceName = getSource(StaffRelUserTransformService).head
    val inputDf = spark.read.json(Seq(value).toDS())

    when(service.readUniqueOptional(sourceName, sprk, uniqueColNames = Seq("uuid"))).thenReturn(Some(inputDf))

    val transformer = new StaffRelUserTransform(sprk, service, StaffRelUserTransformService)

    val sink = transformer.transform()

    val df = sink.head.output

    assert(df.columns.toSet === expectedColumns.toSet)

    val expectedDF = List(
      ("be40f7a9-6fcb-4239-9fee-c9807f93f51b", "ADMIN", Timestamp.valueOf("2024-01-08 02:40:00"), Timestamp.valueOf("2024-06-25 11:26:36")),
      ("bfe6e7e1-a78c-4317-b0a0-8d37328e9486", "ADMIN", Timestamp.valueOf("2024-01-08 02:50:00"), Timestamp.valueOf("2024-06-25 11:26:36")),
      ("60a7375d-a6f7-4988-8f54-2a33ff85fe8e", "ADMIN", Timestamp.valueOf("2024-01-09 02:55:00"), Timestamp.valueOf("2024-06-25 11:26:36"))
    ).toDF(expectedColumns: _*)

    assertSmallDatasetEquality("user", df, expectedDF)
  }

  test("should prepare dataframe for rel user table when event has only role and without membershipsV3") {

    val value =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "UserCreatedEvent",
        |  "uuid": "be40f7a9-6fcb-4239-9fee-c9807f93f51b",
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
        |}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val sourceName = getSource(StaffRelUserTransformService).head
    val inputDf = spark.read.json(Seq(value).toDS())

    when(service.readUniqueOptional(sourceName, sprk, uniqueColNames = Seq("uuid"))).thenReturn(Some(inputDf))

    val transformer = new StaffRelUserTransform(sprk, service, StaffRelUserTransformService)

    val sink = transformer.transform()

    val df = sink.head.output

    assert(df.columns.toSet === expectedColumns.toSet)

    val expectedDF = List(
      ("be40f7a9-6fcb-4239-9fee-c9807f93f51b", "ADMIN", Timestamp.valueOf("2024-01-08 02:40:00"), Timestamp.valueOf("2024-06-25 11:26:36"))
    ).toDF(expectedColumns: _*)

    assertSmallDatasetEquality("user", df, expectedDF)
  }

  test("should prepare dataframe for rel user table when event has only membershipsV3") {

    val value =
      """
        |[
        |{
        |   "eventType":"UserUpdatedEvent",
        |   "occurredOn":"2024-01-09 02:55:00.0",
        |   "uuid":"60a7375d-a6f7-4988-8f54-2a33ff85fe8e",
        |   "username":"60a7375d-a6f7-4988-8f54-2a33ff85fe8e",
        |   "phoneNumber":"+9715008500372",
        |   "firstName":"Abagail",
        |   "middleName":"",
        |   "lastName":"Willow",
        |   "avatar":null,
        |   "onboarded":null,
        |   "enabled":true,
        |   "membershipsV3": [
        |    {
        |        "role": "TEACHER",
        |        "schoolId": "Uuid1",
        |        "organization": "DEMO"
        |    },
        |    {
        |        "role": "TDC",
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

    val sourceName = getSource(StaffRelUserTransformService).head
    val inputDf = spark.read.json(Seq(value).toDS())

    when(service.readUniqueOptional(sourceName, sprk, uniqueColNames = Seq("uuid"))).thenReturn(Some(inputDf))

    val transformer = new StaffRelUserTransform(sprk, service, StaffRelUserTransformService)

    val sink = transformer.transform()

    val df = sink.head.output

    assert(df.columns.toSet === expectedColumns.toSet)

    val expectedDF = List(
      ("60a7375d-a6f7-4988-8f54-2a33ff85fe8e", "ADMIN", Timestamp.valueOf("2024-01-09 02:55:00"), Timestamp.valueOf("2024-06-25 11:26:36"))
    ).toDF(expectedColumns: _*)

    assertSmallDatasetEquality("user", df, expectedDF)
  }
}
