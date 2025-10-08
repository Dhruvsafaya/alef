package com.alefeducation.dimensions.staff

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.staff.StaffUserHelper.{ExtraProps, RedshiftRoleDim, StaffSourceName, StaffUserSchoolRoleAssociationIdKey, StaffUserSchoolRoleEntity}
import com.alefeducation.dimensions.staff.StaffUserSchoolRoleAssociationTransform.{StaffSchoolMovedSourceName, StaffUserSchoolRoleAssociationTransformService}
import com.alefeducation.models.RoleModel.RoleUuid
import com.alefeducation.util.DataFrameEqualityUtils.assertSmallDatasetEquality
import com.alefeducation.util.Resources.getNestedString
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Timestamp

class StaffUserSchoolRoleAssociationTransformSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns: List[String] = List(
    "susra_staff_id",
    "susra_organization",
    "susra_active_until",
    "susra_status",
    "susra_school_id",
    "susra_role_name",
    "susra_role_uuid",
    "susra_created_time",
    "susra_dw_created_time",
    "susra_event_type",
    "susra_dw_id"
  )

  test("create data frame with staff, schools and roles from create event") {
    val value =
      """
        |[
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "UserCreatedEvent",
        |  "uuid": "be40f7a9-6fcb-4239-9fee-c9807f93f51b",
        |  "enabled": true,
        |  "school": {"uuid":"9b82537d-2617-4dcd-982b-ee0c23ee97de"},
        |  "avatar": null,
        |  "membershipsV3": null,
        |  "onboarded": true,
        |  "expirable":true,
        |  "role":"TDC",
        |  "roles":["TDC"],
        |  "permissions":["ALL_TDC_PERMISSIONS"],
        |  "occurredOn": "2024-01-08 02:40:00.0",
        |  "excludeFromReport": true
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "UserEnabledEvent",
        |  "uuid": "be40f7a9-6fcb-4239-9fee-c9807f93f51b",
        |  "enabled": true,
        |  "school": {"uuid":"9b82537d-2617-4dcd-982b-ee0c23ee97de"},
        |  "avatar": null,
        |  "membershipsV3": null,
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
        |   "onboarded":false,
        |   "enabled":true,
        |   "membershipsV3": [
        |    {
        |        "role": "ADMIN",
        |        "roleId": "b399d07c-7b6e-41e6-b62a-8c4e8e824c00",
        |        "schoolId": "fe587a50-e5a2-45fb-8b4e-aa50874e785e",
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

    val schoolMovementValue =
      """
        |[
        |{
        |   "eventType": "UserMovedBetweenSchoolEvent",
        |   "uuid":  "e7914d3d-bfb3-4293-b4b2-ffe77224cb93",
        |   "role": "PRINCIPAL",
        |   "membershipsV3": [],
        |   "sourceSchoolId": "9753cf6b-a6f3-464e-bb11-f496a28db431",
        |   "targetSchoolId": "a3661a1d-81c6-4277-8278-6cdd5d1ce78b",
        |   "occurredOn": "2024-01-08 02:47:00.0"
        |}
        |]
        |""".stripMargin

    val redshiftRolesValue =
      """
        |[
        |{
        |   "role_name": "TDC",
        |   "role_id": "TDC",
        |   "role_uuid": "e810fcdd-3bda-4005-8938-ab2a36aefedf",
        |   "role_description": null,
        |   "role_predefined": false,
        |   "role_is_ccl": false,
        |   "role_status": 1,
        |   "role_created_time": "2021-02-05 03:57:05.0"
        |},
        |{
        |   "role_name": "PRINCIPAL",
        |   "role_id": "PRINCIPAL",
        |   "role_uuid": "15977e2c-5bc9-4035-948b-6b24cb464491",
        |   "role_description": null,
        |   "role_predefined": false,
        |   "role_is_ccl": false,
        |   "role_status": 1,
        |   "role_created_time": "2021-02-05 04:57:05.0"
        |}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val staffSourceName = getNestedString(StaffUserSchoolRoleAssociationTransformService, StaffSourceName)
    val schoolMovedName = getNestedString(StaffUserSchoolRoleAssociationTransformService, StaffSchoolMovedSourceName)
    val inputDf = spark.read.json(Seq(value).toDS())
    val schoolMovementDf = spark.read.json(Seq(schoolMovementValue).toDS())
    val redshiftRolesDf = spark.read.json(Seq(redshiftRolesValue).toDS())

    when(service.readOptional(staffSourceName, sprk, extraProps = ExtraProps)).thenReturn(Some(inputDf))
    when(service.readOptional(schoolMovedName, sprk, extraProps = ExtraProps)).thenReturn(Some(schoolMovementDf))
    when(service.readFromRedshift[RoleUuid](RedshiftRoleDim)).thenReturn(redshiftRolesDf)
    when(service.getStartIdUpdateStatus(StaffUserSchoolRoleAssociationIdKey)).thenReturn(5001)

    val transform = new StaffUserSchoolRoleAssociationTransform(sprk, service, StaffUserSchoolRoleAssociationTransformService)

    val sink = transform.transform()

    val df = sink.head.output

    assert(df.columns.toSet === expectedColumns.toSet)

    val expectedDF = List(
      ("be40f7a9-6fcb-4239-9fee-c9807f93f51b",  null, null, 1, "9b82537d-2617-4dcd-982b-ee0c23ee97de", "TDC", "e810fcdd-3bda-4005-8938-ab2a36aefedf", Timestamp.valueOf("2024-01-08 02:40:00"), Timestamp.valueOf("2024-07-01 12:24:45.763"), "UserCreatedEvent", 5001),
      ("e7914d3d-bfb3-4293-b4b2-ffe77224cb93", null, null, 1, "a3661a1d-81c6-4277-8278-6cdd5d1ce78b", "PRINCIPAL", "15977e2c-5bc9-4035-948b-6b24cb464491", Timestamp.valueOf("2024-01-08 02:47:00"), Timestamp.valueOf("2024-07-01 12:24:45.763"), "UserMovedBetweenSchoolEvent", 5002),
      ("bfe6e7e1-a78c-4317-b0a0-8d37328e9486", "DEMO", null, 1, "fe587a50-e5a2-45fb-8b4e-aa50874e785e", "ADMIN", "b399d07c-7b6e-41e6-b62a-8c4e8e824c00", Timestamp.valueOf("2024-01-08 02:50:00"), Timestamp.valueOf("2024-07-01 12:24:45.763"), "UserCreatedEvent", 5003)
    ).toDF(expectedColumns: _*)

    assertSmallDatasetEquality(StaffUserSchoolRoleEntity, df, expectedDF)
  }

  test("new version school movement event with list of roles") {
    val schoolMovementValue =
      """
        |[
        |{
        |   "eventType": "UserMovedBetweenSchoolEvent",
        |   "uuid":  "e7914d3d-bfb3-4293-b4b2-ffe77224cb93",
        |   "role": "PRINCIPAL",
        |   "membershipsV3": [
        |     {
        |        "role": "ADMIN",
        |        "roleId": "b399d07c-7b6e-41e6-b62a-8c4e8e824c00",
        |        "schoolId": "8a607bcd-25ad-4ce0-a317-3ee5fee3fc96",
        |        "organization": "DEMO"
        |     },
        |     {
        |        "role": "PRINCIPAL",
        |        "roleId": "15977e2c-5bc9-4035-948b-6b24cb464491",
        |        "schoolId": "aa28f777-1407-4087-b291-f4c28e75a76c",
        |        "organization": "DEMO1"
        |     },
        |     {
        |        "role": "TDC",
        |        "roleId": "e810fcdd-3bda-4005-8938-ab2a36aefedf",
        |        "schoolId": "ef171933-8f3b-4baf-818d-6daecb0d7186",
        |        "organization": "DEMO2"
        |     }
        |   ],
        |   "sourceSchoolId": "9753cf6b-a6f3-464e-bb11-f496a28db431",
        |   "targetSchoolId": "fe587a50-e5a2-45fb-8b4e-aa50874e785e",
        |   "occurredOn": "2024-01-08 02:47:00.0"
        |}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val staffSourceName = getNestedString(StaffUserSchoolRoleAssociationTransformService, StaffSourceName)
    val schoolMovedName = getNestedString(StaffUserSchoolRoleAssociationTransformService, StaffSchoolMovedSourceName)
    val schoolMovementDf = spark.read.json(Seq(schoolMovementValue).toDS())
    when(service.readOptional(staffSourceName, sprk, extraProps = ExtraProps)).thenReturn(None)
    when(service.readOptional(schoolMovedName, sprk, extraProps = ExtraProps)).thenReturn(Some(schoolMovementDf))
    when(service.getStartIdUpdateStatus(StaffUserSchoolRoleAssociationIdKey)).thenReturn(5001)

    val transform = new StaffUserSchoolRoleAssociationTransform(sprk, service, StaffUserSchoolRoleAssociationTransformService)

    val sink = transform.transform()

    val df = sink.head.output

    assert(df.columns.toSet === expectedColumns.toSet)

    val expectedDF = List(
      ("e7914d3d-bfb3-4293-b4b2-ffe77224cb93", "DEMO", null, 1, "8a607bcd-25ad-4ce0-a317-3ee5fee3fc96", "ADMIN", "b399d07c-7b6e-41e6-b62a-8c4e8e824c00", Timestamp.valueOf("2024-01-08 02:47:00"), Timestamp.valueOf("2024-07-01 12:24:45.763"), "UserMovedBetweenSchoolEvent", 5001),
      ("e7914d3d-bfb3-4293-b4b2-ffe77224cb93", "DEMO1", null, 1, "aa28f777-1407-4087-b291-f4c28e75a76c", "PRINCIPAL", "15977e2c-5bc9-4035-948b-6b24cb464491", Timestamp.valueOf("2024-01-08 02:47:00"), Timestamp.valueOf("2024-07-01 12:24:45.763"), "UserMovedBetweenSchoolEvent", 5002),
      ("e7914d3d-bfb3-4293-b4b2-ffe77224cb93", "DEMO2", null, 1, "ef171933-8f3b-4baf-818d-6daecb0d7186", "TDC", "e810fcdd-3bda-4005-8938-ab2a36aefedf", Timestamp.valueOf("2024-01-08 02:47:00"), Timestamp.valueOf("2024-07-01 12:24:45.763"), "UserMovedBetweenSchoolEvent", 5003)
    ).toDF(expectedColumns: _*)

    assertSmallDatasetEquality(StaffUserSchoolRoleEntity, df, expectedDF)
  }

  test("old version school movement event with list of roles") {
    val schoolMovementValue =
      """
        |[
        |{
        |   "eventType": "UserMovedBetweenSchoolEvent",
        |   "uuid":  "e7914d3d-bfb3-4293-b4b2-ffe77224cb93",
        |   "role": "PRINCIPAL",
        |   "membershipsV3": null,
        |   "sourceSchoolId": "9753cf6b-a6f3-464e-bb11-f496a28db431",
        |   "targetSchoolId": "fe587a50-e5a2-45fb-8b4e-aa50874e785e",
        |   "occurredOn": "2024-01-08 02:47:00.0"
        |}
        |]
        |""".stripMargin


    val redshiftRolesValue =
      """
        |[
        |{
        |   "role_name": "PRINCIPAL",
        |   "role_id": "PRINCIPAL",
        |   "role_uuid": "15977e2c-5bc9-4035-948b-6b24cb464491",
        |   "role_description": null,
        |   "role_predefined": false,
        |   "role_is_ccl": false,
        |   "role_status": 1,
        |   "role_created_time": "2021-02-05 04:57:05.0"
        |}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val staffSourceName = getNestedString(StaffUserSchoolRoleAssociationTransformService, StaffSourceName)
    val schoolMovedName = getNestedString(StaffUserSchoolRoleAssociationTransformService, StaffSchoolMovedSourceName)
    val structType = StructType(Seq(
      StructField("role", StringType, true),
      StructField("roleId", StringType, true),
      StructField("schoolId", StringType, true),
      StructField("organization", StringType, true)
    ))
    val schoolMovementDf = spark.read.json(Seq(schoolMovementValue).toDS()).withColumn("membershipsV3", lit(null).cast(ArrayType(structType)))
    val redshiftRolesDf = spark.read.json(Seq(redshiftRolesValue).toDS())
    when(service.readOptional(staffSourceName, sprk, extraProps = ExtraProps)).thenReturn(None)
    when(service.readOptional(schoolMovedName, sprk, extraProps = ExtraProps)).thenReturn(Some(schoolMovementDf))
    when(service.readFromRedshift[RoleUuid](RedshiftRoleDim)).thenReturn(redshiftRolesDf)
    when(service.getStartIdUpdateStatus(StaffUserSchoolRoleAssociationIdKey)).thenReturn(5001)

    val transform = new StaffUserSchoolRoleAssociationTransform(sprk, service, StaffUserSchoolRoleAssociationTransformService)

    val sink = transform.transform()

    val df = sink.head.output

    assert(df.columns.toSet === expectedColumns.toSet)

    val expectedDF = List(
      ("e7914d3d-bfb3-4293-b4b2-ffe77224cb93", null, null, 1, "fe587a50-e5a2-45fb-8b4e-aa50874e785e", "PRINCIPAL", "15977e2c-5bc9-4035-948b-6b24cb464491", Timestamp.valueOf("2024-01-08 02:47:00"), Timestamp.valueOf("2024-07-01 12:24:45.763"), "UserMovedBetweenSchoolEvent", 5001)
    ).toDF(expectedColumns: _*)

    assertSmallDatasetEquality(StaffUserSchoolRoleEntity, df, expectedDF)
  }


}
