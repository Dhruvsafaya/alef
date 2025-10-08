package com.alefeducation.facts.pathway.teacher_activity

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.pathway.teacher_activity.PathwayTeacherActivityTransform.{ PathwayTeacherActivityEntity, PathwayTeacherActivityKey, PathwayTeacherActivityTransformService}
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Resources.{getNestedString, getSink}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class PathwayTeacherActivityTransformSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns = Set(
    "fpta_created_time",
    "fpta_dw_created_time",
    "fpta_date_dw_id",
    "fpta_student_id",
    "fpta_class_id",
    "fpta_pathway_id",
    "fpta_level_id",
    "fpta_tenant_id",
    "fpta_activity_id",
    "fpta_action_name",
    "fpta_action_time",
    "eventdate",
    "fpta_dw_id",
    "fpta_teacher_id",
    "fpta_start_date",
    "fpta_end_date",
    "fpta_activity_type",
    "fpta_activity_progress_status",
    "fpta_is_added_as_resource",
    "fpta_activity_type_value"
  )


  test("should create pathway teacher activity dataframe") {

    val expJson =
      """
        |[
        |{"fpta_action_name":1,"fpta_is_added_as_resource":false,"fpta_activity_type_value":"ACTIVITY","fpta_end_date":"2023-12-21","fpta_pathway_id":"6ddb0a44-4115-430c-8252-bc4a5d65ad88","fpta_activity_id":"075f99be-c18b-457d-8d97-000000028219","fpta_level_id":"be0215be-46ea-4b60-acbb-fdd42e9b2317","fpta_teacher_id":"0404d7bc-7292-4ab1-997e-1f8459a2acd2","fpta_activity_type":1,"fpta_activity_progress_status":"NOT_STARTED","fpta_tenant_id":"93e4949d-7eff-4707-9201-dac917a5e013","fpta_action_time":"2023-12-12T07:43:41.251Z","fpta_start_date":"2023-12-14","fpta_class_id":"2b82055e-3b1a-4e2d-8da1-302b1266d7cf","fpta_student_id":"5417a4da-8062-4677-8a93-2768a4d7a239","eventdate":"2023-12-12","fpta_date_dw_id":"20231212","fpta_created_time":"2023-12-12T07:43:41.320Z","fpta_dw_created_time":"2024-05-20T07:51:04.098Z","fpta_dw_id":42},
        |{"fpta_action_name":1,"fpta_is_added_as_resource":false,"fpta_activity_type_value":"INTERIM_CHECKPOINT","fpta_end_date":"2023-12-21","fpta_pathway_id":"6ddb0a44-4115-430c-8252-bc4a5d65ad88","fpta_activity_id":"615cff1b-7907-41de-aced-5cf0aa854ed4","fpta_level_id":"be0215be-46ea-4b60-acbb-fdd42e9b2317","fpta_teacher_id":"0404d7bc-7292-4ab1-997e-1f8459a2acd2","fpta_activity_type":2,"fpta_activity_progress_status":"COMPLETED","fpta_tenant_id":"93e4949d-7eff-4707-9201-dac917a5e013","fpta_action_time":"2023-12-12T07:43:41.251Z","fpta_start_date":"2023-12-14","fpta_class_id":"2b82055e-3b1a-4e2d-8da1-302b1266d7cf","fpta_student_id":"5417a4da-8062-4677-8a93-2768a4d7a239","eventdate":"2023-12-12","fpta_date_dw_id":"20231212","fpta_created_time":"2023-12-12T07:43:41.320Z","fpta_dw_created_time":"2024-05-20T07:51:04.098Z","fpta_dw_id":43},
        |{"fpta_action_name":2,"fpta_is_added_as_resource":false,"fpta_activity_type_value":null,"fpta_pathway_id":"6ddb0a44-4115-430c-8252-bc4a5d65ad88","fpta_activity_id":"e68fa3fb-5072-452a-b03d-000000028364","fpta_level_id":"7fd16327-1856-4e7c-9822-8e09eee8e93c","fpta_teacher_id":"0404d7bc-7292-4ab1-997e-1f8459a2acd2","fpta_activity_type":-1,"fpta_activity_progress_status":null,"fpta_tenant_id":"93e4949d-7eff-4707-9201-dac917a5e013","fpta_action_time":"2023-12-12T07:45:37.922Z","fpta_class_id":"2b82055e-3b1a-4e2d-8da1-302b1266d7cf","fpta_student_id":"5417a4da-8062-4677-8a93-2768a4d7a239","eventdate":"2023-12-12","fpta_date_dw_id":"20231212","fpta_created_time":"2023-12-12T07:45:37.922Z","fpta_dw_created_time":"2024-05-20T07:51:04.208Z","fpta_dw_id":44,"fpta_start_date":null,"fpta_end_date":null},
        |{"fpta_action_name":1,"fpta_is_added_as_resource":true,"fpta_activity_type_value":"ACTIVITY","fpta_pathway_id":"dc634dc2-cca6-4947-b91b-b8e7ee4a0388","fpta_activity_id":"3235ffc3-ca0f-4907-963e-000000104084","fpta_level_id":null,"fpta_teacher_id":"51dd752f-c008-459f-a097-bac731a877f5","fpta_activity_type":1,"fpta_activity_progress_status":"NOT_STARTED","fpta_tenant_id":"93e4949d-7eff-4707-9201-dac917a5e013","fpta_action_time":"2024-09-23T11:40:56.877871251","fpta_class_id":null,"fpta_student_id":"85a523be-080c-40ae-97a4-3134b0ae532b","eventdate":"2024-09-23","fpta_date_dw_id":"20240923","fpta_created_time":"2024-09-23T11:40:56.965Z","fpta_dw_created_time":"2024-10-07T11:14:26.496Z","fpta_dw_id":45,"fpta_start_date":"2024-09-23","fpta_end_date":null},
        |{"fpta_action_name":1,"fpta_is_added_as_resource":true,"fpta_activity_type_value":"PDF","fpta_pathway_id":"dc634dc2-cca6-4947-b91b-b8e7ee4a0388","fpta_activity_id":"dab868a3-001d-467e-8347-000000104073","fpta_level_id":null,"fpta_teacher_id":"51dd752f-c008-459f-a097-bac731a877f5","fpta_activity_type":-1,"fpta_activity_progress_status":"NOT_STARTED","fpta_tenant_id":"93e4949d-7eff-4707-9201-dac917a5e013","fpta_action_time":"2024-09-23T11:40:56.877871251","fpta_class_id":null,"fpta_student_id":"85a523be-080c-40ae-97a4-3134b0ae532b","eventdate":"2024-09-23","fpta_date_dw_id":"20240923","fpta_created_time":"2024-09-23T11:40:56.965Z","fpta_dw_created_time":"2024-10-07T11:14:26.496Z","fpta_dw_id":46,"fpta_start_date":"2024-09-23","fpta_end_date":null},
        |{"fpta_action_name":2,"fpta_is_added_as_resource":true,"fpta_activity_type_value":null,"fpta_pathway_id":"369fb49d-5d21-481e-b034-28249294a355","fpta_activity_id":"22385e11-7d61-4cd4-8b72-000000103879","fpta_level_id":null,"fpta_teacher_id":"931ffd3e-c414-46b1-8687-9ecf2829bbf0","fpta_activity_type":-1,"fpta_activity_progress_status":null,"fpta_tenant_id":"93e4949d-7eff-4707-9201-dac917a5e013","fpta_action_time":"2024-10-07 12:26:14.672618","fpta_class_id":null,"fpta_student_id":"4925670f-4063-4761-b095-865b9928e2c4","eventdate":"2024-10-07","fpta_date_dw_id":"20241007","fpta_created_time":"2024-10-07T12:26:14.672Z","fpta_dw_created_time":"2024-10-07T11:14:26.496Z","fpta_dw_id":47,"fpta_start_date":null,"fpta_end_date":null}
        |]
        |""".stripMargin
    val expDf = createDfFromJsonWithTimeCols(spark, expJson)

    val assignedEvent =
      """
        |[
        |{
        | "eventType": "PathwayActivitiesAssignedEvent",
        | "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |	"studentId": "5417a4da-8062-4677-8a93-2768a4d7a239",
        |	"classId": "2b82055e-3b1a-4e2d-8da1-302b1266d7cf",
        |	"pathwayId": "6ddb0a44-4115-430c-8252-bc4a5d65ad88",
        |	"levelId": "be0215be-46ea-4b60-acbb-fdd42e9b2317",
        |	"activityIds": [
        |		"075f99be-c18b-457d-8d97-000000028219",
        |		"615cff1b-7907-41de-aced-5cf0aa854ed4"
        |	],
        |	"dueDate": {
        |		"startDate": "2023-12-14",
        |		"endDate": "2023-12-21"
        |	},
        |   "activitiesProgressStatus": [
        |		{
        |			"activityId": "075f99be-c18b-457d-8d97-000000028219",
        |			"progressStatus": "NOT_STARTED",
        |			"activityType": "ACTIVITY"
        |		},
        |		{
        |			"activityId": "615cff1b-7907-41de-aced-5cf0aa854ed4",
        |			"progressStatus": "COMPLETED",
        |			"activityType": "INTERIM_CHECKPOINT"
        |		}
        |	],
        |	"assignedBy": "0404d7bc-7292-4ab1-997e-1f8459a2acd2",
        |	"assignedOn": "2023-12-12T07:43:41.251",
        |	"occurredOn": "2023-12-12T07:43:41.320"
        |}
        |]
        """.stripMargin

    val unassignedEvent =
      """
        |[
        |{
        | "eventType": "PathwaysActivityUnAssignedEvent",
        | "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |	"studentId": "5417a4da-8062-4677-8a93-2768a4d7a239",
        |	"pathwayId": "6ddb0a44-4115-430c-8252-bc4a5d65ad88",
        |	"classId": "2b82055e-3b1a-4e2d-8da1-302b1266d7cf",
        |	"levelId": "7fd16327-1856-4e7c-9822-8e09eee8e93c",
        |	"activityId": "e68fa3fb-5072-452a-b03d-000000028364",
        |	"unAssignedBy": "0404d7bc-7292-4ab1-997e-1f8459a2acd2",
        |	"unAssignedOn": "2023-12-12T07:45:37.922",
        |	"occurredOn": "2023-12-12T07:45:37.922"
        |}
        |]
        |""".stripMargin

    val additionalResourceAssignedEvent = """
                                            |[
                                            |{
                                            | "eventType": "AdditionalResourceAssignedEvent",
                                            | "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
                                            |  "id": "37b93a60-6f60-41c1-9c5c-35ed91356e03",
                                            |	 "learnerId": "85a523be-080c-40ae-97a4-3134b0ae532b",
                                            |	 "courseId": "dc634dc2-cca6-4947-b91b-b8e7ee4a0388",
                                            |	 "courseType": "PATHWAY",
                                            |	 "academicYear": "2023-2024",
                                            |	 "activities": [
                                            |		"3235ffc3-ca0f-4907-963e-000000104084",
                                            |		"dab868a3-001d-467e-8347-000000104073"
                                            |	 ],
                                            |	"dueDate": {
                                            |		"startDate": "2024-09-23",
                                            |		"endDate": null
                                            |	},
                                            |	"resourceInfo": [
                                            |		{
                                            |			"activityId": "3235ffc3-ca0f-4907-963e-000000104084",
                                            |     "progressStatus": "NOT_STARTED",
                                            |			"activityType": "ACTIVITY"
                                            |		},
                                            |		{
                                            |			"activityId": "dab868a3-001d-467e-8347-000000104073",
                                            |     "progressStatus": "NOT_STARTED",
                                            |			"activityType": "PDF"
                                            |		}
                                            |	],
                                            |	"assignedBy": "51dd752f-c008-459f-a097-bac731a877f5",
                                            |	"pathwayProgressStatus": "DIAGNOSTIC_TEST_LOCKED",
                                            |	"assignedOn": "2024-09-23T11:40:56.877871251",
                                            |	"uuid": "dc254c51-b594-4f8d-9a2e-2a78af119920",
                                            |	"occurredOn": "2024-09-23T11:40:56.965"
                                            |}
                                            |]
        """.stripMargin

    val additionalResourceUnAssignedEvent = """
                                           |[
                                           |{
                                           | "eventType": "AdditionalResourceUnAssignedEvent",
                                           | "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
                                           | "id": "efe85b19-9faf-4004-8bbe-0072843dfc9a",
                                           | "learnerId": "4925670f-4063-4761-b095-865b9928e2c4",
                                           | "courseId": "369fb49d-5d21-481e-b034-28249294a355",
                                           | "courseType": "PATHWAY",
                                           | "academicYear": "2024-2025",
                                           | "activityId": "22385e11-7d61-4cd4-8b72-000000103879",
                                           | "unAssignedBy": "931ffd3e-c414-46b1-8687-9ecf2829bbf0",
                                           | "unAssignedOn": "2024-10-07T12:26:14.67261854",
                                           | "uuid": "d1648c73-b426-41b8-862e-8858221e4eac",
                                           | "occurredOn": "2024-10-07T12:26:14.672"
                                           |}
                                           |]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new PathwayTeacherActivityTransform(sprk, service)
    val assignedInputDF = spark.read.json(Seq(assignedEvent).toDS())
    val unassignedInputDf = spark.read.json(Seq(unassignedEvent).toDS())
    val additionalResourceAssignedDf = spark.read.json(Seq(additionalResourceAssignedEvent).toDS())
    val additionalResourceUnAssignedDf = spark.read.json(Seq(additionalResourceUnAssignedEvent).toDS())

    val assignedSourceName = getNestedString(PathwayTeacherActivityTransformService, "assigned-source")
    val unassignedSourceName = getNestedString(PathwayTeacherActivityTransformService, "unassigned-source")
    val additionalResourceAssignedSourceName = getNestedString(PathwayTeacherActivityTransformService, "additional-resource-assigned-source")
    val additionalResourceUnAssignedSourceName = getNestedString(PathwayTeacherActivityTransformService, "additional-resource-unassigned-source")

    when(service.readOptional(assignedSourceName, sprk)).thenReturn(Some(assignedInputDF))
    when(service.readOptional(unassignedSourceName, sprk)).thenReturn(Some(unassignedInputDf))
    when(service.readOptional(additionalResourceAssignedSourceName, sprk)).thenReturn(Some(additionalResourceAssignedDf))
    when(service.readOptional(additionalResourceUnAssignedSourceName, sprk)).thenReturn(Some(additionalResourceUnAssignedDf))
    when(service.getStartIdUpdateStatus(PathwayTeacherActivityKey)).thenReturn(42)

    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assertSmallDatasetEquality(PathwayTeacherActivityEntity, df, expDf)
  }

  private def createDfFromJsonWithTimeCols(spark: SparkSession, json: String): DataFrame = {
    createDfFromJson(spark, json)
      .withColumn("fpta_created_time", col("fpta_created_time").cast(TimestampType))
      .withColumn("fpta_action_time", col("fpta_action_time").cast(TimestampType))
      .withColumn("fpta_start_date", col("fpta_start_date").cast(DateType))
      .withColumn("fpta_end_date", col("fpta_end_date").cast(DateType))
  }
}
