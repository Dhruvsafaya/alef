package com.alefeducation.dimensions.role

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.role.RoleTransform.{RoleEntity, RoleTransformService}
import com.alefeducation.util.DataFrameEqualityUtils.assertSmallDatasetEquality
import com.alefeducation.util.Resources.getSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Timestamp

class RoleTransformSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns: List[String] = List(
    "role_uuid",
    "role_name",
    "role_status",
    "role_created_time",
    "role_updated_time",
    "role_dw_created_time",
    "role_dw_updated_time",
    "role_category_id",
    "role_organization_name",
    "role_organization_code",
    "role_type",
    "role_is_ccl",
    "role_description",
    "role_predefined"
  )

  test("should prepare dataframe for role") {
    val value =
      """
        |[
        |   {
        |      "tenantId":"tenant-id",
        |      "eventType":"RoleCreatedEvent",
        |      "role":{
        |         "id":"fdd07b44-a094-4fb6-aadb-cbcfc8df09f7",
        |         "name":"CreateEvents",
        |         "description":"CreateEvents",
        |         "permissions":[
        |
        |         ],
        |         "predefined":false,
        |         "granularPermissions":[
        |            "read:curriculum_editor",
        |            "read:aat.workspace",
        |            "accessCclModule:aat.workspace",
        |            "create:aat.workspace",
        |            "update:aat.workspace",
        |            "assign:aat.workspace",
        |            "delete:aat.workspace",
        |            "readAll:aat.workspace",
        |            "read:aat.pool",
        |            "read:aat.question",
        |            "accessCclModule:aat.question",
        |            "read:curriculum",
        |            "create:aat.question",
        |            "update:aat.question",
        |            "delete:aat.question",
        |            "publish:aat.question",
        |            "accessCclModule:aat.pool",
        |            "create:aat.pool",
        |            "update:aat.pool",
        |            "delete:aat.pool"
        |         ],
        |         "created":"2024-07-10T09:01:56.683555",
        |         "updated":"2024-07-10T09:01:56.683555",
        |         "organization":{
        |            "name":"8 Test 2 a2103251",
        |            "code":"a2103251"
        |         },
        |         "roleType":"CUSTOM",
        |         "categoryId":"100f9d55-9248-485d-b679-11a04253b629",
        |         "permissionToCategories":[
        |
        |         ],
        |         "isCCLRole":true
        |      },
        |      "occurredOn":"2024-07-10 09:01:56",
        |      "id":"fdd07b44-a094-4fb6-aadb-cbcfc8df09f7",
        |      "name":"CreateEvents",
        |      "permissions":[
        |         "read:curriculum_editor",
        |         "read:aat.workspace",
        |         "accessCclModule:aat.workspace",
        |         "create:aat.workspace",
        |         "update:aat.workspace",
        |         "assign:aat.workspace",
        |         "delete:aat.workspace",
        |         "readAll:aat.workspace",
        |         "read:aat.pool",
        |         "read:aat.question",
        |         "accessCclModule:aat.question",
        |         "read:curriculum",
        |         "create:aat.question",
        |         "update:aat.question",
        |         "delete:aat.question",
        |         "publish:aat.question",
        |         "accessCclModule:aat.pool",
        |         "create:aat.pool",
        |         "update:aat.pool",
        |         "delete:aat.pool"
        |      ]
        |   },
        |   {
        |      "role":{
        |         "id":"fdd07b44-a094-4fb6-aadb-cbcfc8df09f7",
        |         "name":"UpdateEvents",
        |         "description":"UpdateEvents2",
        |         "permissions":[
        |
        |         ],
        |         "predefined":false,
        |         "granularPermissions":[
        |            "read:curriculum_editor",
        |            "read:aat.workspace",
        |            "accessCclModule:aat.workspace",
        |            "create:aat.workspace",
        |            "update:aat.workspace",
        |            "assign:aat.workspace",
        |            "delete:aat.workspace",
        |            "readAll:aat.workspace",
        |            "read:aat.pool",
        |            "accessCclModule:aat.pool",
        |            "read:aat.question",
        |            "accessCclModule:aat.question",
        |            "read:curriculum",
        |            "create:aat.question",
        |            "update:aat.question",
        |            "delete:aat.question",
        |            "publish:aat.question",
        |            "create:aat.pool",
        |            "update:aat.pool",
        |            "delete:aat.pool"
        |         ],
        |         "created":"2024-07-10T09:01:56.683",
        |         "updated":"2024-07-10T09:02:14.933783",
        |         "organization":{
        |            "name":"8 Test 2 a2103251",
        |            "code":"a2103251"
        |         },
        |         "roleType":"CUSTOM",
        |         "categoryId":"100f9d55-9248-485d-b679-11a04253b629",
        |         "permissionToCategories":[
        |
        |         ],
        |         "isCCLRole":true
        |      },
        |      "occurredOn":"2024-07-10 09:02:56",
        |      "id":"fdd07b44-a094-4fb6-aadb-cbcfc8df09f7",
        |      "name":"UpdatedEvents",
        |      "permissions":[
        |         "read:curriculum_editor",
        |         "read:aat.workspace",
        |         "accessCclModule:aat.workspace",
        |         "create:aat.workspace",
        |         "update:aat.workspace",
        |         "assign:aat.workspace",
        |         "delete:aat.workspace",
        |         "readAll:aat.workspace",
        |         "read:aat.pool",
        |         "accessCclModule:aat.pool",
        |         "read:aat.question",
        |         "accessCclModule:aat.question",
        |         "read:curriculum",
        |         "create:aat.question",
        |         "update:aat.question",
        |         "delete:aat.question",
        |         "publish:aat.question",
        |         "create:aat.pool",
        |         "update:aat.pool",
        |         "delete:aat.pool"
        |      ]
        |   },
        |   {
        |      "role":{
        |         "id":"034b73d6-ee10-4e93-9d3e-ea5c1dad2503",
        |         "name":"DeletedEvents",
        |         "description":"DeletedEvents",
        |         "permissions":[
        |
        |         ],
        |         "predefined":false,
        |         "granularPermissions":[
        |            "read:curriculum_editor",
        |            "read:aat.workspace",
        |            "accessCclModule:aat.workspace",
        |            "create:aat.workspace",
        |            "update:aat.workspace",
        |            "assign:aat.workspace",
        |            "delete:aat.workspace",
        |            "readAll:aat.workspace",
        |            "read:aat.pool",
        |            "read:aat.question",
        |            "accessCclModule:aat.question",
        |            "read:curriculum",
        |            "create:aat.question",
        |            "update:aat.question",
        |            "delete:aat.question",
        |            "publish:aat.question",
        |            "accessCclModule:aat.pool",
        |            "create:aat.pool",
        |            "update:aat.pool",
        |            "delete:aat.pool"
        |         ],
        |         "created":"2024-07-10T09:02:53.881",
        |         "updated":"2024-07-10T09:02:53.881",
        |         "organization":{
        |            "name":"8 Test 2 a2103251",
        |            "code":"a2103251"
        |         },
        |         "roleType":"CUSTOM",
        |         "categoryId":"100f9d55-9248-485d-b679-11a04253b629",
        |         "permissionToCategories":[
        |
        |         ],
        |         "isCCLRole":true
        |      },
        |      "occurredOn":"2024-07-10 09:03:56",
        |      "id":"034b73d6-ee10-4e93-9d3e-ea5c1dad2503"
        |   }
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val sourceName = getSource(RoleTransformService).head
    val inputDf = spark.read.json(Seq(value).toDS())

    when(service.readOptional(sourceName, sprk)).thenReturn(Some(inputDf))

    val transform = new RoleTransform(sprk, service, RoleTransformService)

    val sink = transform.transform()

    val df = sink.head.output

    assert(df.columns.toSet === expectedColumns.toSet)

    val expectedDF = List(
      ("034b73d6-ee10-4e93-9d3e-ea5c1dad2503", "DeletedEvents", 1, Timestamp.valueOf("2024-07-10 09:03:56"), null, Timestamp.valueOf("2024-07-11 08:07:30"), null, "100f9d55-9248-485d-b679-11a04253b629", "8 Test 2 a2103251", "a2103251", "CUSTOM", true, "DeletedEvents", false),
      ("fdd07b44-a094-4fb6-aadb-cbcfc8df09f7", "UpdateEvents",  1, Timestamp.valueOf("2024-07-10 09:02:56"), null, Timestamp.valueOf("2024-07-11 08:07:30"), null, "100f9d55-9248-485d-b679-11a04253b629", "8 Test 2 a2103251", "a2103251", "CUSTOM", true, "UpdateEvents2", false)
    ).toDF(expectedColumns: _*)

    assertSmallDatasetEquality(RoleEntity, df, expectedDF)
  }

}
