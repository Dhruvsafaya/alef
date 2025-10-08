package com.alefeducation.dimensions

import com.alefeducation.bigdata.commons.testutils.ExpectedFields.assertUserSink
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.admin.AdminUsersDimensionTransformer
import com.alefeducation.models.AdminUserModel.{AdminDim, RoleDim}
import com.alefeducation.models.StudentModel.SchoolDim
import com.alefeducation.models.UserModel.User
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.{AdminUserType, UserCreated, UserDeletedEvent, UserUpdateEnableDisable}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.Encoders

class AdminUsersDimensionSpec extends SparkSuite with BaseDimensionSpec {

  trait Setup {
    implicit val transformer = new AdminUsersDimensionTransformer(AdminUsersDimensionName, spark)
    transformer.secretManager.init("admin_user")
  }

  val userDimExpectedColumns = dimDateCols("admin").toSet ++ Set(
    "admin_uuid",
    "admin_onboarded",
    "admin_avatar",
    "school_uuid",
    "admin_expirable",
    "role_uuid",
    "admin_exclude_from_report"
  )

  test("user created event") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserCreatedEvent",
              |  "uuid": "tdc-id1",
              |  "enabled": true,
              |  "school": {"uuid":"school-id"},
              |  "avatar": null,
              |  "onboarded": true,
              |  "expirable":true,
              |  "role":"TDC",
              |  "roles":["TDC"],
              |  "permissions":["ALL_TDC_PERMISSIONS"],
              |  "membershipsV3": [{
              |     "roleId": "role-uuid",
              |     "role": "TDC",
              |     "organization": "shared",
              |     "schoolId": "school-id"
              |  }],
              |  "occurredOn": "2019-01-08 02:40:00.0",
              |  "excludeFromReport": true
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":3,
                    |"admin_id":"tdc-id2",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true,
                    |"admin_exclude_from_report": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 4)

          val s3Sink = sinks.filter(_.name == ParquetUserSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-01-08")

          val userSink = sinks.filter(_.name == RedshiftUserSink)
          assertUserSink(userSink, List(("tdc-id1", "2019-01-08 02:40:00", AdminUserType)))

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)

          val adminUserCreate = adminUserCreateUpdateSink.head.output
          assert(adminUserCreate.count == 1)
          assert(adminUserCreate.columns.toSet === userDimExpectedColumns)
          assert[String](adminUserCreate, "admin_uuid", "tdc-id1")
          assert[String](adminUserCreate, "admin_created_time", "2019-01-08 02:40:00.0")
          assert[String](adminUserCreate, "school_uuid", "school-id")
          assert[Int](adminUserCreate, "admin_status", ActiveEnabled)
          assert[Boolean](adminUserCreate, "admin_exclude_from_report", true)
        }
      )
    }
  }

  test("user updated event for a disabled user in DB") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserUpdatedEvent",
              |  "uuid": "tdc-id1",
              |  "enabled": true,
              |  "school": {"uuid":"school-id"},
              |  "avatar": null,
              |  "onboarded": true,
              |  "expirable":true,
              |  "role":"TDC",
              |  "roles":["TDC"],
              |  "permissions":["ALL_TDC_PERMISSIONS"],
              |  "membershipsV3": [{
              |     "roleId": "role-uuid",
              |     "role": "TDC",
              |     "organization": "shared",
              |     "schoolId": "school-id"
              |  }],
              |  "occurredOn": "2019-01-08 02:40:00.0",
              |  "excludeFromReport": false
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":3,
                    |"admin_id":"tdc-id1",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true,
                    |"admin_exclude_from_report": false
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 3)

          val s3Sink = sinks.filter(_.name == ParquetUserSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-01-08")

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminUpdatedDf = adminUserCreateUpdateSink.head.output.cache()
          assert(adminUpdatedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminUpdatedDf, "admin_uuid", "tdc-id1")
          assert[String](adminUpdatedDf, "admin_created_time", "2019-01-08 02:40:00.0")
          assert[String](adminUpdatedDf, "school_uuid", "school-id")
          assert[Int](adminUpdatedDf, "admin_status", Disabled)
          assert[Boolean](adminUpdatedDf, "admin_exclude_from_report", false)

        }
      )
    }
  }

  test("user enable/disable events only") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value = """
              |[{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserDisabledEvent",
              |  "uuid": "tdc-id1",
              |  "enabled": true,
              |  "school": {"uuid":"school-id1"},
              |  "avatar": null,
              |  "onboarded": true,
              |  "expirable":true,
              |  "role":"TDC",
              |  "roles":["TDC"],
              |  "permissions":["ALL_TDC_PERMISSIONS"],
              |  "membershipsV3": [{
              |     "roleId": "role-uuid",
              |     "role": "TDC",
              |     "organization": "shared",
              |     "schoolId": "school-id"
              |  }],
              |  "occurredOn": "2019-01-08 02:40:00.0",
              |  "excludeFromReport": false
              |},
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserEnabledEvent",
              |  "uuid": "tdc-id1",
              |  "enabled": true,
              |  "school": {"uuid":"school-id1"},
              |  "avatar": null,
              |  "onboarded": true,
              |  "expirable":true,
              |  "role":"TDC",
              |  "roles":["TDC"],
              |  "permissions":["ALL_TDC_PERMISSIONS"],
              |  "membershipsV3": [{
              |     "roleId": "role-uuid",
              |     "role": "TDC",
              |     "organization": "shared",
              |     "schoolId": "school-id"
              |  }],
              |  "occurredOn": "2019-01-09 02:40:00.0",
              |  "excludeFromReport": false
              |}]
      """.stripMargin
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id1",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":3,
                    |"admin_id":"tdc-id1",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true,
                    |"admin_exclude_from_report": false
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 3)

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminStatusDf = adminUserCreateUpdateSink.head.output.cache()
          assert(adminStatusDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminStatusDf, "admin_uuid", "tdc-id1")
          assert[String](adminStatusDf, "admin_created_time", "2019-01-09 02:40:00.0")
          assert[String](adminStatusDf, "school_uuid", "school-id1")
          assert[Int](adminStatusDf, "admin_status", ActiveEnabled)
          assert[Boolean](adminStatusDf, "admin_exclude_from_report", false)
        }
      )
    }
  }

  test("user enable/disable event when first event is an update") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value = """
              |[{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserUpdatedEvent",
              |  "uuid": "principal-id1",
              |  "enabled": true,
              |  "school": {"uuid":"school-id1"},
              |  "avatar": null,
              |  "onboarded": true,
              |  "expirable":true,
              |  "role":"PRINCIPAL",
              |  "roles":["PRINCIPAL"],
              |  "permissions":["ALL_PRINCIPAL_PERMISSIONS"],
              |  "occurredOn": "2019-01-08 02:40:00.0"
              |},
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserDisabledEvent",
              |  "uuid": "principal-id1",
              |  "enabled": true,
              |  "school": {"uuid":"school-id1"},
              |  "avatar": null,
              |  "onboarded": true,
              |  "expirable":true,
              |  "role":"PRINCIPAL",
              |  "roles":["PRINCIPAL"],
              |  "permissions":["ALL_PRINCIPAL_PERMISSIONS"],
              |  "occurredOn": "2019-01-09 02:40:00.0"
              |}
              |]
      """.stripMargin
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"principal-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id1",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":5,
                    |   "role_id":"PRINCIPAL",
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":1,
                    |"admin_id":"principal-id1",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 5,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 3)

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminStatusUpdateDf = adminUserCreateUpdateSink.head.output.cache()
          assert(adminStatusUpdateDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminStatusUpdateDf, "admin_uuid", "principal-id1")
          assert[Boolean](adminStatusUpdateDf, "admin_onboarded", true)
          assert[String](adminStatusUpdateDf, "school_uuid", "school-id1")
          assert[String](adminStatusUpdateDf, "role_uuid", "PRINCIPAL")
          assert[Int](adminStatusUpdateDf, "admin_status", Disabled)
        }
      )
    }
  }

  //TODO School MOVEMENT tests
  test("user moved school for a disabled user in DB") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetAdminSchoolMovedSource,
          value = """
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserMovedBetweenSchoolEvent",
                    |  "uuid": "tdc-id1",
                    |  "role":"TDC",
                    |  "sourceSchoolId": "school-id",
                    |  "targetSchoolId":"HebbySchool",
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "HebbySchool"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |}
                  """.stripMargin
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":3,
                    |"admin_id":"tdc-id1",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 3)

          val s3Sink = sinks.filter(_.name == ParquetAdminSchoolMovedSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-01-08")

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminUpdatedDf = adminUserCreateUpdateSink.head.output.cache()
          assert(adminUpdatedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminUpdatedDf, "admin_uuid", "tdc-id1")
          assert[String](adminUpdatedDf, "school_uuid", "HebbySchool")
          assert[Int](adminUpdatedDf, "admin_status", Disabled)
          assert[Boolean](adminUpdatedDf, "admin_expirable", true)
          assert[Boolean](adminUpdatedDf, "admin_onboarded", false)
          assert[String](adminUpdatedDf, "admin_avatar", null)

        }
      )
    }
  }

  test("user moved school with update event later than moved event") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetAdminSchoolMovedSource,
          value = """
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserMovedBetweenSchoolEvent",
                    |  "uuid": "tdc-id1",
                    |  "role":"TDC",
                    |  "sourceSchoolId": "school-id",
                    |  "targetSchoolId":"HebbySchool",
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "HebbySchool"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |}
                  """.stripMargin
        ),
        SparkFixture(
          key = ParquetUserSource,
          value = """
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserUpdatedEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"HebbySchool"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"PRINCIPAL",
                    |  "roles":["PRINCIPAL"],
                    |  "permissions":["ALL_PRINCIPAL_PERMISSIONS"],
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |}
                  """.stripMargin
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "PRINCIPAL"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":1,
                    |"admin_id":"tdc-id1",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 4)

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminUpdatedDf = adminUserCreateUpdateSink.head.output.cache()
          assert(adminUpdatedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminUpdatedDf, "admin_uuid", "tdc-id1")
          assert[String](adminUpdatedDf, "school_uuid", "HebbySchool")
          assert[String](adminUpdatedDf, "role_uuid", "PRINCIPAL")
          assert[Int](adminUpdatedDf, "admin_status", ActiveEnabled)
          assert[Boolean](adminUpdatedDf, "admin_expirable", true)
          assert[Boolean](adminUpdatedDf, "admin_onboarded", true)
          assert[String](adminUpdatedDf, "admin_avatar", null)

        }
      )
    }
  }

  test("user moved school with update event earlier than moved event") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value = """
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserUpdatedEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-1"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"TDC",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |}
                  """.stripMargin
        ),
        SparkFixture(
          key = ParquetAdminSchoolMovedSource,
          value = """
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserMovedBetweenSchoolEvent",
                    |  "uuid": "tdc-id1",
                    |  "role":"TDC",
                    |  "sourceSchoolId": "school-id",
                    |  "targetSchoolId":"HebbySchool",
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "HebbySchool"
                    |  }],
                    |  "occurredOn": "2019-01-09 02:40:00.0"
                    |}
                  """.stripMargin
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":1,
                    |"admin_id":"tdc-id1",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 4)

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminUpdatedDf = adminUserCreateUpdateSink.head.output.cache()
          assert(adminUpdatedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminUpdatedDf, "admin_uuid", "tdc-id1")
          assert[String](adminUpdatedDf, "school_uuid", "HebbySchool")
          assert[String](adminUpdatedDf, "role_uuid", "TDC")
          assert[Int](adminUpdatedDf, "admin_status", ActiveEnabled)
          assert[Boolean](adminUpdatedDf, "admin_expirable", true)
          assert[Boolean](adminUpdatedDf, "admin_onboarded", true)
          assert[String](adminUpdatedDf, "admin_avatar", null)

        }
      )
    }
  }

  test("user moved school with update /enable and disable event") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetAdminSchoolMovedSource,
          value = """
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserMovedBetweenSchoolEvent",
                    |  "uuid": "tdc-id1",
                    |  "role":"TDC",
                    |  "sourceSchoolId": "school-id",
                    |  "targetSchoolId":"HebbySchool",
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "HebbySchool"
                    |  }],
                    |  "occurredOn": "2019-01-09 02:40:00.0"
                    |}
                  """.stripMargin
        ),
        SparkFixture(
          key = ParquetUserSource,
          value = """
                    |[{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserUpdatedEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-1"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"PRINCIPAL",
                    |  "roles":["PRINCIPAL"],
                    |  "permissions":["ALL_PRINCIPAL_PERMISSIONS"],
                    |  "occurredOn": "2019-01-06 02:40:00.0"
                    |},
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserDisabledEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-id1"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"PRINCIPAL",
                    |  "roles":["PRINCIPAL"],
                    |  "permissions":["ALL_PRINCIPAL_PERMISSIONS"],
                    |  "occurredOn": "2019-01-06 02:41:00.0"
                    |}
                    |]
                  """.stripMargin
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":1,
                    |"admin_id":"tdc-id1",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 4)

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminUpdatedDf = adminUserCreateUpdateSink.head.output.cache()
          assert(adminUpdatedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminUpdatedDf, "admin_uuid", "tdc-id1")
          assert[String](adminUpdatedDf, "school_uuid", "HebbySchool")
          assert[String](adminUpdatedDf, "role_uuid", "PRINCIPAL")
          assert[Int](adminUpdatedDf, "admin_status", Disabled)
          assert[Boolean](adminUpdatedDf, "admin_expirable", true)
          assert[Boolean](adminUpdatedDf, "admin_onboarded", true)
          assert[String](adminUpdatedDf, "admin_avatar", null)

        }
      )
    }
  }

  test("user moved school between with update and disable event") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetAdminSchoolMovedSource,
          value = """
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserMovedBetweenSchoolEvent",
                    |  "uuid": "tdc-id1",
                    |  "role":"TDC",
                    |  "sourceSchoolId": "school-id",
                    |  "targetSchoolId":"HebbySchool",
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "HebbySchool"
                    |  }],
                    |  "occurredOn": "2019-01-07 02:40:00.0"
                    |}
                  """.stripMargin
        ),
        SparkFixture(
          key = ParquetUserSource,
          value = """
                    |[{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserUpdatedEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-1"},
                    |  "avatar": "avatar_link",
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"PRINCIPAL",
                    |  "roles":["PRINCIPAL"],
                    |  "permissions":["ALL_PRINCIPAL_PERMISSIONS"],
                    |  "occurredOn": "2019-01-06 02:40:00.0"
                    |},
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserDisabledEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"HebbySchool"},
                    |  "avatar": "avatar_link",
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"TDC",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:41:00.0"
                    |}
                    |]
                  """.stripMargin
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":1,
                    |"admin_id":"tdc-id1",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 4)

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminUpdatedDf = adminUserCreateUpdateSink.head.output.cache()
          assert(adminUpdatedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminUpdatedDf, "admin_uuid", "tdc-id1")
          assert[String](adminUpdatedDf, "school_uuid", "HebbySchool")
          assert[String](adminUpdatedDf, "role_uuid", "TDC")
          assert[Int](adminUpdatedDf, "admin_status", Disabled)
          assert[Boolean](adminUpdatedDf, "admin_expirable", true)
          assert[Boolean](adminUpdatedDf, "admin_onboarded", true)
          assert[String](adminUpdatedDf, "admin_avatar", "avatar_link")

        }
      )
    }
  }

  test("user moved school later with update /enable and disable event") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetAdminSchoolMovedSource,
          value = """
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserMovedBetweenSchoolEvent",
                    |  "uuid": "tdc-id1",
                    |  "role":"TDC",
                    |  "sourceSchoolId": "school-id",
                    |  "targetSchoolId":"HebbySchool",
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "HebbySchool"
                    |  }],
                    |  "occurredOn": "2019-01-07 02:40:00.0"
                    |}
                  """.stripMargin
        ),
        SparkFixture(
          key = ParquetUserSource,
          value = """
                    |[{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserUpdatedEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"HebbySchool"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"TDC",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |},
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserDisabledEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"HebbySchool"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"TDC",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:41:00.0"
                    |}
                    |]
                  """.stripMargin
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":1,
                    |"admin_id":"tdc-id1",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 4)

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminUpdatedDf = adminUserCreateUpdateSink.head.output.cache()
          assert(adminUpdatedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminUpdatedDf, "admin_uuid", "tdc-id1")
          assert[String](adminUpdatedDf, "school_uuid", "HebbySchool")
          assert[String](adminUpdatedDf, "role_uuid", "TDC")
          assert[Int](adminUpdatedDf, "admin_status", Disabled)
          assert[Boolean](adminUpdatedDf, "admin_expirable", true)
          assert[Boolean](adminUpdatedDf, "admin_onboarded", true)
          assert[String](adminUpdatedDf, "admin_avatar", null)

        }
      )
    }
  }

  test("user created and updated in same batch") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value = """
                    |[{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserCreatedEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-id"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"TDC",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |},
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserUpdatedEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-id"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"PRINCIPAL",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:42:00.0"
                    |}]
                  """.stripMargin
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |},{
                    |   "role_dw_id":5,
                    |   "role_name": "PRINCIPAL"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":3,
                    |"admin_id":"tdc-id2",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 6)

          val s3Sink = sinks.filter(_.name == ParquetUserSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-01-08")

          val userSink = sinks.filter(_.name == RedshiftUserSink)
          assertUserSink(userSink, List(("tdc-id1", "2019-01-08 02:40:00", AdminUserType)))

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 2)

          val adminUserCreate = adminUserCreateUpdateSink.head.output.cache()
          assert(adminUserCreate.count == 1)
          assert(adminUserCreate.columns.toSet === userDimExpectedColumns)
          assert[String](adminUserCreate, "admin_uuid", "tdc-id1")
          assert[String](adminUserCreate, "admin_created_time", "2019-01-08 02:40:00.0")
          assert[String](adminUserCreate, "school_uuid", "school-id")
          assert[String](adminUserCreate, "role_uuid", "TDC")
          assert[Int](adminUserCreate, "admin_status", ActiveEnabled)

          val adminUserUpdate = adminUserCreateUpdateSink.tail.head.output.cache()
          assert(adminUserUpdate.count == 1)
          assert(adminUserUpdate.columns.toSet === userDimExpectedColumns)
          assert[String](adminUserUpdate, "admin_uuid", "tdc-id1")
          assert[String](adminUserUpdate, "school_uuid", "school-id")
          assert[String](adminUserUpdate, "role_uuid", "PRINCIPAL")
          assert[Int](adminUserUpdate, "admin_status", ActiveEnabled)
        }
      )
    }
  }

  test("user deleted event"){
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserDeletedSource,
          value = """
                    |[{
                    |  "eventType":"UserDeletedEvent",
                    |  "tenantId":"tenantId",
                    |  "uuid": "user_deleted_uuid",
                    |  "occurredOn": "2020-02-18 11:48:46"
                    |}]
                  """.stripMargin
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":3,
                    |"admin_id":"user_deleted_uuid",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val adminUserDeleteSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserDeleteSink.size == 1)
          val adminDeletedDf = adminUserDeleteSink.head.output.cache()
          assert(adminDeletedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminDeletedDf, "admin_uuid", "user_deleted_uuid")
          assert[Boolean](adminDeletedDf, "admin_expirable", true)
          assert[String](adminDeletedDf, "admin_created_time", "2020-02-18 11:48:46.0")
          assert[Int](adminDeletedDf, "admin_status", Deleted)
        }
      )
    }
  }

  test("user created and deleted in same batch"){
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value = """
                    |[
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserCreatedEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-id"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"TDC",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |}
                    |]
                  """.stripMargin
        ),
        SparkFixture(
          key = ParquetUserDeletedSource,
          value = """
                    |[{
                    |  "eventType":"UserDeletedEvent",
                    |  "tenantId":"tenant-id",
                    |  "uuid": "tdc-id1",
                    |  "occurredOn": "2020-02-18 11:48:46"
                    |}]
                  """.stripMargin
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":3,
                    |"admin_id":"tdc-id2",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val allSinks = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(allSinks.size == 2)

          val adminCreatedSink = allSinks.filter(_.eventType == UserCreated)
          val adminCreatedDf = adminCreatedSink.head.output.cache()
          assert(adminCreatedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminCreatedDf, "admin_uuid", "tdc-id1")
          assert[Boolean](adminCreatedDf, "admin_expirable", true)
          assert[String](adminCreatedDf, "admin_created_time", "2019-01-08 02:40:00.0")
          assert[Int](adminCreatedDf, "admin_status", ActiveEnabled)

          val adminDeletedSink = allSinks.filter(_.eventType == UserDeletedEvent)
          val adminDeletedDf = adminDeletedSink.head.output.cache()
          assert(adminDeletedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminDeletedDf, "admin_uuid", "tdc-id1")
          assert[String](adminDeletedDf, "role_uuid", "TDC")
          assert[Boolean](adminDeletedDf, "admin_expirable", true)
          assert[String](adminDeletedDf, "admin_created_time", "2020-02-18 11:48:46.0")
          assert[Int](adminDeletedDf, "admin_status", Deleted)
        }
      )
    }
  }

  test("user updated and deleted in same batch"){
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value = """
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserUpdatedEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-id1"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"TDC",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |}
                    |""".stripMargin
        ),
        SparkFixture(
          key = ParquetUserDeletedSource,
          value = """
                    |[{
                    |  "eventType":"UserDeletedEvent",
                    |  "tenantId":"tenant-id",
                    |  "uuid": "tdc-id1",
                    |  "occurredOn": "2020-02-18 11:48:46"
                    |}]
                  """.stripMargin
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":3,
                    |"admin_id":"tdc-id1",
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val allSinks = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(allSinks.size == 2)

          val adminUpdatedSink = allSinks.filter(_.eventType == UserUpdateEnableDisable)
          val adminUpdatedDf = adminUpdatedSink.head.output.cache()
          assert(adminUpdatedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminUpdatedDf, "admin_uuid", "tdc-id1")
          assert[Boolean](adminUpdatedDf, "admin_onboarded", true)
          assert[Boolean](adminUpdatedDf, "admin_expirable", true)
          assert[String](adminUpdatedDf, "admin_created_time", "2019-01-08 02:40:00.0")
          assert[Int](adminUpdatedDf, "admin_status", Disabled)

          val adminDeletedSink = allSinks.filter(_.eventType == UserDeletedEvent)
          val adminDeletedDf = adminDeletedSink.head.output.cache()
          assert(adminDeletedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminDeletedDf, "admin_uuid", "tdc-id1")
          assert[String](adminDeletedDf, "role_uuid", "TDC")
          assert[Boolean](adminDeletedDf, "admin_expirable", true)
          assert[String](adminDeletedDf, "admin_created_time", "2020-02-18 11:48:46.0")
          assert[Int](adminDeletedDf, "admin_status", Deleted)
        }
      )
    }
  }

  test("user updated event when there was no created event") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value = """
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserDisabledEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-id"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"TDC",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |}
      """.stripMargin
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id2",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":3,
                    |"admin_id":"tdc-id2",
                    |"admin_dw_id":1,
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 4)

          val s3Sink = sinks.filter(_.name == ParquetUserSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-01-08")

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminUpdatedDf = adminUserCreateUpdateSink.head.output.cache()
          assert(adminUpdatedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminUpdatedDf, "admin_uuid", "tdc-id1")
          assert[String](adminUpdatedDf, "admin_created_time", "2019-01-08 02:40:00.0")
          assert[String](adminUpdatedDf, "school_uuid", "school-id")
          assert[Int](adminUpdatedDf, "admin_status", Disabled)

          val deltaUserCreateUpdateSink = sinks.filter(_.name == DeltaAdminUserSink)
          assert(deltaUserCreateUpdateSink.size == 1)
          val deltaUpdatedDf = deltaUserCreateUpdateSink.head.output.cache()
          assert[String](deltaUpdatedDf, "admin_id", "tdc-id1")
          assert[String](deltaUpdatedDf, "admin_created_time", "2019-01-08 02:40:00.0")
          assert[String](deltaUpdatedDf, "admin_school_id", "school-id")
          assert[Int](deltaUpdatedDf, "admin_status", Disabled)
        }
      )
    }
  }

  test("user updated and disabled event when there was no created event") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value = """
                    |[
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserUpdatedEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-id"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"TDC",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |},
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserDisabledEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-id"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"TDC",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:41:00.0"
                    |}]
      """.stripMargin
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id2",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":3,
                    |"admin_id":"tdc-id2",
                    |"admin_dw_id":1,
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 4)

          val s3Sink = sinks.filter(_.name == ParquetUserSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-01-08")

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminUpdatedDf = adminUserCreateUpdateSink.head.output.cache()
          assert(adminUpdatedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminUpdatedDf, "admin_uuid", "tdc-id1")
          assert[String](adminUpdatedDf, "admin_created_time", "2019-01-08 02:41:00.0")
          assert[String](adminUpdatedDf, "school_uuid", "school-id")
          assert[Int](adminUpdatedDf, "admin_status", Disabled)

        }
      )
    }
  }

  test("user updated and disabled event when there is already data in redshift") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value = """
                    |[
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserUpdatedEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-id"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"TDC",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |},
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserDisabledEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-id"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"TDC",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:41:00.0"
                    |},
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserEnabledEvent",
                    |  "uuid": "tdc-id1",
                    |  "enabled": true,
                    |  "school": {"uuid":"school-id"},
                    |  "avatar": null,
                    |  "onboarded": true,
                    |  "expirable":true,
                    |  "role":"TDC",
                    |  "roles":["TDC"],
                    |  "permissions":["ALL_TDC_PERMISSIONS"],
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "school-id"
                    |  }],
                    |  "occurredOn": "2019-01-09 03:42:00.0"
                    |}]
      """.stripMargin
        ),
        SparkFixture(
          key = ParquetAdminSchoolMovedSource,
          value = """
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "UserMovedBetweenSchoolEvent",
                    |  "uuid": "tdc-id1",
                    |  "role":"TDC",
                    |  "sourceSchoolId": "school-id",
                    |  "targetSchoolId":"HebbySchool",
                    |  "membershipsV3": [{
                    |     "roleId": "role-uuid",
                    |     "role": "TDC",
                    |     "organization": "shared",
                    |     "schoolId": "HebbySchool"
                    |  }],
                    |  "occurredOn": "2019-01-08 02:45:00.00"
                    |}
                  """.stripMargin
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value = """
                    |[{
                    |   "user_dw_id" : 1,
                    |   "user_id":"tdc-id1",
                    |   "user_type":"ADMIN",
                    |   "user_created_time":"2019-01-08 02:40:00.0"
                    |}]
                  """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value = """
                    |[{
                    |   "school_status":1,
                    |   "school_dw_id" : 1,
                    |   "school_id":"school-id",
                    |   "school_created_time": "1970-01-15 02:40:00.0"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value = """
                    |[{
                    |   "role_dw_id":4,
                    |   "role_name": "TDC"
                    |}
                    |]
                  """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value = """
                    |{
                    |"rel_admin_dw_id": 1,
                    |"admin_created_time":"1970-01-15 02:40:00.0",
                    |"admin_updated_time":"1970-01-17 02:40:00.0",
                    |"admin_deleted_time":null,
                    |"admin_dw_created_time":"1970-01-15 02:40:00.0",
                    |"admin_dw_updated_time":null,
                    |"admin_active_until":null,
                    |"admin_status":3,
                    |"admin_id":"tdc-id1",
                    |"admin_dw_id":1,
                    |"admin_avatar": null,
                    |"admin_onboarded": false,
                    |"admin_school_dw_id": 1,
                    |"admin_role_dw_id": 4,
                    |"admin_expirable": true
                    |}
                  """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 4)

          val s3Sink = sinks.filter(_.name == ParquetUserSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-01-08")

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminUpdatedDf = adminUserCreateUpdateSink.head.output.cache()
          assert(adminUpdatedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminUpdatedDf, "admin_uuid", "tdc-id1")
          assert[String](adminUpdatedDf, "admin_created_time", "2019-01-09 03:42:00.0")
          assert[String](adminUpdatedDf, "school_uuid", "HebbySchool")
          assert[Int](adminUpdatedDf, "admin_status", ActiveEnabled)

        }
      )
    }
  }

  test("user enabled, and school moved event it is disabled in redshift") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value =
            """
              |[
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserUpdatedEvent",
              |  "uuid": "tdc-id1",
              |  "enabled": true,
              |  "school": {"uuid":"school-id"},
              |  "avatar": "new_avatar",
              |  "onboarded": true,
              |  "expirable":true,
              |  "role":"TDC",
              |  "roles":["TDC"],
              |  "permissions":["ALL_TDC_PERMISSIONS"],
              |  "membershipsV3": [{
              |     "roleId": "role-uuid",
              |     "role": "TDC",
              |     "organization": "shared",
              |     "schoolId": "school-id"
              |  }],
              |  "occurredOn": "2019-01-08 02:40:00.0"
              |},
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserEnabledEvent",
              |  "uuid": "tdc-id1",
              |  "enabled": true,
              |  "school": {"uuid":"school-id"},
              |  "avatar": "new_avatar",
              |  "onboarded": true,
              |  "expirable":true,
              |  "role":"TDC",
              |  "roles":["TDC"],
              |  "permissions":["ALL_TDC_PERMISSIONS"],
              |  "membershipsV3": [{
              |     "roleId": "role-uuid",
              |     "role": "TDC",
              |     "organization": "shared",
              |     "schoolId": "school-id"
              |  }],
              |  "occurredOn": "2019-01-08 02:41:00.0"
              |},
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserUpdatedEvent",
              |  "uuid": "tdc-id1",
              |  "enabled": true,
              |  "school": {"uuid":"school-id"},
              |   "avatar": "new_avatar_1",
              |  "onboarded": false,
              |  "expirable":false,
              |  "role":"TDC",
              |  "roles":["TDC"],
              |  "permissions":["ALL_TDC_PERMISSIONS"],
              |  "membershipsV3": [{
              |     "roleId": "role-uuid",
              |     "role": "TDC",
              |     "organization": "shared",
              |     "schoolId": "school-id"
              |  }],
              |  "occurredOn": "2019-01-09 02:42:00.0"
              |},
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserUpdatedEvent",
              |  "uuid": "tdc-id1",
              |  "enabled": true,
              |  "school": {"uuid":"school-id"},
              |   "avatar": "new_avatar_2",
              |  "onboarded": true,
              |  "expirable":false,
              |  "role":"TDC",
              |  "roles":["TDC"],
              |  "permissions":["ALL_TDC_PERMISSIONS"],
              |  "membershipsV3": [{
              |     "roleId": "role-uuid",
              |     "role": "TDC",
              |     "organization": "shared",
              |     "schoolId": "school-id"
              |  }],
              |  "occurredOn": "2019-01-09 02:44:00.0"
              |}]
    """.stripMargin
        ),
        SparkFixture(
          key = ParquetAdminSchoolMovedSource,
          value =
            """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserMovedBetweenSchoolEvent",
              |  "uuid": "tdc-id1",
              |  "role":"TDC",
              |  "sourceSchoolId": "school-id",
              |  "targetSchoolId":"HebbySchool",
              |  "membershipsV3": [{
              |     "roleId": "role-uuid",
              |     "role": "TDC",
              |     "organization": "shared",
              |     "schoolId": "HebbySchool"
              |  }],
              |  "occurredOn": "2019-01-09 02:43:00.00"
              |}
                """.stripMargin
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value =
            """
              |[{
              |   "user_dw_id" : 1,
              |   "user_id":"tdc-id1",
              |   "user_type":"ADMIN",
              |   "user_created_time":"2019-01-08 02:40:00.0"
              |}
              |]
                """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value =
            """
              |[{
              |   "school_status":1,
              |   "school_dw_id" : 1,
              |   "school_id":"school-id",
              |   "school_created_time": "1970-01-15 02:40:00.0"
              |}
              |]
                """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value =
            """
              |[{
              |   "role_dw_id":4,
              |   "role_name": "TDC"
              |}
              |]
                """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value =
            """
              |{
              |"rel_admin_dw_id": 1,
              |"admin_created_time":"1970-01-15 02:40:00.0",
              |"admin_updated_time":"1970-01-17 02:40:00.0",
              |"admin_deleted_time":null,
              |"admin_dw_created_time":"1970-01-15 02:40:00.0",
              |"admin_dw_updated_time":null,
              |"admin_active_until":null,
              |"admin_status":3,
              |"admin_id":"tdc-id1",
              |"admin_dw_id":1,
              |"admin_avatar": null,
              |"admin_onboarded": false,
              |"admin_school_dw_id": 1,
              |"admin_role_dw_id": 4,
              |"admin_expirable": true
              |}
                """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 4)

          val s3Sink = sinks.filter(_.name == ParquetUserSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-01-08")

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminUpdatedDf = adminUserCreateUpdateSink.head.output.cache()
          assert(adminUpdatedDf.columns.toSet === userDimExpectedColumns)
          assert[String](adminUpdatedDf, "admin_uuid", "tdc-id1")
          assert[String](adminUpdatedDf, "admin_created_time", "2019-01-09 02:44:00.0")
          assert[String](adminUpdatedDf, "school_uuid", "HebbySchool")
          assert[String](adminUpdatedDf, "admin_avatar", "new_avatar_2")
          assert[Boolean](adminUpdatedDf, "admin_onboarded", true)
          assert[Boolean](adminUpdatedDf, "admin_expirable", false)
          assert[Int](adminUpdatedDf, "admin_status", ActiveEnabled)

        }
      )
    }
  }

  test("user created, exclude from report false then moved to another school") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value =
            """
              |[
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserUpdatedEvent",
              |  "uuid": "tdc-id1",
              |  "enabled": true,
              |  "school": {"uuid":"school-id"},
              |   "avatar": "new_avatar_2",
              |  "onboarded": true,
              |  "expirable":false,
              |  "role":"TDC",
              |  "roles":["TDC"],
              |  "permissions":["ALL_TDC_PERMISSIONS"],
              |  "membershipsV3": [{
              |     "roleId": "role-uuid",
              |     "role": "TDC",
              |     "organization": "shared",
              |     "schoolId": "school-id"
              |  }],
              |  "occurredOn": "2019-01-09 02:44:00.0",
              |  "excludeFromReport": false
              |}]
  """.stripMargin
        ),
        SparkFixture(
          key = ParquetAdminSchoolMovedSource,
          value =
            """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserMovedBetweenSchoolEvent",
              |  "uuid": "tdc-id1",
              |  "role":"TDC",
              |  "sourceSchoolId": "school-id",
              |  "targetSchoolId":"HebbySchool",
              |  "membershipsV3": [{
              |     "roleId": "role-uuid",
              |     "role": "TDC",
              |     "organization": "shared",
              |     "schoolId": "HebbySchool"
              |  }],
              |  "occurredOn": "2019-01-09 02:43:00.00"
              |}
              """.stripMargin
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value =
            """
              |[{
              |   "user_dw_id" : 1,
              |   "user_id":"tdc-id1",
              |   "user_type":"ADMIN",
              |   "user_created_time":"2019-01-08 02:40:00.0"
              |}
              |]
              """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value =
            """
              |[{
              |   "school_status":1,
              |   "school_dw_id" : 1,
              |   "school_id":"school-id",
              |   "school_created_time": "1970-01-15 02:40:00.0"
              |}
              |]
              """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value =
            """
              |[{
              |   "role_dw_id":4,
              |   "role_name": "TDC"
              |}
              |]
              """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value =
            """
              |{
              |"rel_admin_dw_id": 1,
              |"admin_created_time":"1970-01-15 02:40:00.0",
              |"admin_updated_time":"1970-01-17 02:40:00.0",
              |"admin_deleted_time":null,
              |"admin_dw_created_time":"1970-01-15 02:40:00.0",
              |"admin_dw_updated_time":null,
              |"admin_active_until":null,
              |"admin_status":3,
              |"admin_id":"tdc-id1",
              |"admin_dw_id":1,
              |"admin_avatar": null,
              |"admin_onboarded": false,
              |"admin_school_dw_id": 1,
              |"admin_role_dw_id": 4,
              |"admin_expirable": true,
              |"admin_exclude_from_report": false
              |}
              """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 4)

          val s3Sink = sinks.filter(_.name == ParquetUserSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-01-09")

          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          assert(adminUserCreateUpdateSink.size == 1)
          val adminUpdatedDf = adminUserCreateUpdateSink.head.output.cache()
          assert[Boolean](adminUpdatedDf, "admin_exclude_from_report", false)

        }
      )
    }
  }

  test("user created, exclude from report true then moved to another school") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetUserSource,
          value =
            """
              |[
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserCreatedEvent",
              |  "uuid": "tdc-id1",
              |  "enabled": true,
              |  "school": {"uuid":"school-id"},
              |   "avatar": "new_avatar_2",
              |  "onboarded": true,
              |  "expirable":false,
              |  "role":"TDC",
              |  "roles":["TDC"],
              |  "permissions":["ALL_TDC_PERMISSIONS"],
              |  "membershipsV3": [{
              |     "roleId": "role-uuid",
              |     "role": "TDC",
              |     "organization": "shared",
              |     "schoolId": "school-id"
              |  }],
              |  "occurredOn": "2019-01-09 02:44:00.0",
              |  "excludeFromReport": true
              |}]
  """.stripMargin
        ),
        SparkFixture(
          key = ParquetAdminSchoolMovedSource,
          value =
            """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "UserMovedBetweenSchoolEvent",
              |  "uuid": "tdc-id1",
              |  "role":"TDC",
              |  "sourceSchoolId": "school-id",
              |  "targetSchoolId":"HebbySchool",
              |  "membershipsV3": [{
              |     "roleId": "role-uuid",
              |     "role": "TDC",
              |     "organization": "shared",
              |     "schoolId": "HebbySchool"
              |  }],
              |  "occurredOn": "2019-01-09 02:43:00.00"
              |}
              """.stripMargin
        ),
        SparkFixture(
          key = RedshiftUserSink,
          value =
            """
              |[{
              |   "user_dw_id" : 1,
              |   "user_id":"tdc-id1",
              |   "user_type":"ADMIN",
              |   "user_created_time":"2019-01-08 02:40:00.0"
              |}
              |]
              """.stripMargin,
          schema = Some(Encoders.product[User].schema)
        ),
        SparkFixture(
          key = RedshiftSchoolSink,
          value =
            """
              |[{
              |   "school_status":1,
              |   "school_dw_id" : 1,
              |   "school_id":"school-id",
              |   "school_created_time": "1970-01-15 02:40:00.0"
              |}
              |]
              """.stripMargin,
          schema = Some(Encoders.product[SchoolDim].schema)
        ),
        SparkFixture(
          key = RedshiftRoleSource,
          value =
            """
              |[{
              |   "role_dw_id":4,
              |   "role_name": "TDC"
              |}
              |]
              """.stripMargin,
          schema = Some(Encoders.product[RoleDim].schema)
        ),
        SparkFixture(
          key = RedshiftAdminUserDimSink,
          value =
            """
              |{
              |"rel_admin_dw_id": 1,
              |"admin_created_time":"1970-01-15 02:40:00.0",
              |"admin_updated_time":"1970-01-17 02:40:00.0",
              |"admin_deleted_time":null,
              |"admin_dw_created_time":"1970-01-15 02:40:00.0",
              |"admin_dw_updated_time":null,
              |"admin_active_until":null,
              |"admin_status":3,
              |"admin_id":"tdc-id1",
              |"admin_dw_id":1,
              |"admin_avatar": null,
              |"admin_onboarded": false,
              |"admin_school_dw_id": 1,
              |"admin_role_dw_id": 4,
              |"admin_expirable": true,
              |"admin_exclude_from_report": true
              |}
              """.stripMargin,
          schema = Some(Encoders.product[AdminDim].schema)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val adminUserCreateUpdateSink = sinks.filter(_.name == RedshiftAdminUserSink)
          val adminUpdatedDf = adminUserCreateUpdateSink.head.output.cache()
          assert[Boolean](adminUpdatedDf, "admin_exclude_from_report", true)

        }
      )
    }
  }

}
