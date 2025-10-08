package com.alefeducation.facts.user_login

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class UserLoginAggregationSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns = Set(
    "ful_id",
    "ful_date_dw_id",
    "role_uuid",
    "tenant_uuid",
    "school_uuid",
    "user_uuid",
    "ful_outside_of_school",
    "ful_login_time",
    "ful_created_time",
    "ful_dw_created_time",
    "eventdate"
  )

  test("transform in user login any user fact successfully") {

    val value = """
                    |[
                    |{
                    |  "eventType": "LoggedInEvent",
                    |  "uuid": "teacher-id",
                    |  "name": "user-name",
                    |  "schools": [{"uuid": "school-id-1"}],
                    |  "roles": ["TEACHER"],
                    |  "createdOn": 123456723,
                    |  "outsideOfSchool":true,
                    |  "eventDateDw": 19700101,
                    |  "occurredOn": "1970-07-14 02:40:00.0",
                    |  "tenantId": "tenant-id"
                    |},
                    |{
                    |  "eventType": "LoggedInEvent",
                    |  "uuid": "user-id-1",
                    |  "name": "user-name-1",
                    |  "schools": [{"uuid": "school-id-1"}],
                    |  "roles": ["TDC"],
                    |  "createdOn": 123456723,
                    |  "outsideOfSchool":true,
                    |  "eventDateDw": 19700101,
                    |  "occurredOn": "1970-07-14 02:40:00.0",
                    |  "tenantId": "tenant-id"
                    |}]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._
    val transformer = new UserLoginAggregationTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetUserLoginSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output
    val tdc = df.filter($"role_uuid" === "TDC")
    val teacher = df.filter($"role_uuid" === "TEACHER")

    assert(df.columns.toSet === expectedColumns)
    assert[String](tdc, "ful_id", "n/a")
    assert[String](tdc, "ful_created_time", "1970-07-14 02:40:00.0")
    assert[String](tdc, "user_uuid", "user-id-1")
    assert[String](tdc, "role_uuid", "TDC")
    assert[String](tdc, "ful_login_time", "1970-07-14 02:40:00.0")
    assert[String](teacher, "ful_id", "n/a")
    assert[String](teacher, "ful_created_time", "1970-07-14 02:40:00.0")
    assert[String](teacher, "user_uuid", "teacher-id")
    assert[String](teacher, "role_uuid", "TEACHER")
    assert[String](teacher, "ful_login_time", "1970-07-14 02:40:00.0")
  }

  test("transform in user login for students in fact successfully") {
    val value = """
                    |[{
                    |  "eventType": "LoggedInEvent",
                    |  "uuid": "student-id",
                    |  "name": "user-name",
                    |  "schools": [{"uuid": "school-id-1"}],
                    |  "roles": ["STUDENT"],
                    |  "createdOn": 123456723,
                    |  "outsideOfSchool":true,
                    |  "eventDateDw": 19700101,
                    |  "occurredOn": "1970-07-14 02:40:00.0",
                    |  "tenantId": "tenant-id"
                    |}]
      """.stripMargin
    val sprk = spark
    import sprk.implicits._
    val transformer = new UserLoginAggregationTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetUserLoginSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output
    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "ful_id", "n/a")
    assert[String](df, "ful_created_time", "1970-07-14 02:40:00.0")
    assert[String](df, "user_uuid", "student-id")
    assert[String](df, "role_uuid", "STUDENT")
  }

  test("transform in user login for principal in fact successfully") {

    val value = """
                    |[{
                    |  "eventType": "LoggedInEvent",
                    |  "uuid": "principal-id",
                    |  "name": "user-name",
                    |  "schools": [{"uuid": "school-id-1"}],
                    |  "roles": ["PRINCIPAL"],
                    |  "createdOn": 123456723,
                    |  "outsideOfSchool":true,
                    |  "eventDateDw": 19700101,
                    |  "occurredOn": "1970-07-14 02:40:00.0",
                    |  "tenantId": "tenant-id"
                    |}]
      """.stripMargin
    val sprk = spark
    import sprk.implicits._
    val transformer = new UserLoginAggregationTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetUserLoginSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output
    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "ful_id", "n/a")
    assert[String](df, "ful_created_time", "1970-07-14 02:40:00.0")
    assert[String](df, "user_uuid", "principal-id")
    assert[String](df, "role_uuid", "PRINCIPAL")

  }

  test("transform in user login with delta sink") {
    val value = """
                    |[{
                    |  "ful_id": "n/a",
                    |  "user_uuid": "principal-id",
                    |  "school_uuid": "school-id-1",
                    |  "role_uuid": "PRINCIPAL",
                    |  "ful_outside_of_school":true,
                    |  "ful_date_dw_id": 19700101,
                    |  "ful_created_time": "1970-07-14 02:40:00.0",
                    |  "ful_dw_created_time": "1970-07-14 02:40:00.0",
                    |  "ful_login_time": "1970-07-14 02:40:00.0",
                    |  "tenant_uuid": "tenant-id"
                    |}]
                  """.stripMargin

    val expectedDeltaColumns = Set(
      "ful_id",
      "ful_date_dw_id",
      "ful_role_id",
      "ful_tenant_id",
      "ful_school_id",
      "ful_user_id",
      "ful_outside_of_school",
      "ful_login_time",
      "ful_created_time",
      "ful_dw_created_time",
      "eventdate"
    )
    val sprk = spark
    import sprk.implicits._
    val transformer = new UserLoginAggregationDelta(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetUserLoginTransformedSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedDeltaColumns)
    assert[String](df, "ful_id", "n/a")
    assert[String](df, "ful_created_time", "1970-07-14 02:40:00.0")
    assert[String](df, "ful_user_id", "principal-id")
    assert[String](df, "ful_role_id", "PRINCIPAL")
  }

  test("transform in user login when role does not exist") {
    val value = """
                    |[{
                    |  "eventType": "LoggedInEvent",
                    |  "uuid": "principal-id",
                    |  "name": "user-name",
                    |  "schools": [{"uuid": "school-id-1"}],
                    |  "roles": ["XYZ_ROLE"],
                    |  "createdOn": 123456723,
                    |  "outsideOfSchool":true,
                    |  "eventDateDw": 19700101,
                    |  "occurredOn": "1970-07-14 02:40:00.0",
                    |  "tenantId": "tenant-id"
                    |}]
      """.stripMargin
    val sprk = spark
    import sprk.implicits._
    val transformer = new UserLoginAggregationTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetUserLoginSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform()
    assert(df === None)
  }
}
