package com.alefeducation.facts.heartbeat

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.heartbeat.HeartbeatAggregationTransform.{DeltaHeartbeatRaw, HeartbeatOffsetTimestampKey}
import com.alefeducation.models.StudentModel.SchoolDim
import com.alefeducation.models.UserModel.User
import com.alefeducation.schema.admin.{ContentRepositoryRedshift, DwIdMapping}
import com.alefeducation.util.Helpers.{RedshiftContentRepositorySource, RedshiftDwIdMappingSource, RedshiftSchoolSink, RedshiftUserSink}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Timestamp

class HeartbeatAggregationTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "eventdate",
    "fuhha_created_time",
    "fuhha_dw_created_time",
    "fuhha_date_dw_id",
    "fuhha_user_id",
    "fuhha_user_dw_id",
    "fuhha_role",
    "fuhha_school_id",
    "fuhha_school_dw_id",
    "fuhha_content_repository_id",
    "fuhha_content_repository_dw_id",
    "fuhha_tenant_id",
    "fuhha_channel",
    "fuhha_activity_date_hour",
    "fuhe_dw_created_time"
  )

  val relUserData: String =
    """
      |[
      |{
      | "user_id": "user_id_1",
      | "user_dw_id": 100,
      | "user_created_time": "2023-01-01",
      | "user_type": "",
      | "user_dw_created_time": ""
      |},
      |{
      | "user_id": "user_id_1.2",
      | "user_dw_id": 222,
      | "user_created_time": "2022-01-01",
      | "user_type": "",
      | "user_dw_created_time": ""
      |},
      |{
      | "user_id": "user_id_2",
      | "user_dw_id": 200,
      | "user_created_time": "2018-01-01",
      | "user_type": "",
      | "user_dw_created_time": ""
      |},
      |{
      | "user_id": "user_id_with_no_school",
      | "user_dw_id": 300,
      | "user_created_time": "2018-01-01",
      | "user_type": "",
      | "user_dw_created_time": ""
      |}
      |]
      |""".stripMargin


  val schoolData: String =
    """
      |{
      | "school_dw_id": 1,
      | "school_id": "school_id_1",
      | "school_content_repository_dw_id": 1
      |}
      |""".stripMargin

  val contentRepositoryData: String =
    """
      |{
      | "content_repository_dw_id": 1,
      | "content_repository_id": "content_repository_id_1"
      |}
      |""".stripMargin

  test("should return empty DataSink on Heartbeat event when no data in Raw source") {
    val sprk = spark
    import sprk.implicits._

    val transformer = new HeartbeatAggregationTransform(sprk, service)

    when(service.readOptional(DeltaHeartbeatRaw, sprk)).thenReturn(None)

    val studentDF = spark.read.json(Seq(relUserData).toDS())
    when(
      service.readFromRedshift[User](
        RedshiftUserSink,
        withoutDropDuplicates = true
      )).thenReturn(studentDF)

    val schoolDF = spark.read.json(Seq(schoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink)).thenReturn(schoolDF)

    val contentRepositoryDF = spark.read.json(Seq(contentRepositoryData).toDS())
    when(service.readFromRedshift[ContentRepositoryRedshift](RedshiftContentRepositorySource)).thenReturn(contentRepositoryDF)

    when(service.getDataOffset(HeartbeatOffsetTimestampKey)).thenReturn(Timestamp.valueOf("1970-01-01 00:00:00"))

    val dataSink = transformer.transform()

    assert(dataSink.isEmpty)
  }

  test("should return empty DataSink on Heartbeat event when raw data is too old") {
    val deltaRawData =
      """
          |[
          |{
          | "fuhe_created_time": "2002-09-04 00:00:00.000",
          | "fuhe_dw_created_time": "2002-09-04 00:00:10.000",
          | "fuhe_date_dw_id": "20020904",
          | "fuhe_user_id": "user_id_1",
          | "fuhe_role": "STUDENT",
          | "fuhe_channel": "WEB",
          | "fuhe_tenant_id": "93e4949d-7eff-4707-9201-dac917a5e013",
          | "fuhe_school_id": "93e4949d-7eff-4707-9201-dac917a5e013",
          | "eventdate": "2002-09-04"
          |},
          |{
          | "fuhe_created_time": "2002-09-04 00:22:00.000",
          | "fuhe_dw_created_time": "2002-09-04 00:23:00.000",
          | "fuhe_date_dw_id": "20020904",
          | "fuhe_user_id": "user_id_x",
          | "fuhe_role": "STUDENT",
          | "fuhe_channel": "WEB",
          | "fuhe_tenant_id": "93e4949d-7eff-4707-9201-dac917a5e013",
          | "fuhe_school_id": "93e4949d-7eff-4707-9201-dac917a5e013",
          | "eventdate": "2002-09-04"
          |},
          |{
          | "fuhe_created_time": "2023-09-04 00:01:00.000",
          | "fuhe_dw_created_time": "2023-09-04 00:01:10.000",
          | "fuhe_date_dw_id": "20230904",
          | "fuhe_user_id": "user_id_with_no_school",
          | "fuhe_role": "TEACHER",
          | "fuhe_channel": "MOBILE",
          | "fuhe_tenant_id": "93e4949d-7eff-4707-9201-dac917a5e013",
          | "fuhe_school_id": "93e4949d-7eff-4707-9201-dac917a5e013",
          | "eventdate": "2023-09-04"
          |}
          |]
          |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val transformer = new HeartbeatAggregationTransform(sprk, service)

    val deltaRawDF = spark.read.json(Seq(deltaRawData).toDS())
    when(service.readOptional(DeltaHeartbeatRaw, sprk)).thenReturn(Some(deltaRawDF))

    val studentDF = spark.read.json(Seq(relUserData).toDS())
    when(
      service.readFromRedshift[User](
        RedshiftUserSink,
        withoutDropDuplicates = true
      )).thenReturn(studentDF)

    val schoolDF = spark.read.json(Seq(schoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink)).thenReturn(schoolDF)

    val contentRepositoryDF = spark.read.json(Seq(contentRepositoryData).toDS())
    when(service.readFromRedshift[ContentRepositoryRedshift](RedshiftContentRepositorySource)).thenReturn(contentRepositoryDF)

    when(service.getDataOffset(HeartbeatOffsetTimestampKey)).thenReturn(Timestamp.valueOf("2023-11-01 00:00:00.0"))

    val df = transformer.transform().get.output.cache()

    assert(df.isEmpty)
  }

  test("should transform Heartbeat event successfully when no corresponding student and school found") {
    val deltaRawData =
      """
        |[
        |{
        | "fuhe_created_time": "2018-09-04 00:00:00.000",
        | "fuhe_dw_created_time": "2018-09-04 00:00:10.000",
        | "fuhe_date_dw_id": "20180904",
        | "fuhe_user_id": "user_id_1",
        | "fuhe_role": "STUDENT",
        | "fuhe_channel": "WEB",
        | "fuhe_tenant_id": "93e4949d-7eff-4707-9201-dac917a5e013",
        | "fuhe_school_id": "93e4949d-7eff-4707-9201-dac917a5e013",
        | "eventdate": "2018-09-04"
        |},
        |{
        | "fuhe_created_time": "2018-09-04 00:22:00.000",
        | "fuhe_dw_created_time": "2018-09-04 00:23:00.000",
        | "fuhe_date_dw_id": "20180904",
        | "fuhe_user_id": "user_id_x",
        | "fuhe_role": "STUDENT",
        | "fuhe_channel": "WEB",
        | "fuhe_tenant_id": "93e4949d-7eff-4707-9201-dac917a5e013",
        | "fuhe_school_id": null,
        | "eventdate": "2018-09-04"
        |},
        |{
        | "fuhe_created_time": "2018-09-04 00:01:00.000",
        | "fuhe_dw_created_time": "2018-09-04 00:01:10.000",
        | "fuhe_date_dw_id": "20180904",
        | "fuhe_user_id": "user_id_with_no_school",
        | "fuhe_role": "TEACHER",
        | "fuhe_channel": "MOBILE",
        | "fuhe_tenant_id": "93e4949d-7eff-4707-9201-dac917a5e013",
        | "fuhe_school_id": null,
        | "eventdate": "2018-09-04"
        |}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val transformer = new HeartbeatAggregationTransform(sprk, service)

    val deltaRawDF = spark.read.json(Seq(deltaRawData).toDS())
    when(service.readOptional(DeltaHeartbeatRaw, sprk)).thenReturn(Some(deltaRawDF))

    val studentDF = spark.read.json(Seq(relUserData).toDS())
    when(
      service.readFromRedshift[User](
        RedshiftUserSink,
        withoutDropDuplicates = true
      )).thenReturn(studentDF)

    val schoolDF = spark.read.json(Seq(schoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink)).thenReturn(schoolDF)

    val contentRepositoryDF = spark.read.json(Seq(contentRepositoryData).toDS())
    when(service.readFromRedshift[ContentRepositoryRedshift](RedshiftContentRepositorySource)).thenReturn(contentRepositoryDF)

    when(service.getDataOffset(HeartbeatOffsetTimestampKey)).thenReturn(Timestamp.valueOf("1970-01-01 00:00:00"))

    val df = transformer.transform().get.output.cache()

    assert(expectedColumns.subsetOf(df.columns.toSet))
    assert(df.count() == 3)

    val student = df.filter(($"fuhha_role" === "STUDENT") && ($"fuhha_user_dw_id".isNull))
    assert[String](student, "fuhha_user_id", "user_id_x")
    assert[String](student, "fuhha_channel", "WEB")
    assert[String](student, "fuhha_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](student, "fuhha_activity_date_hour", "2018-09-04 00:00:00.0")
    assert[String](student, "fuhha_school_id", null)
    assert[Object](student, "fuhha_school_dw_id", null)
    assert[Object](student, "fuhha_content_repository_dw_id", null)
    assert[String](student, "fuhha_content_repository_id", null)

    val teacher = df.filter($"fuhha_role" === "TEACHER")
    assert[String](teacher, "fuhha_user_id", "user_id_with_no_school")
    assert[String](teacher, "fuhha_channel", "MOBILE")
    assert[String](teacher, "fuhha_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](teacher, "fuhha_activity_date_hour", "2018-09-04 00:00:00.0")
    assert[Int](teacher, "fuhha_user_dw_id", 300)
    assert[String](teacher, "fuhha_school_id", null)
    assert[Object](teacher, "fuhha_content_repository_dw_id", null)
    assert[String](teacher, "fuhha_content_repository_id", null)
  }

  test("should transform Heartbeat event successfully") {
    val deltaRawData =
      """
        |[
        |{
        | "fuhe_created_time": "2018-09-04 00:30:00.000",
        | "fuhe_dw_created_time": "2018-09-04 00:30:10.000",
        | "fuhe_date_dw_id": "20180904",
        | "fuhe_user_id": "user_id_1.2",
        | "fuhe_role": "STUDENT",
        | "fuhe_channel": "WEB",
        | "fuhe_tenant_id": "93e4949d-7eff-4707-9201-dac917a5e013",
        | "fuhe_school_id": "93e4949d-7eff-4707-9201-dac917a5e013",
        | "eventdate": "2018-09-04"
        |},
        |{
        | "fuhe_created_time": "2018-09-04 01:22:00.000",
        | "fuhe_dw_created_time": "2018-09-04 01:23:00.000",
        | "fuhe_date_dw_id": "20180904",
        | "fuhe_user_id": "user_id_1",
        | "fuhe_role": "STUDENT",
        | "fuhe_channel": "WEB",
        | "fuhe_tenant_id": "93e4949d-7eff-4707-9201-dac917a5e013",
        | "fuhe_school_id": "93e4949d-7eff-4707-9201-dac917a5e013",
        | "eventdate": "2018-09-04"
        |},
        |{
        | "fuhe_created_time": "2018-09-04 00:01:00.000",
        | "fuhe_dw_created_time": "2018-09-04 03:00:10.000",
        | "fuhe_date_dw_id": "20180904",
        | "fuhe_user_id": "user_id_2",
        | "fuhe_role": "TEACHER",
        | "fuhe_channel": "MOBILE",
        | "fuhe_tenant_id": "tenant_id_2",
        | "fuhe_school_id": "school_id_1",
        | "eventdate": "2018-09-04"
        |}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val transformer = new HeartbeatAggregationTransform(sprk, service)

    val deltaRawDF =
      spark.read.json(Seq(deltaRawData).toDS()).withColumn("fuhe_dw_created_time", $"fuhe_dw_created_time".cast(TimestampType))
    when(transformer.service.readOptional(DeltaHeartbeatRaw, sprk)).thenReturn(Some(deltaRawDF))

    val studentDF = spark.read.json(Seq(relUserData).toDS())
    when(
      service.readFromRedshift[User](
        RedshiftUserSink,
        withoutDropDuplicates = true
      )).thenReturn(studentDF)

    val schoolDF = spark.read.json(Seq(schoolData).toDS())
    when(transformer.service.readFromRedshift[SchoolDim](RedshiftSchoolSink)).thenReturn(schoolDF)

    val contentRepositoryDF = spark.read.json(Seq(contentRepositoryData).toDS())
    when(service.readFromRedshift[ContentRepositoryRedshift](RedshiftContentRepositorySource)).thenReturn(contentRepositoryDF)

    when(service.getDataOffset(HeartbeatOffsetTimestampKey)).thenReturn(Timestamp.valueOf("2018-09-04 01:23:00.0"))

    val df = transformer.transform().get.output.cache()

    assert(expectedColumns.subsetOf(df.columns.toSet))
    assert(df.count() == 1)

    assert[String](df, "fuhha_role", "TEACHER")
    assert[String](df, "fuhha_user_id", "user_id_2")
    assert[String](df, "fuhha_channel", "MOBILE")
    assert[String](df, "fuhha_tenant_id", "tenant_id_2")
    assert[String](df, "fuhha_activity_date_hour", "2018-09-04 00:00:00.0")
    assert[Int](df, "fuhha_user_dw_id", 200)
    assert[String](df, "fuhha_school_id", "school_id_1")
    assert[Int](df, "fuhha_school_dw_id", 1)
    assert[Int](df, "fuhha_content_repository_dw_id", 1)
    assert[String](df, "fuhha_content_repository_id", "content_repository_id_1")
  }
}
