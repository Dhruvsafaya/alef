package com.alefeducation.facts.user_avatar

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.user_avatar.UserAvatarTransform.{UserAvatarEntity, UserAvatarKey, UserAvatarService}
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Resources.{getSink, getSource}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class UserAvatarSpec extends SparkSuite {

  val service = mock[SparkBatchService]

  val expectedColumns = Set(
    "fua_tenant_id",
    "fua_dw_id",
    "fua_user_id",
    "fua_school_id",
    "fua_grade_id",
    "fua_id",
    "fua_avatar_type",
    "fua_avatar_file_id",
    "fua_created_time",
    "fua_dw_created_time",
    "fua_date_dw_id",
    "eventdate"
  )

  test("transform user avatar events successfully") {

    val expJson =
      """
        |[
        |{"fua_dw_id":1043,"fua_school_id":"3a207ff4-4b86-405b-abe0-7b9771edf905","fua_avatar_type":null,"fua_avatar_file_id":null,"fua_user_id":"5ba72728-f6eb-4ddd-ab85-9aa20815b5f6","fua_tenant_id":"93e4949d-7eff-4707-9201-dac917a5e013","fua_id":"avatar_43","fua_grade_id":"d3296950-4eac-4706-aeb8-de071963fdbb","eventdate":"2023-09-06","fua_date_dw_id":"20230906","fua_created_time":"2023-09-06T05:20:00.914Z","fua_dw_created_time":"2023-09-06T12:00:01.531Z"},
        |{"fua_dw_id":1044,"fua_school_id":"b040236e-3cc0-4db2-a289-710273568368","fua_avatar_type":"PREDEFINED","fua_avatar_file_id":"c040236e-3cc0-4db2-a289-710273568369","fua_user_id":"231924b3-1c34-4d46-84ee-194b552237e5","fua_tenant_id":"93e4949d-7eff-4707-9201-dac917a5e013","fua_id":"avatar_44","fua_grade_id":"90b2ae9c-f882-4190-a4aa-a3e880d93fe2","eventdate":"2023-09-06","fua_date_dw_id":"20230906","fua_created_time":"2023-09-06T07:00:00.000Z","fua_dw_created_time":"2023-09-06T12:00:01.531Z"}
        |]
        |""".stripMargin

    val value =
      """
        |[
        |{
        |   "eventType":"UserAvatarSelectedEvent",
        |   "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
        |   "uuid": "5ba72728-f6eb-4ddd-ab85-9aa20815b5f6",
        | 	"avatarId": "avatar_43",
        |	  "gradeId": "d3296950-4eac-4706-aeb8-de071963fdbb",
        |	  "schoolId": "3a207ff4-4b86-405b-abe0-7b9771edf905",
        |   "avatarType": null,
        |   "avatarFileId": null,
        |   "occurredOn": "2023-09-06T05:20:00.914"
        |},
        |{
        |   "eventType":"UserAvatarUpdatedEvent",
        |   "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
        |   "uuid": "231924b3-1c34-4d46-84ee-194b552237e5",
        |	  "avatarId": "avatar_44",
        |	  "gradeId": "90b2ae9c-f882-4190-a4aa-a3e880d93fe2",
        |	  "schoolId": "b040236e-3cc0-4db2-a289-710273568368",
        |   "avatarType": "PREDEFINED",
        |   "avatarFileId": "c040236e-3cc0-4db2-a289-710273568369",
        |   "occurredOn": "2023-09-06T07:00:00.000"
        |}
        |]
        |""".stripMargin


    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())

    val sourceName = getSource(UserAvatarService).head
    val sinkName = getSink(UserAvatarService).head
    val expDwId = 1043

    when(service.readOptional(sourceName, sprk)).thenReturn(Some(inputDF))
    when(service.getStartIdUpdateStatus(UserAvatarKey)).thenReturn(expDwId)

    val transform = new UserAvatarTransform(spark, service)
    val sinks = transform.transform(sourceName, sinkName)

    val df = sinks.filter(_.name == sinkName).head.output
    assert(df.columns.toSet === expectedColumns)

    val expDf = createDfFromJsonWithTimeCols(spark, UserAvatarEntity, expJson)
    assertSmallDatasetEquality(UserAvatarEntity, df, expDf)
  }

  private def createDfFromJsonWithTimeCols(spark: SparkSession, prefix: String, json: String): DataFrame = {
    val df = createDfFromJson(spark, json)
      .withColumn(s"${prefix}_created_time", col(s"${prefix}_created_time").cast(TimestampType))

    if (df.columns.contains(s"${prefix}_updated_time")) {
      df.withColumn(s"${prefix}_updated_time", col(s"${prefix}_updated_time").cast(TimestampType))
    } else df
  }

}
