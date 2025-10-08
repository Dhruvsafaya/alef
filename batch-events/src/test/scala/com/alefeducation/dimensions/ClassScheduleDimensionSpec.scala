package com.alefeducation.dimensions

import java.util.TimeZone

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Constants.OutcomeAssociationCategoryType
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.scalatest.matchers.must.Matchers

class ClassScheduleDimensionSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: ClassScheduleDimension = new ClassScheduleDimension(ClassScheduleDimensionName, spark)
  }

  test("Class schedule added/delete events") {
    val expParquetJson =
      """
        |[
        |{"classId":"78b9007f-9e36-467d-894b-75d93cc63473","day":"SUNDAY","endTime":"10:00","eventType":"ClassScheduleSlotAddedEvent","occurredOn":"2020-07-20 02:40:00.0","startTime":"09:00","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","eventdate":"2020-07-20"},
        |{"classId":"78b9007f-9e36-467d-894b-75d93cc63473","day":"SUNDAY","endTime":"10:00","eventType":"ClassScheduleSlotDeletedEvent","occurredOn":"2020-07-20 02:41:00.0","startTime":"09:00","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","eventdate":"2020-07-20"},
        |{"classId":"602c7ee0-3095-4231-835a-244d82b13674","day":"MONDAY","endTime":"11:00","eventType":"ClassScheduleSlotAddedEvent","occurredOn":"2020-07-20 02:42:00.0","startTime":"10:00","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","eventdate":"2020-07-20"}
        |]
      """.stripMargin
    val expJson =
      """
        |[
        |{"class_schedule_start_time":"09:00","class_schedule_status":4,"class_schedule_end_time":"10:00","class_schedule_attach_status":1,"class_schedule_updated_time":"2020-07-20T02:41:00.000Z","class_schedule_class_id":"78b9007f-9e36-467d-894b-75d93cc63473","class_schedule_day":"SUNDAY","class_schedule_created_time":"2020-07-20T02:40:00.000Z","class_schedule_dw_created_time":"2020-10-04T10:09:21.469Z","class_schedule_dw_updated_time":null},
        |{"class_schedule_start_time":"09:00","class_schedule_status":1,"class_schedule_end_time":"10:00","class_schedule_attach_status":0,"class_schedule_class_id":"78b9007f-9e36-467d-894b-75d93cc63473","class_schedule_day":"SUNDAY","class_schedule_created_time":"2020-07-20T02:41:00.000Z","class_schedule_dw_created_time":"2020-10-04T10:09:21.469Z","class_schedule_updated_time":null,"class_schedule_dw_updated_time":null},
        |{"class_schedule_start_time":"10:00","class_schedule_status":1,"class_schedule_end_time":"11:00","class_schedule_attach_status":1,"class_schedule_class_id":"602c7ee0-3095-4231-835a-244d82b13674","class_schedule_day":"MONDAY","class_schedule_created_time":"2020-07-20T02:42:00.000Z","class_schedule_dw_created_time":"2020-10-04T10:09:21.469Z","class_schedule_updated_time":null,"class_schedule_dw_updated_time":null}
        |]
      """.stripMargin
    val expDf = createDfFromJsonWithTimeCols(spark, expJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetClassScheduleSource,
          value = """
                    |[
                    |{
                    |   "eventType":"ClassScheduleSlotAddedEvent",
                    |   "day":"SUNDAY",
                    |   "startTime":"09:00",
                    |   "endTime":"10:00",
                    |   "classId":"78b9007f-9e36-467d-894b-75d93cc63473",
                    |   "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
                    |   "occurredOn":"2020-07-20 02:40:00.0"
                    |},
                    |{
                    |   "eventType":"ClassScheduleSlotDeletedEvent",
                    |   "day":"SUNDAY",
                    |   "startTime":"09:00",
                    |   "endTime":"10:00",
                    |   "classId":"78b9007f-9e36-467d-894b-75d93cc63473",
                    |   "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
                    |   "occurredOn":"2020-07-20 02:41:00.0"
                    |},
                    |{
                    |   "eventType": "ClassScheduleSlotAddedEvent",
                    |   "day":"MONDAY",
                    |   "startTime":"10:00",
                    |   "endTime":"11:00",
                    |   "classId":"602c7ee0-3095-4231-835a-244d82b13674",
                    |   "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
                    |   "occurredOn":"2020-07-20 02:42:00.0"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquetClassScheduleDf = sinks.find(_.name === ParquetClassScheduleSink).get.output
          val redshiftClassScheduleDf = sinks.find(_.name === RedshiftClassScheduleSink)
            .get.output.sort("class_schedule_created_time")
          val deltaClassScheduleDf = sinks.find(_.name === DeltaClassScheduleSink)
            .get.output.sort("class_schedule_created_time")

          assertSmallDatasetEquality(spark, ClassScheduleEntity, parquetClassScheduleDf, expParquetJson)
          assertSmallDatasetEquality(ClassScheduleEntity, redshiftClassScheduleDf, expDf)
          assertSmallDatasetEquality(ClassScheduleEntity, deltaClassScheduleDf, expDf)
        }
      )
    }
  }

  def createDfFromJsonWithTimeCols(spark: SparkSession, json: String): DataFrame = {
    createDfFromJson(spark, json)
      .withColumn("class_schedule_created_time", col("class_schedule_created_time").cast(TimestampType))
      .withColumn("class_schedule_updated_time", col("class_schedule_updated_time").cast(TimestampType))
  }
}
