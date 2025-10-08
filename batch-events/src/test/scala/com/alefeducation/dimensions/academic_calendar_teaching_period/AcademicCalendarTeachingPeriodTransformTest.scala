package com.alefeducation.dimensions.academic_calendar_teaching_period

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class AcademicCalendarTeachingPeriodTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val ExpectedColumns: Set[String] = Set(
    "actp_teaching_period_title",
    "actp_academic_calendar_id",
    "actp_dw_updated_time",
    "actp_teaching_period_is_current",
    "actp_teaching_period_id",
    "actp_teaching_period_start_date",
    "actp_teaching_period_end_date",
    "actp_dw_created_time",
    "actp_updated_time",
    "actp_created_time",
    "actp_dw_id",
    "actp_teaching_period_created_by_id",
    "actp_teaching_period_updated_by_id",
    "actp_status",
    "actp_teaching_period_order"
  )

  test("Should transform created event") {

    val created_value = """
                          |{
                          |  "eventType": "AcademicCalendarCreatedEvent",
                          |  "tenantId": "tenant-id",
                          |  "uuid": "e75e917c-c5bf-459c-a7b8-180cc4385054",
                          |  "occurredOn": "1970-07-14 02:40:00.0",
                          |  "title": "title",
                          |  "default": "school-id",
                          |  "type": "1970-07-14",
                          |  "academicYearId": "e75e917c-c5bf-459c-a7b8-180cc43850c4",
                          |   "schoolId":"school-id",
                          |   "organization": "organizaion-id",
                          |   "createdBy": "e75e917c-c5bf-459c-a7b8-908cc43850c4",
                          |   "updatedBy": "e75e917c-c5bf-459c-a7b8-oiucc43850c4",
                          |   "createdOn": "1970-07-14 02:10:00.0",
                          |   "updatedOn": "1970-07-14 02:40:00.1",
                          |   "eventDateDw": "12/12/2024",
                          |   "teachingPeriods": [
                          |     {"uuid": "5y5e917c-c5bf-459c-a7b8-180cc4385054",
                          |     "title": "title",
                          |     "startDate": 1705795200000,
                          |     "endDate": 1736839107000,
                          |     "current": true,
                          |     "createdBy": "e75e917c-c5bf-459c-a7b8-908cc43850c4",
                          |     "updatedBy": "e75e917c-c5bf-459c-a7b8-oiucc43850c4"}
                          |   ]
                          |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-academic-calendar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))

    val transformer = new AcademicCalendarTeachingPeriodTransform(sprk, service, "academic-calendar-teaching-period-created-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === ExpectedColumns)
    assert(df.count === 1)
  }

  test("Should transform updated event") {

    val created_value = """
                          |{
                          |  "eventType": "AcademicCalendarUpdatedEvent",
                          |  "tenantId": "tenant-id",
                          |  "uuid": "e75e917c-c5bf-459c-a7b8-180cc4385054",
                          |  "occurredOn": "1970-07-14 02:40:00.0",
                          |  "title": "title",
                          |  "default": "school-id",
                          |  "type": "1970-07-14",
                          |  "academicYearId": "e75e917c-c5bf-459c-a7b8-180cc43850c4",
                          |   "schoolId":"school-id",
                          |   "organization": "organizaion-id",
                          |   "createdBy": "e75e917c-c5bf-459c-a7b8-908cc43850c4",
                          |   "updatedBy": "e75e917c-c5bf-459c-a7b8-oiucc43850c4",
                          |   "createdOn": "1970-07-14 02:10:00.0",
                          |   "updatedOn": "1970-07-14 02:40:00.1",
                          |   "eventDateDw": "12/12/2024",
                          |   "teachingPeriods": [
                          |     {"uuid": "5y5e917c-c5bf-459c-a7b8-180cc4385054",
                          |     "title": "title",
                          |     "startDate": 1705795200000,
                          |     "endDate": 1736839107000,
                          |     "current": true,
                          |     "createdBy": "e75e917c-c5bf-459c-a7b8-908cc43850c4",
                          |     "updatedBy": "e75e917c-c5bf-459c-a7b8-oiucc43850c4"}
                          |   ]
                          |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-academic-calendar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))

    val transformer = new AcademicCalendarTeachingPeriodTransform(sprk, service, "academic-calendar-teaching-period-updated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === ExpectedColumns)
    assert(df.count === 1)
  }

}
