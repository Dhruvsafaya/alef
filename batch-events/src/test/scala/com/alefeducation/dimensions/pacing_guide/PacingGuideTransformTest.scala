package com.alefeducation.dimensions.pacing_guide

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.dimensions.pacing_guide.PacingGuideTransform.PacingGuideTransformedSink
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class PacingGuideTransformTest extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("Should transform created pacing guide event") {
    val expectedCols = List("pacing_period_start_date", "pacing_period_label", "pacing_activity_id", "pacing_course_id", "pacing_interval_type", "pacing_updated_time", "pacing_status", "pacing_interval_id", "pacing_academic_calendar_id", "pacing_academic_year_id", "pacing_period_id", "pacing_ip_id", "pacing_activity_order", "pacing_period_end_date", "pacing_id", "pacing_interval_start_date", "pacing_interval_label", "pacing_tenant_id", "pacing_class_id", "pacing_dw_updated_time", "pacing_interval_end_date", "pacing_created_time", "pacing_dw_created_time", "pacing_dw_id")
    val created_value = """
                          |{
                          | "eventType": "PacingGuideCreatedEvent",
                          | "eventDateDw":"12/12/2024",
                          |	"id": "8965370d-0b6a-4a36-8f41-95ff2868af5e",
                          |	"classId": "effa3e2a-cfdd-4982-b365-c2e61b6c8675",
                          |	"courseId": "edb3216b-c96d-44ed-b663-7a8f1213616f",
                          |	"instructionalPlanId": "aab3216b-c96d-44ed-b663-7a8f1213616f",
                          |	"academicCalendarId": "bab3216b-c96d-44ed-b663-7a8f1213616f",
                          | "tenantId": "qeb3216b-c96d-44ed-b663-7a8f1213625r",
                          |	"academicYearId": "bab3216b-c96d-44ed-b663-7a8f1213616f",
                          |	"activities": [
                          |		{
                          |			"activity": {
                          |				"order": 1,
                          |				"id": "c3464b3f-8132-4b28-a37f-000000028858"
                          |			},
                          |			"associations": [
                          |				{
                          |					"id": "7809cbf2-1c39-497a-a468-2c2fd12a91c4",
                          |					"startDate": "2023-01-01",
                          |					"endDate": "2023-01-15",
                          |					"type": "TEACHING_PERIOD",
                          |					"label": "Teaching Period"
                          |				},
                          |				{
                          |					"id": "7809cbf2-1c39-497a-a468-2c2fd12a91c4",
                          |					"startDate": "2023-01-01",
                          |					"endDate": "2023-01-15",
                          |					"type": "WEEK",
                          |					"label": "Week"
                          |				},
                          |				{
                          |					"id": "cbb01725-2407-44ff-8fac-d337573845bf",
                          |					"type": "UNIT",
                          |					"label": "Unit"
                          |				}
                          |			]
                          |		}
                          |	],
                          |	"occurredOn": 1708929191264
                          |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-pacing-guide-created-source", sprk)).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-pacing-guide-updated-source", sprk)).thenReturn(None)
    when(service.readOptional("parquet-pacing-guide-deleted-source", sprk)).thenReturn(None)

    val transformer = new PacingGuideTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.filter(_.get.name == PacingGuideTransformedSink).head.get.output

    assert(df.count === 1)
    assert(df.columns.toList === expectedCols)
  }
}