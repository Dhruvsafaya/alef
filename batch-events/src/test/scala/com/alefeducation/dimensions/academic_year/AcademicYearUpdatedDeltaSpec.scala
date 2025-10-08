package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.DeltaUpdateSink
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.academic_year.AcademicYearUpdatedDelta.AcademicYearUpdatedDeltaService
import com.alefeducation.util.Resources.getNestedString
import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DateType
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Date

class AcademicYearUpdatedDeltaSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should update delta data frame") {

    val data =
      """
        |{
        |   "academic_year_start_date": "2024-01-21",
        |   "academic_year_end_date": "2025-01-14",
        |   "academic_year_created_time": "2024-03-28 11:00:53.169",
        |   "academic_year_school_id": "4813ddff-483f-4e7d-819f-a2946d52547a",
        |   "academic_year_id": "53ce86c7-45b9-4605-ac8e-22e9cc7cfaa6",
        |   "academic_year_status": 1,
        |   "academic_year_delta_dw_id": 1001,
        |   "academic_year_state": "CURRENT",
        |   "academic_year_organization_code": "MoE-AbuDhabi(Public)",
        |   "academic_year_created_by": "5baf1be9-6450-4688-b44c-94832bc70929",
        |   "academic_year_updated_by": "5baf1be9-6450-4688-b44c-94832bc70929",
        |   "academic_year_is_roll_over_completed": true,
        |   "academic_year_type": "SCHOOL"
        |}
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val updateSourceName = getNestedString(AcademicYearUpdatedDeltaService, "update-source")
    val sinkName = getNestedString(AcademicYearUpdatedDeltaService, "sink")

    val dataDf = spark.read.json(Seq(data).toDS())
      .withColumn("academic_year_start_date", col("academic_year_start_date").cast(DateType))
      .withColumn("academic_year_end_date", col("academic_year_end_date").cast(DateType))
    when(service.readOptional(updateSourceName, sprk)).thenReturn(Some(dataDf))

    val delta = new AcademicYearUpdatedDelta(sprk, service)

    val sinks = delta.transform(updateSourceName, sinkName)
    val sink = sinks.head

    val upsertSink = sink.asInstanceOf[DeltaUpdateSink]

    assert(replaceSpecChars(upsertSink.matchConditions) === replaceSpecChars(
         """|delta.academic_year_id = events.academic_year_id AND
            |delta.academic_year_school_id <=> events.academic_year_school_id and
            |delta.academic_year_created_time < events.academic_year_created_time
            | """.stripMargin))

    assert(upsertSink.updateFields === Map(
      "academic_year_end_date" -> "events.academic_year_end_date",
      "academic_year_updated_by" -> "events.academic_year_updated_by",
      "academic_year_id" -> "events.academic_year_id",
      "academic_year_organization_code" -> "events.academic_year_organization_code",
      "academic_year_created_by" -> "events.academic_year_created_by",
      "academic_year_dw_updated_time" -> "events.academic_year_dw_created_time",
      "academic_year_state" -> "events.academic_year_state",
      "academic_year_school_id" -> "events.academic_year_school_id",
      "academic_year_status" -> "events.academic_year_status",
      "academic_year_deleted_time" -> "events.academic_year_deleted_time",
      "academic_year_start_date" -> "events.academic_year_start_date",
      "academic_year_updated_time" -> "events.academic_year_created_time",
      "academic_year_is_roll_over_completed" -> "events.academic_year_is_roll_over_completed",
      "academic_year_type" -> "events.academic_year_type"
    ))

    val df = sink.output
    assert[Date](df, "academic_year_start_date", Date.valueOf("2024-01-21"))
    assert[Date](df, "academic_year_end_date", Date.valueOf("2025-01-14"))
    assert[String](df, "academic_year_created_time", "2024-03-28 11:00:53.169")
    assert[String](df, "academic_year_school_id", "4813ddff-483f-4e7d-819f-a2946d52547a")
    assert[String](df, "academic_year_id", "53ce86c7-45b9-4605-ac8e-22e9cc7cfaa6")
    assert[Int](df, "academic_year_status", 1)
    assert[Long](df, "academic_year_delta_dw_id", 1001)
    assert[String](df, "academic_year_state", "CURRENT")
    assert[String](df, "academic_year_organization_code", "MoE-AbuDhabi(Public)")
    assert[String](df, "academic_year_created_by", "5baf1be9-6450-4688-b44c-94832bc70929")
    assert[String](df, "academic_year_updated_by", "5baf1be9-6450-4688-b44c-94832bc70929")
    assert[Boolean](df, "academic_year_is_roll_over_completed", true)
    assert[String](df, "academic_year_type", "SCHOOL")
  }

}
