package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.academic_year.AcademicYearUpdatedTransform.AcademicYearUpdatedService
import com.alefeducation.util.Resources.getNestedString
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Date

class AcademicYearUpdatedTransformSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val ExpectedColumns: Set[String] = Set(
    "academic_year_end_date",
    "academic_year_id",
    "academic_year_dw_updated_time",
    "academic_year_school_id",
    "academic_year_status",
    "academic_year_deleted_time",
    "academic_year_start_date",
    "academic_year_dw_created_time",
    "academic_year_updated_time",
    "academic_year_created_time",
    "academic_year_created_by",
    "academic_year_updated_by",
    "academic_year_organization_code",
    "academic_year_state",
    "academic_year_is_roll_over_completed",
    "academic_year_type"
  )

  test("should create data frame for update academic year") {
    val update =
      """
        |{
        |   "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
        |   "eventType":"AcademicYearUpdatedEvent",
        |   "loadtime":"2024-03-29T05:30:26.574Z",
        |   "occurredOn":"2024-03-27 11:15:07.457",
        |   "uuid":"8ba59c3d-214f-44ad-a5dd-c2a11cabeb7a",
        |   "type":"SCHOOL",
        |   "status":"CONCLUDED",
        |   "startDate":1705795200000,
        |   "endDate":1736839107000,
        |   "schoolId":"7cc554fd-56e3-48b2-bd0d-c97a1c951e4e",
        |   "organization":"MoE-AbuDhabi(Public)",
        |   "createdBy":"4e17e74a-8532-4ae3-aa90-93e7e62f9a3d",
        |   "updatedBy":"4e17e74a-8532-4ae3-aa90-93e7e62f9a3d",
        |   "createdOn":1709638094000,
        |   "updatedOn":1711103441000,
        |   "eventDateDw":"20240327"
        |}
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val updateSourceName = getNestedString(AcademicYearUpdatedService, "update-source")
    val updateSinkName = getNestedString(AcademicYearUpdatedService, "update-sink")

    val updatedDf = spark.read.json(Seq(update).toDS())
    when(service.readOptional(updateSourceName, sprk)).thenReturn(Some(updatedDf))

    val transform = new AcademicYearUpdatedTransform(sprk, service)
    val sinks = transform.transform(updateSourceName, updateSinkName)

    val udf = sinks.find(_.name == updateSinkName).get.output

    assert(udf.columns.toSet === ExpectedColumns)
    assert[Date](udf, "academic_year_start_date", Date.valueOf("2024-01-21"))
    assert[Date](udf, "academic_year_end_date", Date.valueOf("2025-01-14"))
    assert[String](udf, "academic_year_created_time", "2024-03-27 11:15:07.457")
    assert[String](udf, "academic_year_school_id", "7cc554fd-56e3-48b2-bd0d-c97a1c951e4e")
    assert[String](udf, "academic_year_id", "8ba59c3d-214f-44ad-a5dd-c2a11cabeb7a")
    assert[Int](udf, "academic_year_status", 1)
    assert[String](udf, "academic_year_state", "CONCLUDED")
    assert[String](udf, "academic_year_organization_code", "MoE-AbuDhabi(Public)")
    assert[String](udf, "academic_year_created_by", "4e17e74a-8532-4ae3-aa90-93e7e62f9a3d")
    assert[String](udf, "academic_year_updated_by", "4e17e74a-8532-4ae3-aa90-93e7e62f9a3d")
    assert[Boolean](udf, "academic_year_is_roll_over_completed", true)
    assert[String](udf, "academic_year_type", "SCHOOL")
  }
}
