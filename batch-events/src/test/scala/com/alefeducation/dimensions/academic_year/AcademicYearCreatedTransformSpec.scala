package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.academic_year.AcademicYearCreatedTransform.{AcademicYearKey, AcademicYearCreatedService}
import com.alefeducation.util.Resources.getNestedString
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Date

class AcademicYearCreatedTransformSpec extends SparkSuite {

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
    "academic_year_delta_dw_id",
    "academic_year_created_by",
    "academic_year_updated_by",
    "academic_year_organization_code",
    "academic_year_state",
    "academic_year_is_roll_over_completed",
    "academic_year_type"
  )

  test("should create data frame") {

    val created =
      """
        |{
        |   "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
        |   "eventType":"AcademicYearCreatedEvent",
        |   "loadtime":"2024-03-29T05:30:26.590Z",
        |   "occurredOn":"2024-03-28 11:00:53.169",
        |   "uuid":"53ce86c7-45b9-4605-ac8e-22e9cc7cfaa6",
        |   "type":"SCHOOL",
        |   "status":"CURRENT",
        |   "startDate":1705795200000,
        |   "endDate":1736839107000,
        |   "schoolId":"4813ddff-483f-4e7d-819f-a2946d52547a",
        |   "organization":"MoE-AbuDhabi(Public)",
        |   "createdBy":"5baf1be9-6450-4688-b44c-94832bc70929",
        |   "updatedBy":"5baf1be9-6450-4688-b44c-94832bc70929",
        |   "createdOn":1711623653159,
        |   "updatedOn":1711623653159
        |}
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val createSourceName = getNestedString(AcademicYearCreatedService, "create-source")
    val createSinkName = getNestedString(AcademicYearCreatedService, "create-sink")

    when(service.getStartIdUpdateStatus(AcademicYearKey)).thenReturn(1001L)

    val createdDf = spark.read.json(Seq(created).toDS())
    when(service.readOptional(createSourceName, sprk)).thenReturn(Some(createdDf))

    val transform = new AcademicYearCreatedTransform(sprk, service)
    val sinks = transform.transform(createSourceName, createSinkName)

    val cdf = sinks.find(_.name == createSinkName).get.output

    assert(cdf.columns.toSet === ExpectedColumns)
    assert[Date](cdf, "academic_year_start_date", Date.valueOf("2024-01-21"))
    assert[Date](cdf, "academic_year_end_date", Date.valueOf("2025-01-14"))
    assert[String](cdf, "academic_year_created_time", "2024-03-28 11:00:53.169")
    assert[String](cdf, "academic_year_school_id", "4813ddff-483f-4e7d-819f-a2946d52547a")
    assert[String](cdf, "academic_year_id", "53ce86c7-45b9-4605-ac8e-22e9cc7cfaa6")
    assert[Int](cdf, "academic_year_status", 1)
    assert[Long](cdf, "academic_year_delta_dw_id", 1001)
    assert[String](cdf, "academic_year_state", "CURRENT")
    assert[String](cdf, "academic_year_organization_code", "MoE-AbuDhabi(Public)")
    assert[String](cdf, "academic_year_created_by", "5baf1be9-6450-4688-b44c-94832bc70929")
    assert[String](cdf, "academic_year_updated_by", "5baf1be9-6450-4688-b44c-94832bc70929")
    assert[Boolean](cdf, "academic_year_is_roll_over_completed", false)
    assert[String](cdf, "academic_year_type", "SCHOOL")
  }
}
