package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.academic_year.AcademicYearRollOverTransform._
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class AcademicYearDimensionSpec extends SparkSuite{

  val expectedColumns = Set(
    "academic_year_is_roll_over_completed",
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
    "academic_year_organization_code",
    "academic_year_created_by",
    "academic_year_updated_by",
    "academic_year_state",
    "academic_year_type",
  )


  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]


  test("academic year started") {

    val incoming_rolled_over =
      """
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "AcademicYearRollOverCompleted",
        |  "id":"ay2",
        |  "previousId":"ay1",
        |  "schoolId":"school-1",
        |  "occurredOn":"2023-12-20 10:50:21.000"
        |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val incomingRolledOverDF = spark.read.json(Seq(incoming_rolled_over).toDS())
    when(service.readOptional(AcademicYearRolledOverSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(incomingRolledOverDF))

    val transformer = new AcademicYearRollOverTransform(sprk, service)
    val sinks = transformer.transform()

    val AcademicYearRolledOverDF = sinks
      .flatten
      .find(_.name == AcademicYearRolledOverTransformedSinkName).get.output

    assert[Boolean](AcademicYearRolledOverDF, "academic_year_is_roll_over_completed",true)
  }
}
