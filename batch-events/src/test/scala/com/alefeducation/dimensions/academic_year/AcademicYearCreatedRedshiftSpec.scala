package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.academic_year.AcademicYearCreatedRedshift.AcademicYearCreatedRedshiftService
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.getNestedString
import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DateType
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Date

class AcademicYearCreatedRedshiftSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should create redshift data frame") {

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
        |   "academic_year_is_roll_over_completed": false,
        |   "academic_year_type": "SCHOOL"
        |}
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val createSourceName = getNestedString(AcademicYearCreatedRedshiftService, "create-source")
    val sinkName = getNestedString(AcademicYearCreatedRedshiftService, "sink")

    val dataDf = spark.read.json(Seq(data).toDS())
      .withColumn("academic_year_start_date", col("academic_year_start_date").cast(DateType))
      .withColumn("academic_year_end_date", col("academic_year_end_date").cast(DateType))
    when(service.readOptional(createSourceName, sprk)).thenReturn(Some(dataDf))

    val redshift = new AcademicYearCreatedRedshift(spark, service)

    val sinks = redshift.transform(createSourceName, sinkName)
    val sink = sinks.head

    val options = sink.asInstanceOf[DataSink].options

    assert(options.get("dbtable") === Some("rs_stage_schema.staging_rel_academic_year"))

    assert(options.get("postactions").map(replaceSpecChars) === Some(replaceSpecChars(
      """BEGIN TRANSACTION;
        |MERGE INTO rs_stage_schema.rel_academic_year
        |USING rs_stage_schema.staging_rel_academic_year AS temp_table
        |ON ( rel_academic_year.academic_year_id = temp_table.academic_year_id AND
        |    NVL(rel_academic_year.academic_year_school_id, '') = NVL(temp_table.academic_year_school_id, '') )
        |WHEN MATCHED THEN
        |    UPDATE
        |    SET academic_year_is_roll_over_completed = temp_table.academic_year_is_roll_over_completed,
        |        academic_year_end_date               = temp_table.academic_year_end_date,
        |        academic_year_type                   = temp_table.academic_year_type,
        |        academic_year_updated_by             = temp_table.academic_year_updated_by,
        |        academic_year_id                     = temp_table.academic_year_id,
        |        academic_year_organization_code      = temp_table.academic_year_organization_code,
        |        academic_year_created_by             = temp_table.academic_year_created_by,
        |        academic_year_dw_updated_time        = temp_table.academic_year_dw_updated_time,
        |        academic_year_state                  = temp_table.academic_year_state,
        |        academic_year_school_id              = temp_table.academic_year_school_id,
        |        academic_year_status                 = temp_table.academic_year_status,
        |        academic_year_deleted_time           = temp_table.academic_year_deleted_time,
        |        academic_year_start_date             = temp_table.academic_year_start_date,
        |        academic_year_updated_time           = temp_table.academic_year_updated_time
        |WHEN NOT MATCHED THEN
        |    INSERT (academic_year_is_roll_over_completed, academic_year_end_date, academic_year_type, academic_year_updated_by, academic_year_id,
        |            academic_year_organization_code, academic_year_created_by, academic_year_dw_updated_time,
        |            academic_year_state, academic_year_school_id, academic_year_status, academic_year_delta_dw_id,
        |            academic_year_deleted_time, academic_year_start_date, academic_year_dw_created_time,
        |            academic_year_updated_time, academic_year_created_time)
        |    VALUES (temp_table.academic_year_is_roll_over_completed, temp_table.academic_year_end_date, temp_table.academic_year_type,
        |            temp_table.academic_year_updated_by, temp_table.academic_year_id,
        |            temp_table.academic_year_organization_code, temp_table.academic_year_created_by,
        |            temp_table.academic_year_dw_updated_time, temp_table.academic_year_state,
        |            temp_table.academic_year_school_id, temp_table.academic_year_status, temp_table.academic_year_delta_dw_id,
        |            temp_table.academic_year_deleted_time, temp_table.academic_year_start_date, temp_table.academic_year_dw_created_time,
        |            temp_table.academic_year_updated_time, temp_table.academic_year_created_time);
        |DROP TABLE rs_stage_schema.staging_rel_academic_year;
        |END TRANSACTION;
        |""".stripMargin)))


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
    assert[Boolean](df, "academic_year_is_roll_over_completed", false)
    assert[String](df, "academic_year_type", "SCHOOL")
  }
}
