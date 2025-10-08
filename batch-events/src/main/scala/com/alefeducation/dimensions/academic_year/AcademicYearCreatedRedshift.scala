package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.academic_year.AcademicYearCreatedRedshift.{ColumnsToInsert, ColumnsToUpdate, TargetTableName, toMapCols}
import com.alefeducation.dimensions.academic_year.AcademicYearRollOverRedshift.targetTableName
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.getNestedString
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class AcademicYearCreatedRedshift(val session: SparkSession, val service: SparkBatchService) {

  val matchConditions: String = s"""
                                   |$targetTableName.academic_year_id = $TempTableAlias.academic_year_id
                                   | AND NVL($targetTableName.academic_year_school_id, '') = NVL($TempTableAlias.academic_year_school_id, '')
                                   |""".stripMargin


  def transform(createSourceName: String, sinkName: String): Option[Sink] = {
    val createdSource = service.readOptional(createSourceName, session)

    createdSource.map(_.toRedshiftUpsertSink(
      sinkName = sinkName,
      tableName = TargetTableName,
      matchConditions = matchConditions,
      columnsToInsert = toMapCols(ColumnsToInsert, TempTableAlias),
      columnsToUpdate = toMapCols(ColumnsToUpdate, TempTableAlias),
      isStaging = true
    ))
  }
}

object AcademicYearCreatedRedshift {

  val AcademicYearCreatedRedshiftService = "redshift-academic-year-created-service"

  val session = SparkSessionUtils.getSession(AcademicYearCreatedRedshiftService)
  val service = new SparkBatchService(AcademicYearCreatedRedshiftService, session)

  val TargetTableName = "rel_academic_year"

  val AcademicYearColumns: List[String] = List(
    "academic_year_updated_time",
    "academic_year_deleted_time",
    "academic_year_dw_updated_time",
    "academic_year_status",
    "academic_year_id",
    "academic_year_school_id",
    "academic_year_organization_code",
    "academic_year_start_date",
    "academic_year_end_date",
    "academic_year_created_by",
    "academic_year_updated_by",
    "academic_year_state",
    "academic_year_is_roll_over_completed",
    "academic_year_type"
  )

  val ColumnsToInsert: List[String] = "academic_year_delta_dw_id" :: "academic_year_created_time" ::
    "academic_year_dw_created_time" :: AcademicYearColumns

  val ColumnsToUpdate: List[String] = AcademicYearColumns

  def toMapCols(cols: List[String], tableAlias: String): Map[String, String] = cols.map(col => col -> s"$tableAlias.$col").toMap

  def main(args: Array[String]): Unit = {
    val redshift = new AcademicYearCreatedRedshift(session, service)
    val createSourceName = getNestedString(AcademicYearCreatedRedshiftService, "create-source")
    val sinkName = getNestedString(AcademicYearCreatedRedshiftService, "sink")

    service.run(redshift.transform(createSourceName, sinkName))
  }
}
