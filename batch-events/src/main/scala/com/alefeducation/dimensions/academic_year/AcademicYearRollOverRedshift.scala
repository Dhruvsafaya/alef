package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers.AcademicYearEntity
import com.alefeducation.util.Resources.getSink
import com.alefeducation.util.SparkSessionUtils

object AcademicYearRollOverRedshift {

  private val academicYearRedshiftService = "redshift-academic-year-rollover-service"
  val transformedAcademicYearRolledOver = "transformed-academic-year-rollover"

  val targetTableName = "rel_academic_year"

  private val session = SparkSessionUtils.getSession(academicYearRedshiftService)
  val service = new SparkBatchService(academicYearRedshiftService, session)

  val matchConditionsStartedUpdated: String = s"""
    |$targetTableName.academic_year_id = $TempTableAlias.academic_year_id
    | AND $targetTableName.academic_year_school_id = $TempTableAlias.academic_year_school_id
    |""".stripMargin

  def main(args: Array[String]): Unit = {
    service.run {
      val AcademicYearRolledOverTransformed = service.readOptional(transformedAcademicYearRolledOver, session)

      AcademicYearRolledOverTransformed.map(_.toRedshiftUpdateSink(
          getSink(academicYearRedshiftService).head,
          "AcademicYearRollOverCompleted",
          AcademicYearEntity,
          List("academic_year_id","academic_year_school_id"),
          isStaging = true
        )
      )
    }
  }

}
