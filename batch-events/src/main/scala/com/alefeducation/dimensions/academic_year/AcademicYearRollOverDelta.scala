package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
import com.alefeducation.dimensions.academic_year.AcademicYearRollOverRedshift.transformedAcademicYearRolledOver
import com.alefeducation.util.Helpers.AcademicYearEntity
import com.alefeducation.util.Resources.getSink
import com.alefeducation.util.SparkSessionUtils

object AcademicYearRollOverDelta {

  private val academicYearDeltaService = "delta-academic-year-rollover-service"
  val targetTableName = "dim_academic_year"

  private val session = SparkSessionUtils.getSession(academicYearDeltaService)
  val service = new SparkBatchService(academicYearDeltaService, session)

  private val matchConditions: String = s"""
    |${Alias.Delta}.academic_year_id = ${Alias.Events}.academic_year_id
    | AND ${Alias.Delta}.academic_year_school_id = ${Alias.Events}.academic_year_school_id
    |""".stripMargin

  def main(args: Array[String]): Unit = {
    service.run {
      val AcademicYearRolledOverTransformed = service.readOptional(transformedAcademicYearRolledOver, session)

      AcademicYearRolledOverTransformed.flatMap(
        _.toUpdate(matchConditions = matchConditions).map(_.toSink(getSink(academicYearDeltaService).head, AcademicYearEntity))
      )
    }
  }
}
