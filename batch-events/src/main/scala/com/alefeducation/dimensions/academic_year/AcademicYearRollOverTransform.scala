package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.academic_year.AcademicYearRollOverTransform._
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers.{AcademicYearEntity, AcademicYearRollOverCompletedCols}
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

class AcademicYearRollOverTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(): List[Option[Sink]] = {
    import com.alefeducation.util.BatchTransformerUtility._

    val academicYearRolledOverDF: Option[DataFrame] =
      service.readOptional(AcademicYearRolledOverSource, session, extraProps = List(("mergeSchema", "true")))

    val transformedAcademicYearRolledOver = academicYearRolledOverDF.map(
      _.withColumn("academic_year_is_roll_over_completed", lit(true))
        .transformForUpdateDim(AcademicYearRollOverCompletedCols, AcademicYearEntity, List("previousId", "schoolId"))
    )
    List(
      transformedAcademicYearRolledOver.map(DataSink(AcademicYearRolledOverTransformedSinkName, _))
    )
  }
}

object AcademicYearRollOverTransform {

  private val AcademicYearTransformService: String = "transform-academic-year-rollover-service"

  private val AcademicYearSources = getSource(AcademicYearTransformService)

  val AcademicYearRolledOverSource: String = AcademicYearSources(0)
  val AcademicYearRolledOverTransformedSinkName: String = getSink(AcademicYearTransformService)(0)

  val session: SparkSession = SparkSessionUtils.getSession(AcademicYearTransformService)
  val service = new SparkBatchService(AcademicYearTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new AcademicYearRollOverTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }
}
