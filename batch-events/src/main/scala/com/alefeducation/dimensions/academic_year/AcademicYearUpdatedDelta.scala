package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
import com.alefeducation.dimensions.academic_year.AcademicYearCreatedDelta.MatchConditions
import com.alefeducation.dimensions.academic_year.AcademicYearCreatedRedshift.ColumnsToUpdate
import com.alefeducation.util.Helpers.AcademicYearEntity
import com.alefeducation.util.Resources.getNestedString
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class AcademicYearUpdatedDelta(val session: SparkSession, val service: SparkBatchService) {

  def transform(updateSourceName: String, sinkName: String): Option[Sink] = {
    val updatedSource = service.readOptional(updateSourceName, session)

    updatedSource.flatMap(_.toUpdate(
      matchConditions = MatchConditions,
      updateColumns = ColumnsToUpdate
    )).map(_.toSink(sinkName, AcademicYearEntity))
  }
}

object AcademicYearUpdatedDelta {

  val AcademicYearUpdatedDeltaService = "delta-academic-year-updated-service"

  private val session = SparkSessionUtils.getSession(AcademicYearUpdatedDeltaService)
  val service = new SparkBatchService(AcademicYearUpdatedDeltaService, session)


  def main(args: Array[String]): Unit = {
    val delta = new AcademicYearUpdatedDelta(session, service)
    val updateSourceName = getNestedString(AcademicYearUpdatedDeltaService, "update-source")
    val sinkName = getNestedString(AcademicYearUpdatedDeltaService, "sink")

    service.run(delta.transform(updateSourceName, sinkName))
  }
}
