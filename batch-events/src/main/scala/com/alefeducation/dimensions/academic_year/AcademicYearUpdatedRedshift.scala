package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.AcademicYearUpdatedEvent
import com.alefeducation.util.Helpers.AcademicYearEntity
import com.alefeducation.util.Resources.getNestedString
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class AcademicYearUpdatedRedshift(val session: SparkSession, val service: SparkBatchService) {

  def transform(updateSourceName: String, sinkName: String): Option[Sink] = {
    val updatedSource = service.readOptional(updateSourceName, session)

    updatedSource.map(_.toRedshiftUpdateSink(
      sinkName = sinkName,
      eventType = AcademicYearUpdatedEvent,
      entity = AcademicYearEntity,
      ids = List("academic_year_id"),
      isStaging = true
    ))
  }
}

object AcademicYearUpdatedRedshift {

  val AcademicYearUpdatedRedshiftService = "redshift-academic-year-updated-service"

  val session = SparkSessionUtils.getSession(AcademicYearUpdatedRedshiftService)
  val service = new SparkBatchService(AcademicYearUpdatedRedshiftService, session)

  def main(args: Array[String]): Unit = {
    val redshift = new AcademicYearUpdatedRedshift(session, service)
    val updateSourceName = getNestedString(AcademicYearUpdatedRedshiftService, "update-source")
    val sinkName = getNestedString(AcademicYearUpdatedRedshiftService, "sink")

    service.run(redshift.transform(updateSourceName, sinkName))
  }
}
