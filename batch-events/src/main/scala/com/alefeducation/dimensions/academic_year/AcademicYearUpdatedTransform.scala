package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.academic_year.AcademicYearCreatedTransform.applyTransformations
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.getNestedString
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class AcademicYearUpdatedTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(updateSourceName: String, updateSinkName: String): Option[Sink] = {
    val updateSource = service.readOptional(updateSourceName, session)

    val updateTransformed = updateSource.map(_.transform(applyTransformations))

    updateTransformed.map(DataSink(updateSinkName, _))
  }

}

object AcademicYearUpdatedTransform {

  val AcademicYearUpdatedService = "transform-academic-year-updated-service"

  val session: SparkSession = SparkSessionUtils.getSession(AcademicYearUpdatedService)
  val service = new SparkBatchService(AcademicYearUpdatedService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new AcademicYearUpdatedTransform(session, service)
    val updateSourceName = getNestedString(AcademicYearUpdatedService, "update-source")
    val updateSinkName = getNestedString(AcademicYearUpdatedService, "update-sink")
    service.run(transformer.transform(updateSourceName, updateSinkName))
  }
}

