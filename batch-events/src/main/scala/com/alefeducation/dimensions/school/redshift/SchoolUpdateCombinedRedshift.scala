package com.alefeducation.dimensions.school.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.school.transform.SchoolUpdateCombinedTransform.{SchoolDeletedTransformSink, SchoolStatusUpdatedTransformSink, SchoolUpdatedTransformSink}
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.{AdminSchoolDeleted, AdminSchoolUpdated, SchoolStatusToggle}
import com.alefeducation.util.Helpers.{RedshiftSchoolSink, SchoolEntity}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

object SchoolUpdateCombinedRedshift {
  val serviceName = "redshift-school-update-combined"
  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  val sinkName: String = RedshiftSchoolSink

  def main(args: Array[String]): Unit = {
    val sinkArgs: List[(String, String)] = List(
      (SchoolUpdatedTransformSink, AdminSchoolUpdated),
      (SchoolStatusUpdatedTransformSink, SchoolStatusToggle),
      (SchoolDeletedTransformSink, AdminSchoolDeleted),
    )

    val sinks: List[Option[Sink]] = sinkArgs.map { case (sourceName, eventType) => getSink(sourceName, eventType) }
    service.runAll(sinks.flatten)
  }

  private def getSink(sourceName: String, eventType: String): Option[Sink] = {
    service
      .readOptional(sourceName, session)
      .map(
        _.toRedshiftUpdateSink(sinkName, eventType, SchoolEntity, ids = List("school_id"))
      )
  }

}
