package com.alefeducation.dimensions.school.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.school.transform.SchoolUpdateCombinedTransform.{SchoolDeletedTransformSink, SchoolStatusUpdatedTransformSink, SchoolUpdatedTransformSink}
import com.alefeducation.util.Helpers.{DeltaSchoolSink, SchoolEntity}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object SchoolUpdateCombinedDelta {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  val serviceName = "delta-school-update-combined"
  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  val sinkName: String = DeltaSchoolSink

  def main(args: Array[String]): Unit = {
    val sinkArgs: List[(String, Boolean)] = List(
      (SchoolUpdatedTransformSink, false),
      (SchoolStatusUpdatedTransformSink, false),
      (SchoolDeletedTransformSink, true),
    )

    val sinks: List[Option[Sink]] = sinkArgs.map { case (sourceName, isDelete) => getSink(sourceName, isDelete) }
    service.runAll(sinks.flatten)
  }

  private def getSink(sourceName: String, isDelete: Boolean): Option[Sink] = {
    val source: Option[DataFrame] = service
      .readOptional(sourceName, session)
      .map(_.drop("school_content_repository_dw_id", "school_organization_dw_id", "_is_complete"))

    if (isDelete)
      return source.flatMap(_.toDelete()).map(_.toSink(sinkName, SchoolEntity))

    source.flatMap(_.toUpdate()).map(_.toSink(sinkName, SchoolEntity))
  }

}
