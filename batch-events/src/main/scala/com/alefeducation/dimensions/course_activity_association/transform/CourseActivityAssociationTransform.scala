package com.alefeducation.dimensions.course_activity_association.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.course_activity_association.transform.CourseActivityAssociationTransform.{
  ActivityAssociationKey,
  CourseActivityAssociationEntity
}
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.getSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class CourseActivityAssociationTransform(val session: SparkSession, val service: SparkBatchService) {
  import com.alefeducation.util.BatchTransformerUtility._

  def transform(activitySources: List[String], interimCheckpointSources: List[String], sinkName: String): Option[Sink] = {
    val activity: Option[DataFrame] = new ActivityTransform(session, service).transform(activitySources)
    val interimCheckpoint: Option[DataFrame] = new InterimCheckpointTransform(session, service).transform(interimCheckpointSources)

    val startId = service.getStartIdUpdateStatus(ActivityAssociationKey)
    val associations = activity
      .unionOptionalByName(interimCheckpoint)
      .map(
        _.genDwId(s"${CourseActivityAssociationEntity}_dw_id", startId, orderByField=s"${CourseActivityAssociationEntity}_created_time")
      )

    associations.map(DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> ActivityAssociationKey)))
  }

}

object CourseActivityAssociationTransform {
  private val ActivityAssociationKey = "dim_course_activity_association"

  val CourseActivityAssociationEntity = "caa"
  val ActivityType = 1
  val InterimCheckpointType = 2
  val ActivityPlannedStatus = 1
  val ActivityUnplannedStatus = 4
  val CourseActivityAssociationInactiveStatus = 2

  val CourseActivityAssociationTransformService = "transform-course-activity-association"
  private val activitySources = List(
    "parquet-published-activity-planned-in-courses-source",
    "parquet-published-activity-unplanned-in-courses-source",
    "parquet-published-activity-updated-in-courses-source",
  )
  private val interimCheckpointSources = List(
    "parquet-published-interim-checkpoint-in-courses-created-source",
    "parquet-published-interim-checkpoint-in-courses-updated-source",
    "parquet-published-interim-checkpoint-in-courses-deleted-source"
  )

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(CourseActivityAssociationTransformService)
    val service = new SparkBatchService(CourseActivityAssociationTransformService, session)

    val sinkName = getSink(CourseActivityAssociationTransformService).head

    val transform = new CourseActivityAssociationTransform(session, service)
    service.run(transform.transform(activitySources, interimCheckpointSources, sinkName))
  }
}
