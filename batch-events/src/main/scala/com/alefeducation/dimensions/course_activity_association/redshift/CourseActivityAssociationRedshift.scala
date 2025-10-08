package com.alefeducation.dimensions.course_activity_association.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.course_activity_association.transform.CourseActivityAssociationTransform.{
  CourseActivityAssociationEntity,
  CourseActivityAssociationInactiveStatus
}
import com.alefeducation.util.Constants.ActivityPlannedInCourseEvent
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class CourseActivityAssociationRedshift(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._

  def transform(sourceName: String, sinkName: String): Option[Sink] = {
    val source = service.readOptional(sourceName, session)

    source.map(
      _.drop(s"${CourseActivityAssociationEntity}_metadata")
        .toRedshiftIWHSink(
          sinkName = sinkName,
          entity = CourseActivityAssociationEntity,
          eventType = ActivityPlannedInCourseEvent,
          ids = List(
            s"${CourseActivityAssociationEntity}_activity_id",
            s"${CourseActivityAssociationEntity}_course_id"
          ),
          inactiveStatus = CourseActivityAssociationInactiveStatus,
          isStagingSink = true
        )
    )
  }

}

object CourseActivityAssociationRedshift {

  val CourseActivityAssociationRedshiftService = "redshift-course-activity-association"

  def main(args: Array[String]): Unit = {
    val session = SparkSessionUtils.getSession(CourseActivityAssociationRedshiftService)
    val service = new SparkBatchService(CourseActivityAssociationRedshiftService, session)

    val sourceName = getSource(CourseActivityAssociationRedshiftService).head
    val sinkName = getSink(CourseActivityAssociationRedshiftService).head

    val transformer = new CourseActivityAssociationRedshift(session, service)
    service.run(transformer.transform(sourceName, sinkName))
  }
}
