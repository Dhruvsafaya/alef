package com.alefeducation.dimensions.course.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.course.transform.CourseDeletedTransform.CourseDeletedTransformedSink
import com.alefeducation.dimensions.course.transform.CourseMutatedTransform.CourseEntityPrefix
import com.alefeducation.util.Constants.CourseInactiveStatusVal
import com.alefeducation.util.SparkSessionUtils

class CourseDeletedDelta {
  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  val CourseDeletedDeltaService = "delta-course-deleted-service"

  val CourseDeletedDeltaSink: String = "delta-course-sink"

  private val session = SparkSessionUtils.getSession(CourseDeletedDeltaService)
  val service = new SparkBatchService(CourseDeletedDeltaService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val deletedSource = service.readOptional(CourseDeletedTransformedSink, session)
      deletedSource.flatMap(
        _.toIWH(uniqueIdColumns =  List("course_id"), inactiveStatus = CourseInactiveStatusVal)
          .map(_.toSink(CourseDeletedDeltaSink, CourseEntityPrefix))
      )
    }
  }
}
