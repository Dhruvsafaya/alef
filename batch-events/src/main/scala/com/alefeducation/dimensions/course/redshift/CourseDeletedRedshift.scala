package com.alefeducation.dimensions.course.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.course.transform.CourseDeletedTransform.CourseDeletedTransformedSink
import com.alefeducation.dimensions.course.transform.CourseMutatedTransform.CourseEntityPrefix
import com.alefeducation.util.Constants.CourseInactiveStatusVal
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.BatchTransformerUtility._


object CourseDeletedRedshift {
  val CourseDeletedService = "redshift-course-deleted-service"

  val CourseDeletedTransformedSource: String = CourseDeletedTransformedSink
  val CourseDeletedRedshiftSink: String = "redshift-course-sink"

  private val session = SparkSessionUtils.getSession(CourseDeletedService)
  val service = new SparkBatchService(CourseDeletedService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val courseDeletedSource = service.readOptional(CourseDeletedTransformedSource, session)
      courseDeletedSource.map(
        //Drop delta-specific columns
        _.drop("course_description", "course_goal")
          .toRedshiftIWHSink(
            CourseDeletedRedshiftSink,
            CourseEntityPrefix,
            ids = List("course_id"),
            isStagingSink = true,
            inactiveStatus = CourseInactiveStatusVal
          )
      )
    }
  }
}
