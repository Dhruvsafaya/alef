package com.alefeducation.dimensions.course.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.course.transform.CourseContentRepositoryTransform.CourseContentRepositoryEntityPrefix
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.CourseInactiveStatusVal
import com.alefeducation.util.SparkSessionUtils

object CourseContentRepositoryRedshift {
  val CourseContentRepositoryService = "redshift-course-content-repository"
  val CourseContentRepositoryTransformedSource = "parquet-course-content-repository-transformed-source"
  val CourseContentRepositoryRedshiftSink: String = "redshift-course-content-repository-sink"

  private val session = SparkSessionUtils.getSession(CourseContentRepositoryService)
  val service = new SparkBatchService(CourseContentRepositoryService, session)

  def main(args: Array[String]): Unit = {

    service.run {
      val updatedSource = service.readOptional(CourseContentRepositoryTransformedSource, session)
      updatedSource.map(
        _.toRedshiftIWHSink(
          CourseContentRepositoryRedshiftSink,
          CourseContentRepositoryEntityPrefix,
          ids = List("ccr_course_id", "ccr_repository_id"),
          isStagingSink = true,
          inactiveStatus = CourseInactiveStatusVal
        )
      )
    }
  }
}

