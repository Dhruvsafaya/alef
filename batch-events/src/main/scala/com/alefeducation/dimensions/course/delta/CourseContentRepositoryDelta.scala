package com.alefeducation.dimensions.course.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.dimensions.course.transform.CourseContentRepositoryTransform.CourseContentRepositoryEntityPrefix
import com.alefeducation.util.Constants.CourseInactiveStatusVal
import com.alefeducation.util.SparkSessionUtils

object CourseContentRepositoryDelta {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  val CourseContentRepositoryDeltaService = "delta-course-content-repository"
  val CourseContentRepositoryTransformedSink = "parquet-course-content-repository-transformed-source"
  val CourseContentRepositoryDeltaSink: String = "delta-course-content-repository-sink"

  val CourseContentRepositoryDeltaMatchConditions: String =
    s"""
       |${Alias.Delta}.${CourseContentRepositoryEntityPrefix}_course_id = ${Alias.Events}.${CourseContentRepositoryEntityPrefix}_course_id
       | and ${Alias.Delta}.${CourseContentRepositoryEntityPrefix}_repository_id = ${Alias.Events}.${CourseContentRepositoryEntityPrefix}_repository_id
     """.stripMargin

  private val session = SparkSessionUtils.getSession(CourseContentRepositoryDeltaService)
  val service = new SparkBatchService(CourseContentRepositoryDeltaService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val source = service.readOptional(CourseContentRepositoryTransformedSink, session)
      source.flatMap(
        _.toIWH(
          matchConditions = CourseContentRepositoryDeltaMatchConditions,
          uniqueIdColumns = List("ccr_course_id", "ccr_repository_id"),
          inactiveStatus = CourseInactiveStatusVal)
          .map(_.toSink(CourseContentRepositoryDeltaSink, CourseContentRepositoryEntityPrefix))
      )
    }
  }

}

