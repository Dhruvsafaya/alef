package com.alefeducation.dimensions.course.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.course.transform.CourseMutatedTransform.{CourseEntityPrefix, CourseMutatedTransformedSink}
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.CourseInactiveStatusVal
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, current_timestamp, lit, when}

object CourseMutatedRedshift {
  private val CourseMutatedService = "redshift-course-mutated-service"
  private val CourseRedshiftSink: String = "redshift-course-sink"

  private val session = SparkSessionUtils.getSession(CourseMutatedService)
  private val service = new SparkBatchService(CourseMutatedService, session)

  def main(args: Array[String]): Unit = {

    service.run {
      val mutatedSource = service
        .readOptional(CourseMutatedTransformedSink, session)
        .map(
          _.withColumn(
            "course_dw_updated_time",
            when(col("course_status") === lit(CourseInactiveStatusVal), current_timestamp())
          )
        )
      mutatedSource.map(
        //Drop delta-specific columns
        _.drop("course_description", "course_goal")
          .toRedshiftIWHSink(
            CourseRedshiftSink,
            CourseEntityPrefix,
            ids = List("course_id"),
            isStagingSink = true,
            inactiveStatus = CourseInactiveStatusVal
          )
      )
    }
  }
}
