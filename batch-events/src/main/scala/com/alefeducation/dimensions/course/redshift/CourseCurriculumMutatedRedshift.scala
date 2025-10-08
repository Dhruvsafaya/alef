package com.alefeducation.dimensions.course.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.course.transform.CourseCurriculumMutatedTransform.CourseCurriculumEntityPrefix
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.CourseInactiveStatusVal
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, current_timestamp, lit, when}

object CourseCurriculumMutatedRedshift {
  private val CourseCurriculumMutatedService = "redshift-course-curriculum-updated"
  private val CourseCurriculumMutatedTransformedSource = "parquet-course-curriculum-mutated-transformed-source"
  private val CourseCurriculumRedshiftSink: String = "redshift-course-curriculum-sink"

  private val session = SparkSessionUtils.getSession(CourseCurriculumMutatedService)
  val service = new SparkBatchService(CourseCurriculumMutatedService, session)

  def main(args: Array[String]): Unit = {

    service.run {
      val updatedSource = service
        .readOptional(CourseCurriculumMutatedTransformedSource, session)
        .map(
          _.withColumn("cc_dw_updated_time", when(col("cc_status") === lit(CourseInactiveStatusVal), current_timestamp()))
        )
      updatedSource.map(
        _.toRedshiftIWHSink(
          CourseCurriculumRedshiftSink,
          CourseCurriculumEntityPrefix,
          ids = List("cc_course_id"),
          isStagingSink = true,
          inactiveStatus = CourseInactiveStatusVal
        )
      )
    }
  }
}
