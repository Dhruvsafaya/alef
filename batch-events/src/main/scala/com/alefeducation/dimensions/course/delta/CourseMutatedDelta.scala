package com.alefeducation.dimensions.course.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.course.transform.CourseMutatedTransform.{CourseEntityPrefix, CourseMutatedTransformedSink}
import com.alefeducation.util.Constants.CourseInactiveStatusVal
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, current_timestamp, lit, when}

object CourseMutatedDelta {
  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  private val CourseMutatedDeltaService = "delta-course-mutated-service"
  private val CourseDeltaSink: String = "delta-course-sink"

  private val session = SparkSessionUtils.getSession(CourseMutatedDeltaService)
  private val service = new SparkBatchService(CourseMutatedDeltaService, session)

  def main(args: Array[String]): Unit = {

    service.run {
      val updatedSource = service
        .readOptional(CourseMutatedTransformedSink, session)
        .map(
          _.withColumn(
            "course_dw_updated_time",
            when(col("course_status") === lit(CourseInactiveStatusVal), current_timestamp())
          )
        )

      updatedSource.map(
        _.toIWHContext(uniqueIdColumns = List("course_id"), inactiveStatus = CourseInactiveStatusVal)
          .toSink(CourseDeltaSink, CourseEntityPrefix)
      )
    }
  }
}
