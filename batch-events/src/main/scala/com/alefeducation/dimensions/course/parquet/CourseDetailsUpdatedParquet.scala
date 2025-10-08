package com.alefeducation.dimensions.course.parquet

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.course.transform.CourseMutatedTransform.CourseUpdatedParquetSource
import com.alefeducation.util.{ParquetBatchWriter, SparkSessionUtils}
import org.apache.spark.sql.SparkSession

object CourseDetailsUpdatedParquet {
  val CourseDetailsUpdatedParquetService = "parquet-course-details-updated"
  val CourseDetailsUpdatedParquetSink = "parquet-course-details-updated-sink"

  val session: SparkSession = SparkSessionUtils.getSession(CourseDetailsUpdatedParquetService)
  val service = new SparkBatchService(CourseDetailsUpdatedParquetService, session)

  def main(args: Array[String]): Unit = {
    val writer = new ParquetBatchWriter(session, service, CourseUpdatedParquetSource, CourseDetailsUpdatedParquetSink)
    service.run(writer.write())
  }

}