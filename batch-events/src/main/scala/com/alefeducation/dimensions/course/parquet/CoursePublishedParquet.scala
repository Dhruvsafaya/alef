package com.alefeducation.dimensions.course.parquet

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.course.transform.CourseMutatedTransform.CoursePublishedParquetSource
import com.alefeducation.util.{ParquetBatchWriter, SparkSessionUtils}
import org.apache.spark.sql.SparkSession

object CoursePublishedParquet {
  val CoursePublishedParquetService = "parquet-course-published"
  val CoursePublishedParquetSink = "parquet-course-published-sink"

  val session: SparkSession = SparkSessionUtils.getSession(CoursePublishedParquetService)
  val service = new SparkBatchService(CoursePublishedParquetService, session)

  def main(args: Array[String]): Unit = {
    val writer = new ParquetBatchWriter(session, service, CoursePublishedParquetSource, CoursePublishedParquetSink)
    service.run(writer.write())
  }

}
