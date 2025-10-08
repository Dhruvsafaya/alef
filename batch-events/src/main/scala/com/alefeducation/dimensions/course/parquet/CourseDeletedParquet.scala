package com.alefeducation.dimensions.course.parquet

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.course.transform.CourseDeletedTransform.CourseDeletedParquetSource
import com.alefeducation.util.{ParquetBatchWriter, SparkSessionUtils}
import org.apache.spark.sql.SparkSession


object CourseDeletedParquet {
  val CourseDeletedParquetService = "parquet-course-deleted"
  val CourseDeletedParquetSink = "parquet-course-deleted-sink"

  val session: SparkSession = SparkSessionUtils.getSession(CourseDeletedParquetService)
  val service = new SparkBatchService(CourseDeletedParquetService, session)

  def main(args: Array[String]): Unit = {
    val writer = new ParquetBatchWriter(session, service, CourseDeletedParquetSource, CourseDeletedParquetSink)
    service.run(writer.write())
  }

}
