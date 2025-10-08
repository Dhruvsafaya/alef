package com.alefeducation.dimensions.teacher

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.{ParquetBatchWriter, SparkSessionUtils}

object TeacherParquet {
  val ParquetTeacherSource = "parquet-teacher-source"
  val ParquetTeacherSchoolMovedSource = "parquet-teacher-school-moved-source"

  val ParquetTeacherSink = "parquet-teacher-sink"
  val ParquetTeacherSchoolMovedSink = "parquet-teacher-school-moved-sink"

  val sourceSinks = List((ParquetTeacherSource, ParquetTeacherSink),
    (ParquetTeacherSchoolMovedSource, ParquetTeacherSchoolMovedSink))

  val StudentParquetService = "parquet-teacher"
  val session = SparkSessionUtils.getSession(StudentParquetService)
  val service = new SparkBatchService(StudentParquetService, session)

  def main(args: Array[String]): Unit = {
    sourceSinks.foreach(list => {
    val (source, sink) = (list._1, list._2)
    val writer = new ParquetBatchWriter(session, service, source, sink)
    service.run(writer.write())
    })
  }
}
