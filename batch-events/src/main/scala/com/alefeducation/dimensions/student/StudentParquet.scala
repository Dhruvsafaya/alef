package com.alefeducation.dimensions.student

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.{ParquetBatchWriter, SparkSessionUtils}

object StudentParquet {
  val ParquetStudentSource = "parquet-student-source"
  val ParquetStudentsDeletedSource = "parquet-students-deleted-source"
  val ParquetStudentSectionUpdatedSource = "parquet-student-section-updated-source"
  val ParquetStudentEnableDisableSource = "parquet-student-enable-disable-source"
  val ParquetStudentSchoolOrGradeMoveSource = "parquet-student-school-grade-move-source"
  val ParquetStudentPromotedSource = "parquet-student-promoted-source"
  val ParquetStudentTagUpdatedSource = "parquet-student-tag-updated-source"

  val ParquetStudentSink = "parquet-student-sink"
  val ParquetStudentsDeletedSink = "parquet-students-deleted-sink"
  val ParquetStudentSectionUpdatedSink = "parquet-student-section-updated-sink"
  val ParquetStudentEnableDisableSink = "parquet-student-enable-disable-sink"
  val ParquetStudentSchoolGradeMoveSink = "parquet-student-school-grade-move-sink"
  val ParquetStudentPromotedSink = "parquet-student-promoted-sink"
  val ParquetStudentTagUpdatedSink = "parquet-student-tag-updated-sink"

  val sourceSinks = List((ParquetStudentSource, ParquetStudentSink),
    (ParquetStudentsDeletedSource, ParquetStudentsDeletedSink),
    (ParquetStudentSectionUpdatedSource, ParquetStudentSectionUpdatedSink),
    (ParquetStudentEnableDisableSource, ParquetStudentEnableDisableSink),
    (ParquetStudentSchoolOrGradeMoveSource, ParquetStudentSchoolGradeMoveSink),
    (ParquetStudentPromotedSource, ParquetStudentPromotedSink),
    (ParquetStudentTagUpdatedSource, ParquetStudentTagUpdatedSink))

  val StudentParquetService = "parquet-student"
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
