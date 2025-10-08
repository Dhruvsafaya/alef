package com.alefeducation.dimensions.student

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta.Transformations.{DeltaDataFrameTransformationsOptional, DeltaSCDContextTransformations}
import com.alefeducation.dimensions.student.StudentDelta.{StudentDeltaSink, StudentTransformedSource}
import com.alefeducation.dimensions.student.StudentRelUserTransform.StudentRelTransformedSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.AdminStudent
import com.alefeducation.util.Helpers.{ParquetStudentSource, StudentEntity}
import com.alefeducation.util.{Resources, SparkSessionUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Map

class StudentDelta(val session: SparkSession, val service: SparkBatchService) {
  def transform(): Option[Sink] = {
    val source = service.readOptional(StudentTransformedSource, session)
    source.map(_.withColumnRenamed("student_uuid", "student_id")
      .withColumnRenamed("section_uuid", "section_id")
      .withColumnRenamed("school_uuid", "school_id")
      .withColumnRenamed("grade_uuid", "grade_id"))
      .toSCD(uniqueIdColumns = List("student_id"))
      .map(_.toSink(StudentDeltaSink, StudentEntity))
  }
}

object StudentDelta {
  val StudentDeltaService = "delta-student"
  val StudentTransformedSource = "transform-student-rel"
  val StudentDeltaSink = "delta-student-sink"

  private val session = SparkSessionUtils.getSession(StudentDeltaService)
  val service = new SparkBatchService(StudentDeltaService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new StudentDelta(session, service)
    service.run(transformer.transform())
  }
}
