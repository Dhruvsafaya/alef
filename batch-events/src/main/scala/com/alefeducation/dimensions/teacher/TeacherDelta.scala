package com.alefeducation.dimensions.teacher

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.teacher.TeacherDelta.TeacherDeltaSink
import com.alefeducation.dimensions.teacher.TeacherRedshift.TeacherTransformedSource
import com.alefeducation.util.Helpers.TeacherEntity
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class TeacherDelta(val session: SparkSession, val service: SparkBatchService) {
  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  def transform(): Option[Sink] = {
    val source = service.readOptional(TeacherTransformedSource, session)
    source.map(_.toSCDContext(uniqueIdColumns = List("teacher_id")))
      .map(_.toSink(TeacherDeltaSink, TeacherEntity))
  }
}

object TeacherDelta {
  val TeacherDeltaService = "delta-teacher"
  val TeacherTransformedSource = "teacher-transform-rel"
  val TeacherDeltaSink = "delta-teacher-sink"

  private val session = SparkSessionUtils.getSession(TeacherDeltaService)
  val service = new SparkBatchService(TeacherDeltaService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new TeacherDelta(session, service)
    service.run(transformer.transform())
  }
}
