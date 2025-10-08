package com.alefeducation.dimensions.teacher

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.teacher.TeacherRelUserTransform.TeacherRelUserTransformedSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.{AdminTeacher, TeacherUserType}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class TeacherRelUserTransform(val session: SparkSession, val service: SparkBatchService) {
  def transform(): Option[Sink] = {
    val teacherCreatedDf = service.readOptional(ParquetTeacherSource, session).map(_.filter(col("eventType") === AdminTeacher))
    val df = teacherCreatedDf.map(_.select(col("uuid").as("user_id"), col("occurredOn"))
      .transformForInsertDwIdMapping("user", TeacherUserType)
    )
    df.map(DataSink(TeacherRelUserTransformedSink, _))
  }
}

object TeacherRelUserTransform {
  val TeacherRelUserTransformService = "transform-rel-user-teacher"
  val TeacherRelUserTransformedSink = "transform-teacher-rel-user-sink"
  val session = SparkSessionUtils.getSession(TeacherRelUserTransformService)
  val service = new SparkBatchService(TeacherRelUserTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new TeacherRelUserTransform(session, service)
    service.run(transformer.transform())
  }
}
