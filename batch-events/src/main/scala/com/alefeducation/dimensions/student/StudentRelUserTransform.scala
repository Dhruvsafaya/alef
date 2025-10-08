package com.alefeducation.dimensions.student

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.student.StudentRelUserTransform.StudentRelTransformedSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.{AdminStudent, StudentUserType}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

class StudentRelUserTransform(val session: SparkSession, val service: SparkBatchService) {
  def transform(): Option[Sink] = {
    val studentCreatedDf = service.readOptional(ParquetStudentSource, session).map(_.filter(col("eventType") === AdminStudent))
    val df = studentCreatedDf.map(_.select(col("uuid").as("user_id"), col("occurredOn"))
      .transformForInsertDwIdMapping("user", StudentUserType)
    )
    df.map(DataSink(StudentRelTransformedSink, _))
  }
}

object StudentRelUserTransform {
  val StudentRelTransformedSink = "transform-user-rel"
  val StudentParquetService = "transform-rel-user-student"
  val session = SparkSessionUtils.getSession(StudentParquetService)
  val service = new SparkBatchService(StudentParquetService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new StudentRelUserTransform(session, service)
    service.run(transformer.transform())
  }
}
