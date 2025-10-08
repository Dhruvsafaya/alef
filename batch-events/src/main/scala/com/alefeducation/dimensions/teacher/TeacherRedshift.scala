package com.alefeducation.dimensions.teacher

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.teacher.TeacherRedshift._
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.AdminTeacher
import com.alefeducation.util.DataFrameUtility.setScdTypeIIPostAction
import com.alefeducation.util.Helpers.TeacherEntity
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class TeacherRedshift(val session: SparkSession, val service: SparkBatchService) {
  def transform(): Option[Sink] = {
    val createdSource = service.readOptional(TeacherTransformedSource, session)
    createdSource.map(_.toRedshiftSCDSink(TeacherRedshiftSink, AdminTeacher, TeacherEntity, setScdTypeIIPostAction))
  }
}

object TeacherRedshift {
  val TeacherRedshiftService = "redshift-teacher"

  val TeacherTransformedSource = "teacher-transform-rel"
  val TeacherRedshiftSink = "redshift-teacher-rel"

  private val session = SparkSessionUtils.getSession(TeacherRedshiftService)
  val service = new SparkBatchService(TeacherRedshiftService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new TeacherRedshift(session, service)
    service.run(transformer.transform())
  }
}
