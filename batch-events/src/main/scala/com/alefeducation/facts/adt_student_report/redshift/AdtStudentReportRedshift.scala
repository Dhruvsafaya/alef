package com.alefeducation.facts.adt_student_report.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.facts.adt_student_report.transform.AdtStudentReportTransform.ADTStudentReportTransformedSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class AdtStudentReportRedshift(val session: SparkSession,
                               val service: SparkBatchService) {
 import AdtStudentReportRedshift._
  def transform(): Option[Sink] = {
    val dataFrame = service.readOptional(ADTStudentReportTransformedSink, session).map(_.drop("fasr_breakdown"))
    dataFrame.map(DataSink(ADTStudentReportRedshiftSink, _))
  }

}

object AdtStudentReportRedshift {
  val ADTStudentReportRedshiftService = "redshift-adt-student-report"
  val ADTStudentReportRedshiftSink = "redshift-adt-student-report-sink"


  val session = SparkSessionUtils.getSession(ADTStudentReportRedshiftService)
  val service = new SparkBatchService(ADTStudentReportRedshiftService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new AdtStudentReportRedshift(session, service)
    service.run(transformer.transform())
  }
}

