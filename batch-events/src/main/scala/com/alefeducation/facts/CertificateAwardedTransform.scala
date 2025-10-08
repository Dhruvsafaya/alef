package com.alefeducation.facts

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.CertificateAwardedTransform.{cols, entity, sinkName, sourceName}
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType

class CertificateAwardedTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {
    val source = service.readOptional(sourceName, session)

    val transformed = source.map(
      _.transformForInsertFact(cols, entity)
        .withColumn(s"${entity}_date_dw_id", col(s"${entity}_date_dw_id").cast(LongType))
    )

    transformed.map(DataSink(sinkName, _))
  }
}

object CertificateAwardedTransform {

  val serviceName = "transform-certificate-awarded"
  val sourceName = "parquet-certificate-awarded-source"
  val sinkName = "transformed-certificate-awarded-sink"
  val entity = "fsca"

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  val cols: Map[String, String] = Map(
    "certificateId" -> s"${entity}_certificate_id",
    "studentId" -> s"${entity}_student_id",
    "academicYearId" -> s"${entity}_academic_year_id",
    "gradeId" -> s"${entity}_grade_id",
    "classId" -> s"${entity}_class_id",
    "awardedBy" -> s"${entity}_teacher_id",
    "category" -> s"${entity}_award_category",
    "purpose" -> s"${entity}_award_purpose",
    "language" -> s"${entity}_language",
    "tenantId" -> s"${entity}_tenant_id",
    "occurredOn" -> "occurredOn"
  )

  def main(args: Array[String]): Unit = {
    val transform = new CertificateAwardedTransform(session, service)
    service.run(transform.transform())
  }
}


