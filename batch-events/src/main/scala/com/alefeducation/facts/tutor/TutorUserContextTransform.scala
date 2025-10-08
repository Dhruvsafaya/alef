package com.alefeducation.facts.tutor

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.tutor.TutorUserContextTransform._
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class TutorUserContextTransform(val session: SparkSession,
                       val service: SparkBatchService) {

  def transform(): Option[DataSink] = {
    val tutorUserContextParquet = service.readOptional(ParquetTutorUserContextSource, session)

    val tutorUserContextTransformed = tutorUserContextParquet.map(
      _.transformForInsertFact(TutorUserContextCols, FactTutorUserContextEntity)
        .withColumn("ftc_subject_id", col("ftc_subject_id").cast(LongType))
      )

    tutorUserContextTransformed.map(df => {
      DataSink(TransformTutorUserContextSink, df)
    })
  }
}

object TutorUserContextTransform {
  val TutorUserContextTransformService = "transform-tutor-user-context"

  val FactTutorUserContextEntity = "ftc"

  val ParquetTutorUserContextSource = getSource(TutorUserContextTransformService).head
  val TransformTutorUserContextSink = getSink(TutorUserContextTransformService).head

  val TutorUserContextCols = Map[String, String](
    "userId" -> "ftc_user_id",
    "role" -> "ftc_role",
    "contextId" -> "ftc_context_id",
    "schoolId" -> "ftc_school_id",
    "gradeId" -> "ftc_grade_id",
    "grade" -> "ftc_grade",
    "subjectId" -> "ftc_subject_id",
    "subject" -> "ftc_subject",
    "language" -> "ftc_language",
    "occurredOn" -> "occurredOn",
    "tenantId" -> "ftc_tenant_id",
    "tutorLocked" -> "ftc_tutor_locked"
  )

  val session: SparkSession = SparkSessionUtils.getSession(TutorUserContextTransformService)
  val service = new SparkBatchService(TutorUserContextTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new TutorUserContextTransform(session, service)
    service.run(transformer.transform())
  }
}
