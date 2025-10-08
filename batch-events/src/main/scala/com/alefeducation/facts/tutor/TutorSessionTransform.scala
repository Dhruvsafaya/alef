package com.alefeducation.facts.tutor

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.tutor.TutorSessionTransform._
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType

class TutorSessionTransform(val session: SparkSession,
                           val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {
    val tutorSessionParquet = service.readOptional(ParquetTutorSessionSource, session, extraProps = List(("mergeSchema", "true")))

    val tutorSessionTransformed = tutorSessionParquet.map(
      _.transformForInsertFact(TutorSessionCols, FactTutorSessionEntity)
        .transform(sessionStatus(FactTutorSessionEntity))
        .withColumn("fts_subject_id", col("fts_subject_id").cast(LongType))
    )

    tutorSessionTransformed.map(df => {
      DataSink(TransformTutorSessionSink, df)
    })
  }

}

object TutorSessionTransform {
  val TutorSessionTransformService = "transform-tutor-session"

  val FactTutorSessionEntity = "fts"

  val ParquetTutorSessionSource = getSource(TutorSessionTransformService).head
  val TransformTutorSessionSink = getSink(TutorSessionTransformService).head
  
  val TutorSessionCols = Map[String, String](
    "userId" -> "fts_user_id",
    "role" -> "fts_role",
    "contextId" -> "fts_context_id",
    "schoolId" -> "fts_school_id",
    "gradeId" -> "fts_grade_id",
    "grade" -> "fts_grade",
    "subjectId" -> "fts_subject_id",
    "subject" -> "fts_subject",
    "language" -> "fts_language",
    "sessionStatus" -> "fts_session_state",
    "materialId" -> "fts_material_id",
    "materialType" -> "fts_material_type",
    "activityId" -> "fts_activity_id",
    "activityStatus" -> "fts_activity_status",
    "levelId" -> "fts_level_id",
    "outcomeId" -> "fts_outcome_id",
    "occurredOn" -> "occurredOn",
    "tenantId" -> "fts_tenant_id",
    "sessionId" -> "fts_session_id",
    "learningSessionId" -> "fts_learning_session_id",
    "sessionMessageLimitReached" -> "fts_session_message_limit_reached"
  )

  val session: SparkSession = SparkSessionUtils.getSession(TutorSessionTransformService)
  val service = new SparkBatchService(TutorSessionTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new TutorSessionTransform(session, service)
    service.run(transformer.transform())
  }
}