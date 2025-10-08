package com.alefeducation.facts

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.Constants.{LearningContentFinished, LearningContentSkipped, LearningContentStarted}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.IntegerType

import scala.collection.immutable.Map

class LearningContentSession(override val name: String, override val session: SparkSession) extends SparkBatchService {

  import LearningContentSession._
  import Delta.Transformations._
  import com.alefeducation.util.BatchTransformerUtility._
  import session.implicits._

  override def transform(): List[Sink] = {
    transformContentStarted() ++ transformContentSkipped() ++ transformContentFinished()
  }

  private val learningContentEventTypeMap: Map[String, Int] = Map(
    LearningContentStarted -> 1,
    LearningContentSkipped -> 2,
    LearningContentFinished -> 3
  )

  private def transformContentStarted(): List[Sink] = {
    val contentStarted = readOptional(ParquetLearningContentStartedSource, session)
    val parquetSink = contentStarted.map(_.toParquetSink(ParquetLearningContentStartedSink))

    val transformedContentStarted = contentStarted.map(
      _.withColumn("eventType", lit(learningContentEventTypeMap(LearningContentStarted)))
        .withColumn("isStart", lit(true))
        .transformForInsertFact(LearningContentSessionColMapping, LearningContentSessionEntity, ids = List("uuid"))
        .withColumn("fcs_date_dw_id",col("fcs_date_dw_id").cast(IntegerType))
    )

    val redshiftSink = transformedContentStarted.map(
      _.drop("eventdate")
        .toRedshiftInsertSink(RedshiftLearningContentSessionSink, LearningContentStarted)
    )
    val deltaSink = transformedContentStarted
      .map(_.renameUUIDcolsForDelta(Some(s"${LearningContentSessionEntity}_")))
      .flatMap(_.toCreate(isFact = true).map(_.toSink(DeltaLearningContentSessionSink)))

    (parquetSink ++ redshiftSink ++ deltaSink).toList
  }

  private def transformContentSkipped(): List[Sink] = {
    val contentSkipped = readOptional(ParquetLearningContentSkippedSource, session)
    val parquetSink = contentSkipped.map(_.toParquetSink(ParquetLearningContentSkippedSink))

    val transformedContentSkipped = contentSkipped.map(
      _.withColumn("eventType", lit(learningContentEventTypeMap(LearningContentSkipped)))
        .withColumn("isStart", lit(false))
        .transformForInsertFact(LearningContentSessionColMapping, LearningContentSessionEntity, ids = List("uuid"))
        .withColumn("fcs_date_dw_id",col("fcs_date_dw_id").cast(IntegerType))
    )

    val redshiftSink = transformedContentSkipped.map(
      _.drop("eventdate")
        .toRedshiftInsertSink(RedshiftLearningContentSessionSink, LearningContentSkipped)
    )
    val deltaSink = transformedContentSkipped
      .map(_.renameUUIDcolsForDelta(Some(s"${LearningContentSessionEntity}_")))
      .flatMap(_.toCreate(isFact = true).map(_.toSink(DeltaLearningContentSessionSink)))

    (parquetSink ++ redshiftSink ++ deltaSink).toList
  }

  private def transformContentFinished(): List[Sink] = {
    val contentFinished = readOptional(ParquetLearningContentFinishedSource, session)
    val parquetSink = contentFinished.map(_.toParquetSink(ParquetLearningContentFinishedSink))

    val transformedContentFinished = contentFinished.map(
      _.withColumn("eventType", lit(learningContentEventTypeMap(LearningContentFinished)))
        .withColumn("isStart", lit(false))
        .transformForInsertFact(LearningContentFinishedColMapping, LearningContentSessionEntity, ids = List("uuid"))
        .withColumn("fcs_date_dw_id",col("fcs_date_dw_id").cast(IntegerType))
    )

    val redshiftSink = transformedContentFinished.map(
      _.drop("eventdate")
        .toRedshiftInsertSink(RedshiftLearningContentSessionSink, LearningContentFinished)
    )
    val deltaSink = transformedContentFinished
      .map(_.renameUUIDcolsForDelta(Some(s"${LearningContentSessionEntity}_")))
      .flatMap(_.toCreate(isFact = true).map(_.toSink(DeltaLearningContentSessionSink)))

    (parquetSink ++ redshiftSink ++ deltaSink).toList
  }
}

object LearningContentSession {
  val LearningContentSessionEntity = "fcs"

  val LearningContentSession = "learning-content-session-fact"

  val ParquetLearningContentStartedSource = "parquet-learning-content-started-source"
  val ParquetLearningContentSkippedSource = "parquet-learning-content-skipped-source"
  val ParquetLearningContentFinishedSource = "parquet-learning-content-finished-source"

  val ParquetLearningContentStartedSink = "parquet-learning-content-started-sink"
  val ParquetLearningContentSkippedSink = "parquet-learning-content-skipped-sink"
  val ParquetLearningContentFinishedSink = "parquet-learning-content-finished-sink"

  val RedshiftLearningContentSessionSink = "redshift-learning-content-session-sink"
  val DeltaLearningContentSessionSink = "delta-learning-content-session-sink"

  val LearningContentSessionColMapping: Map[String, String] = Map(
    "occurredOn" -> "occurredOn",
    "uuid" -> "fcs_id",
    "eventType" -> "fcs_event_type",
    "isStart" -> "fcs_is_start",
    "learningSessionId" -> "fcs_ls_id",
    "contentId" -> "fcs_content_id",
    "learningObjectiveId" -> "fcs_lo_id",
    "studentId" -> "fcs_student_id",
    "classId" -> "fcs_class_id",
    "studentGradeId" -> "fcs_grade_id",
    "tenantId" -> "fcs_tenant_id",
    "schoolId" -> "fcs_school_id",
    "academicYearId" -> "fcs_ay_id",
    "studentSection" -> "fcs_section_id",
    "learningPathId" -> "fcs_lp_id",
    "instructionalPlanId" -> "fcs_ip_id",
    "outsideOfSchool" -> "fcs_outside_of_school",
    "contentAcademicYear" -> "fcs_content_academic_year",
  )
  val LearningContentFinishedColMapping: Map[String, String] = LearningContentSessionColMapping ++ Map(
    "vendorData.timespent" -> "fcs_app_timespent",
    "vendorData.score" -> "fcs_app_score"
  )

  def apply(implicit spark: SparkSession): LearningContentSession = new LearningContentSession(LearningContentSession, spark)

  def main(args: Array[String]): Unit =
    new LearningContentSession(LearningContentSession, SparkSessionUtils.getSession(LearningContentSession)).run

}
