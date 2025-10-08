package com.alefeducation.facts

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.BatchUtility.getParquetSink
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import com.alefeducation.util.BatchSessionUtility.{addIsStartEventProcDefCol, filterOutProcessedEvents, transformForDelta, filterOutProcessedEventsOpt}
import com.alefeducation.util.BatchTransformerUtility._

class PracticeSessionTransformer(override val name: String, override val session: SparkSession) extends SparkBatchService {

  import Delta.Transformations._
  import session.implicits._

  val isStartEventProcessed = s"${FactPracticeSessionEntity}_is_start_event_processed"

  val dateFormat = "yyyy-MM-dd"

  val isEndEventGroupCondition: Column = col("practice_session_is_start") === false
  val isSessionStartedCondition: Column = col("practice_session_is_start") === true

  val endEventCols = List(
    s"${FactPracticeSessionEntity}_outside_of_school",
    s"${FactPracticeSessionEntity}_date_dw_id",
    s"${FactPracticeSessionEntity}_is_start",
    s"${FactPracticeSessionEntity}_score",
    s"${FactPracticeSessionEntity}_sa_score",
    s"${FactPracticeSessionEntity}_stars"
  )

  override def transform(): List[Sink] = {
    val sessionStarted = readOptional(PracticeSessionStarted, session)
    val sessionFinished = readOptional(PracticeSessionFinished, session)
    val itemSessionStarted = readOptional(PracticeItemSessionStarted, session)
    val itemSessionFinished = readOptional(PracticeItemSessionFinished, session)
    val itemContentSessionStarted = readOptional(PracticeItemContentSessionStarted, session)
    val itemContentSessionFinished = readOptional(PracticeItemContentSessionFinished, session)
    val deltaStaging = readOptional(PracticeDeltaStaging, session, isMandatory = false)
    val deltaItemStaging = readOptional(PracticeItemDeltaStaging, session, isMandatory = false)
    val deltaItemContentStaging = readOptional(PracticeItemContentDeltaStaging, session, isMandatory = false)

    val redshiftSessionStarted = sessionStarted.map(
      preprocessRedshift(
        FactPracticeSessionEntity,
        StagingPracticeSession,
        addDefaultCols(practiceSessionType, isStart = true) _ compose addDefaultScoreCol compose addDefaultStarsCol,
        isFact = true
      )
    )
    val redshiftSessionFinished = sessionFinished.map(
      preprocessRedshift(
        FactPracticeSessionEntity,
        StagingPracticeSession,
        addDefaultCols(practiceSessionType, isStart = false) _ compose addDefaultStarsIfNullCol,
        isFact = true
      )
    )

    val redshiftItemSessionStarted = itemSessionStarted.map(
      preprocessRedshift(
        FactPracticeSessionEntity,
        StagingPracticeItemSession,
        addDefaultCols(practiceItemSessionType, isStart = true) _ compose addDefaultScoreCol compose addDefaultStarsCol,
        isFact = true
      )
    )
    val redshiftItemSessionFinished = itemSessionFinished.map(
      preprocessRedshift(
        FactPracticeSessionEntity,
        StagingPracticeItemSession,
        addDefaultCols(practiceItemSessionType, isStart = false) _ compose addDefaultStarsCol,
        isFact = true
      )
    )

    val redshiftItemContentSessionStarted = itemContentSessionStarted.map(
      preprocessRedshift(
        FactPracticeSessionEntity,
        StagingPracticeItemContentSession,
        addDefaultCols(practiceItemContentSessionType, isStart = true) _ compose addDefaultScoreCol compose addDefaultStarsCol,
        isFact = true
      )
    )
    val redshiftItemContentSessionFinished = itemContentSessionFinished.map(
      preprocessRedshift(
        FactPracticeSessionEntity,
        StagingPracticeItemContentSession,
        addDefaultCols(practiceItemContentSessionType, isStart = false) _ compose addDefaultStarsCol,
        isFact = true
      )
    )

    val redshiftSinks = (
      redshiftSessionStarted.map(DataSink(RedshiftPracticeSessionSink, _)) ++
        redshiftSessionFinished.map(DataSink(RedshiftPracticeSessionSink, _)) ++
        redshiftItemSessionStarted.map(DataSink(RedshiftPracticeSessionSink, _)) ++
        redshiftItemSessionFinished.map(DataSink(RedshiftPracticeSessionSink, _)) ++
        redshiftItemContentSessionStarted.map(DataSink(RedshiftPracticeSessionSink, _)) ++
        redshiftItemContentSessionFinished.map(DataSink(RedshiftPracticeSessionSink, _))
    ).toList

    val parquetPracticeSessionStartedSinks = sessionStarted.map(getParquetSink(session, PracticeSessionStarted, _))
    val parquetPracticeSessionFinishedSinks = sessionFinished.map(getParquetSink(session, PracticeSessionFinished, _))
    val parquetPracticeItemSessionStartedSinks = itemSessionStarted.map(getParquetSink(session, PracticeItemSessionStarted, _))
    val parquetPracticeItemSessionFinishedSinks = itemSessionFinished.map(getParquetSink(session, PracticeItemSessionFinished, _))
    val parquetPracticeItemContentSessionStartedSinks =
      itemContentSessionStarted.map(getParquetSink(session, PracticeItemContentSessionStarted, _))
    val parquetPracticeItemContentSessionFinishedSinks =
      itemContentSessionFinished.map(getParquetSink(session, PracticeItemContentSessionFinished, _))

    val deltaPracticeSessionsSinks = getDeltaPracticeSessionSinks(
      List(s"${FactPracticeSessionEntity}_id"),
      redshiftSessionStarted,
      redshiftSessionFinished,
      deltaStaging,
      PracticeDeltaStaging,
      s"${FactPracticeSessionEntity}_$practiceSessionType"
    )

    val deltaPracticeItemSessionsSinks = getDeltaPracticeSessionSinks(
      List(s"${FactPracticeSessionEntity}_id", s"${FactPracticeSessionEntity}_item_lo_id"),
      redshiftItemSessionStarted,
      redshiftItemSessionFinished,
      deltaItemStaging,
      PracticeItemDeltaStaging,
      s"${FactPracticeSessionEntity}_$practiceItemSessionType"
    )

    val deltaPracticeItemContentSessionsSinks = getDeltaPracticeSessionSinks(
      List(s"${FactPracticeSessionEntity}_id", s"${FactPracticeSessionEntity}_item_lo_id", s"${FactPracticeSessionEntity}_item_step_id"),
      redshiftItemContentSessionStarted,
      redshiftItemContentSessionFinished,
      deltaItemContentStaging,
      PracticeItemContentDeltaStaging,
      s"${FactPracticeSessionEntity}_$practiceItemContentSessionType"
    )

    redshiftSinks ++
      deltaPracticeSessionsSinks ++ deltaPracticeItemSessionsSinks ++ deltaPracticeItemContentSessionsSinks ++
      parquetPracticeSessionStartedSinks ++ parquetPracticeSessionFinishedSinks ++
      parquetPracticeItemSessionStartedSinks ++ parquetPracticeItemSessionFinishedSinks ++
      parquetPracticeItemContentSessionStartedSinks ++ parquetPracticeItemContentSessionFinishedSinks
  }

  def renameCols(df: DataFrame): DataFrame =
    df.withColumnRenamed("tenant_id", s"${FactPracticeSessionEntity}_tenant_id")
      .withColumnRenamed("student_id", s"${FactPracticeSessionEntity}_student_id")
      .withColumnRenamed("subject_id", s"${FactPracticeSessionEntity}_subject_id")
      .withColumnRenamed("school_id", s"${FactPracticeSessionEntity}_school_id")
      .withColumnRenamed("grade_id", s"${FactPracticeSessionEntity}_grade_id")
      .withColumnRenamed("section_id", s"${FactPracticeSessionEntity}_section_id")
      .withColumnRenamed("lo_id", s"${FactPracticeSessionEntity}_lo_id")
      .withColumnRenamed("academic_year_id", s"${FactPracticeSessionEntity}_academic_year_id")
      .withColumnRenamed("class_id", s"${FactPracticeSessionEntity}_class_id")

  def addCols(df: DataFrame): DataFrame =
    df.transform(addColIfNotExist(s"${FactPracticeSessionEntity}_item_content_title"))
      .transform(addColIfNotExist(s"${FactPracticeSessionEntity}_item_content_lesson_type"))
      .transform(addColIfNotExist(s"${FactPracticeSessionEntity}_item_content_location"))
      .transform(addColIfNotExist(s"${FactPracticeSessionEntity}_item_lo_id"))
      .transform(addColIfNotExist(s"${FactPracticeSessionEntity}_item_step_id"))

  def addColIfNotExist(name: String)(df: DataFrame): DataFrame =
    if (!df.columns.contains(name)) df.withColumn(name, lit(null).cast(StringType))
    else df

  def getDeltaPracticeSessionSinks(ids: List[String],
                                   startSessions: Option[DataFrame],
                                   endSessions: Option[DataFrame],
                                   deltaStaging: Option[DataFrame],
                                   stagingSinkName: String,
                                   eventType: String): List[Sink] = {

    val deltaStagingWithIsStartEventProcColDf = deltaStaging.map(addIsStartEventProcDefCol(isStartEventProcessed))

    val deltaSessionDFs = combineOptionalDfs(startSessions, endSessions)
      .map(_.renameUUIDcolsForDelta())
      .map(renameCols)
      .map(addCols)
      .map(_.unionOptionalByName(deltaStagingWithIsStartEventProcColDf, true))
      .map(
        transformForDelta(
          FactPracticeSessionEntity,
          ids,
          endEventCols,
          isEndEventGroupCondition,
          isSessionStartedCondition,
          consumeAllStartEvents = true
        )
      )

    val deltaFactDF = deltaSessionDFs.map {
      case (startEndEventsDf, startEventsDf) => startEndEventsDf.unionByName(filterOutProcessedEvents(isStartEventProcessed)(startEventsDf))
    }
    val deltaStagingDF = deltaSessionDFs
      .map(_._2)
      .flatMap(filterOutProcessedEventsOpt(isStartEventProcessed))
      .map(
        _.withColumnRenamed(s"${FactPracticeSessionEntity}_start_time", s"${FactPracticeSessionEntity}_created_time")
          .withColumn(isStartEventProcessed, lit(true))
          .drop(s"${FactPracticeSessionEntity}_end_time", s"${FactPracticeSessionEntity}_time_spent")
      )

    val deltaFactSink: Option[Sink] = deltaFactDF.flatMap(
      _.withColumn("eventdate", date_format(col(s"${FactPracticeSessionEntity}_start_time"), dateFormat))
        .toCreate(isFact = true)
        .map(_.toSink(DeltaPracticeSessionSink, eventType = eventType)))

    val deltaParquetStatingSink: Option[DataSink] =
      deltaStagingDF.map(DataSink(stagingSinkName, _, eventType = eventType))

    (deltaFactSink ++ deltaParquetStatingSink).toList
  }

  private def addDefaultCols(tp: Int, isStart: Boolean)(df: DataFrame): DataFrame =
    df.withColumn("occurredOn", regexp_replace($"occurredOn", "T", " "))
      .withColumn("type", lit(tp))
      .withColumn("isStart", lit(isStart))
      .withColumn("isStartProcessed", lit(false))

  private def addDefaultScoreCol(df: DataFrame): DataFrame =
    df.withColumn("score", lit(DefaultScore).cast(DoubleType))

  private def addDefaultStarsCol(df: DataFrame): DataFrame =
    df.withColumn("stars", lit(DefaultStars))

  private def addDefaultStarsIfNullCol(df: DataFrame): DataFrame =
    df.withColumn("stars", when($"stars".isNotNull, $"stars").otherwise(lit(DefaultStars)))

}

object PracticeSession {
  def main(args: Array[String]): Unit = {
    new PracticeSessionTransformer(PracticeFactName, SparkSessionUtils.getSession(PracticeFactName)).run
  }
}
