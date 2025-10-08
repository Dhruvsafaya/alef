package com.alefeducation.facts

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers.FactKtGameSessionEntity
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.ktgame.KTGameHelper._
import com.alefeducation.util.ktgame.KTGameUtility
import org.apache.spark.sql.functions.{col, date_format, lit}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class KTGameSessionTransformer(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {

  val isStartEventProcessed = "ktg_session_is_start_event_processed"

  override def transform(): List[Sink] = {
    import KTGameUtility._
    import com.alefeducation.util.BatchTransformerUtility._

    val ktGameStartedDF = readOptional(ParquetKTGameStartedSource, session)
    val ktGameQuestionStartedDF = readOptional(ParquetKTGameQuestionStartedSource, session)
    val ktGameQuestionFinishedDF = readOptional(ParquetKTGameQuestionFinishedSource, session)
    val ktGameFinishedDF = readOptional(ParquetKTGameFinishedSource, session)

    val ktGameStagingDF = readOptional(ParquetTtGameDeltaStagingSource, session, isMandatory = false)
    val ktGameQuestionStagingDF = readOptional(ParquetTtGameQuestionDeltaStagingSource, session, isMandatory = false)

    //Redshift Transformations
    val ktgStartedTransDF = ktGameStartedDF.map(_.selectStartedColumns.transform(transformForRedshift))
    val ktgQuestionStartedTransDF = ktGameQuestionStartedDF.map(_.selectQuestionStartedColumns.transform(transformForRedshift))
    val ktgQuestionFinishedTransDF = ktGameQuestionFinishedDF.map(_.selectQuestionFinishedColumns.transform(transformForRedshift))
    val ktgFinishedTransDF = ktGameFinishedDF.map(_.selectFinishedColumns.transform(transformForRedshift))

    //Parquet Sinks
    val ktgStartedParquetSink = ktGameStartedDF.map(_.toParquetSink(ParquetKTGameStartedSource))
    val ktgQuestionStartedParquetSink = ktGameQuestionStartedDF.map(_.toParquetSink(ParquetKTGameQuestionStartedSource))
    val ktgQuestionFinishedParquetSink = ktGameQuestionFinishedDF.map(_.toParquetSink(ParquetKTGameQuestionFinishedSource))
    val ktgFinishedParquetSink = ktGameFinishedDF.map(_.toParquetSink(ParquetKTGameFinishedSource))

    //Redshift Sinks
    val ktgStartedRedshiftSink = ktgStartedTransDF.map(_.toRedshiftInsertSink(RedshiftKTGameSessionSink, "ktGameStarted"))
    val ktgQuestionStartedRedshiftSink =
      ktgQuestionStartedTransDF.map(_.toRedshiftInsertSink(RedshiftKTGameSessionSink, "ktGameQuestionStarted"))
    val ktgQuestionFinishedRedshiftSink =
      ktgQuestionFinishedTransDF.map(_.toRedshiftInsertSink(RedshiftKTGameSessionSink, "ktGameQuestionFinished"))
    val ktgFinishedRedshiftSink = ktgFinishedTransDF.map(_.toRedshiftInsertSink(RedshiftKTGameSessionSink, "ktGameFinished"))

    //Delta Sinks
    val ktGameSessionDeltaSink: List[Sink] =
      getDeltaKtGameSessionSinks(ktgStartedTransDF,
                                 ktgFinishedTransDF,
                                 ktGameStagingDF,
                                 ParquetTtGameDeltaStagingSource,
                                 List("ktg_session_id"),
                                 "ktg_session")

    val ktGameQuestionDeltaSink: List[Sink] = getDeltaKtGameSessionSinks(
      ktgQuestionStartedTransDF,
      ktgQuestionFinishedTransDF,
      ktGameQuestionStagingDF,
      ParquetTtGameQuestionDeltaStagingSource,
      List("ktg_session_id", "ktg_session_question_id"),
      "ktg_question_session"
    )

    //All Sinks
    val parquetSinks: List[DataSink] =
      (ktgStartedParquetSink ++ ktgQuestionStartedParquetSink ++ ktgQuestionFinishedParquetSink ++ ktgFinishedParquetSink).toList

    val redshiftSinks: List[DataSink] =
      (ktgStartedRedshiftSink ++ ktgQuestionStartedRedshiftSink ++ ktgQuestionFinishedRedshiftSink ++ ktgFinishedRedshiftSink).toList

    val deltaSinks: List[Sink] =
      ktGameSessionDeltaSink ++ ktGameQuestionDeltaSink

    parquetSinks ++ redshiftSinks ++ deltaSinks

  }

  private def transformForRedshift(df: DataFrame): DataFrame =
    df.selectColumnsWithMapping(StagingKtGameSession).appendTimestampColumns(FactKtGameSession, isFact = true)

  private def getDeltaKtGameSessionSinks(startTransDF: Option[DataFrame],
                                         finishTransDF: Option[DataFrame],
                                         stagingDF: Option[DataFrame],
                                         stagingSinkName: String,
                                         ids: List[String],
                                         eventType: String): List[Sink] = {
    import Delta.Transformations._
    import com.alefeducation.util.BatchSessionUtility._

    val stagingWithIsStartEventProcColDf = stagingDF.map(addIsStartEventProcDefCol(isStartEventProcessed))

    val isEndEventGroupCondition: Column = col("ktg_session_is_start") === false
    val isSessionStartedCondition: Column = col("ktg_session_is_start") === true
    val endEventCols = List(
      "ktg_session_answer",
      "ktg_session_date_dw_id",
      "ktg_session_event_type",
      "ktg_session_instructional_plan_id",
      "ktg_session_is_attended",
      "ktg_session_is_start",
      "ktg_session_kt_id",
      "ktg_session_learning_path_id",
      "ktg_session_max_score",
      "ktg_session_num_attempts",
      "ktg_session_outside_of_school",
      "ktg_session_question_time_allotted",
      "ktg_session_score",
      "ktg_session_stars",
      "ktg_session_trimester_id",
      "ktg_session_trimester_order",
      "ktg_session_type",
      "ktg_session_material_id",
      "ktg_session_material_type"
    )

    val unionDF = unionStartEndSessions(startTransDF, finishTransDF).map(
      _.renameUUIDcolsForDelta(Some(FactKtGameSessionEntity + "_"))
        .unionOptionalByName(stagingWithIsStartEventProcColDf, true)
    )

    val deltaSessionDFs = unionDF
      .map(transformForDelta(FactKtGameSessionEntity, ids, endEventCols, isEndEventGroupCondition, isSessionStartedCondition))

    val deltaFactDF = deltaSessionDFs.map {
      case (startEndEventsDf, startEventsDf) => startEndEventsDf.unionByName(filterOutProcessedEvents(isStartEventProcessed)(startEventsDf))
    }
    val deltaStatingDF = deltaSessionDFs
      .map(
        _._2
          .withColumnRenamed("ktg_session_start_time", "ktg_session_created_time")
          .withColumn(isStartEventProcessed, lit(true))
          .drop("ktg_session_end_time", "ktg_session_time_spent"))

    val deltaFactSink: Option[Sink] = deltaFactDF.flatMap(
      _.withColumn("eventdate", date_format(col("ktg_session_start_time"), "yyyy-MM-dd"))
        .toCreate(isFact = true)
        .map(_.toSink(DeltaKTGameSessionSink, eventType = eventType)))

    val deltaParquetStagingSink: Option[DataSink] =
      deltaStatingDF.map(DataSink(stagingSinkName, _, eventType = eventType))

    (deltaFactSink ++ deltaParquetStagingSink).toList
  }

}

object KTGameSession {
  def main(args: Array[String]): Unit = {
    new KTGameSessionTransformer(ktGameSessionService, SparkSessionUtils.getSession(ktGameSessionService)).run
  }
}
