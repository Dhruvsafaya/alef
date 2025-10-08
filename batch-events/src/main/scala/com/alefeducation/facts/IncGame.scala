package com.alefeducation.facts

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{regexp_replace, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.alefeducation.util.BatchTransformerUtility._
import org.apache.spark.sql.types.{IntegerType, StringType}

class IncGame(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {

  import Delta.Transformations._
  import session.implicits._

  val isStartEventProcessed = "inc_game_session_is_start_event_processed"

  private val dateFormat = "yyyy-MM-dd"

  private val incGameTableName = "staging_inc_game"
  private val incGameOutcomeTableName = "staging_inc_game_outcome"
  private val incGameSessionTableName = "staging_inc_game_session"

  private val deltaMatchCondition =
    s"""
       |${Alias.Delta}.${FactIncGameEntity}_id = ${Alias.Events}.${FactIncGameEntity}_id
     """.stripMargin

  private val redshiftMatchCondition =
    s"""
       |${FactIncGameEntity}_id = t.${FactIncGameEntity}_id
     """.stripMargin

  private val deltaOutcomeMatchCondition =
    s"""
       |${Alias.Delta}.${FactIncGameOutcomeEntity}_game_id <=> ${Alias.Events}.${FactIncGameOutcomeEntity}_game_id AND
       |${Alias.Delta}.${FactIncGameOutcomeEntity}_session_id <=> ${Alias.Events}.${FactIncGameOutcomeEntity}_session_id AND
       |${Alias.Delta}.${FactIncGameOutcomeEntity}_id = ${Alias.Events}.${FactIncGameOutcomeEntity}_id AND
       |${Alias.Delta}.${FactIncGameOutcomeEntity}_player_id <=> ${Alias.Events}.${FactIncGameOutcomeEntity}_player_id AND
       |${Alias.Delta}.${FactIncGameOutcomeEntity}_status <=> ${Alias.Events}.${FactIncGameOutcomeEntity}_status AND
       |${Alias.Delta}.${FactIncGameOutcomeEntity}_created_time = ${Alias.Events}.${FactIncGameOutcomeEntity}_created_time
       |""".stripMargin

  private val deltaSessionMatchCondition =
    s"""
       |${Alias.Delta}.${FactIncGameSessionEntity}_game_id <=> ${Alias.Events}.${FactIncGameSessionEntity}_game_id AND
       |${Alias.Delta}.${FactIncGameSessionEntity}_id = ${Alias.Events}.${FactIncGameSessionEntity}_id AND
       |${Alias.Delta}.${FactIncGameSessionEntity}_start_time <=> ${Alias.Events}.${FactIncGameSessionEntity}_start_time AND
       |${Alias.Delta}.${FactIncGameSessionEntity}_end_time <=> ${Alias.Events}.${FactIncGameSessionEntity}_end_time AND
       |${Alias.Delta}.${FactIncGameSessionEntity}_time_spent <=> ${Alias.Events}.${FactIncGameSessionEntity}_time_spent AND
       |${Alias.Delta}.${FactIncGameSessionEntity}_started_by_id <=> ${Alias.Events}.${FactIncGameSessionEntity}_started_by_id AND
       |${Alias.Delta}.${FactIncGameSessionEntity}_status = ${Alias.Events}.${FactIncGameSessionEntity}_status
       |""".stripMargin

  private val redshiftOutcomeMathCondition =
    s"""
       |NVL(game_uuid, '') = NVL(t.game_uuid, '') AND
       |NVL(session_uuid, '') = NVL(t.session_uuid, '') AND
       |NVL(player_uuid, '') = NVL(t.player_uuid) AND
       |${FactIncGameOutcomeEntity}_id = t.${FactIncGameOutcomeEntity}_id AND
       |${FactIncGameOutcomeEntity}_status = t.${FactIncGameOutcomeEntity}_status AND
       |${FactIncGameOutcomeEntity}_created_time = t.${FactIncGameOutcomeEntity}_created_time
       |""".stripMargin

  private val redshiftSessionMatchCondition =
    s"""
       |NVL(game_uuid, '') = NVL(t.game_uuid, '') AND
       |${FactIncGameSessionEntity}_id = t.${FactIncGameSessionEntity}_id AND
       |${FactIncGameSessionEntity}_started_by = t.${FactIncGameSessionEntity}_started_by AND
       |${FactIncGameSessionEntity}_status = t.${FactIncGameSessionEntity}_status AND
       |${FactIncGameSessionEntity}_created_time = t.${FactIncGameSessionEntity}_created_time
       |""".stripMargin

  private val typeField = "inc_game_event_type"

  private val deltaUpdateCols = List(
    s"${FactIncGameEntity}_title",
    s"${FactIncGameEntity}_instructional_plan_id"
  )

  private val filterNot = List("eventdate")

  def addNumQuestions(incGameSource: DataFrame): DataFrame = {
    incGameSource.withColumn("numQuestions",
                             when(col("questions").isNotNull, size(col("questions")))
                               .otherwise(lit(ZeroArraySize)))
  }

  override def transform(): List[Sink] = {
    //Read Raw Data
    val incGameSource = readOptional(IncGameSource, session)
    val incGameOutcomeSource = readOptional(IncGameOutcomeSource, session)
    val incGameSessionSource = readOptional(IncGameSessionSource, session)
    val incGameSessionDeltaStagingSource = readOptional(IncGameSessionDeltaStagingSource, session, isMandatory = false)

    //Redshift Transformations
    val redshiftIncGame = incGameSource.map(preprocessRedshift(FactIncGameEntity, IncGameCols, addReqColsForIncGame, true))
    val redshiftIncGameOutcome =
      incGameOutcomeSource.map(preprocessRedshift(FactIncGameOutcomeEntity, IncGameOutcomeCols, addReqColsForIncGameOutcome, true))
    val redshiftIncGameSession =
      incGameSessionSource.map(preprocessRedshift(FactIncGameSessionEntity, IncGameSessionCols, addReqColsForIncGameSession, true))

    //Delta Transformations
    import com.alefeducation.util.BatchTransformerUtility._
    val deltaIncGameCreatedDf = redshiftIncGame.map(preprocessDeltaIncGame(filterByType(IncGameCreatedEventType)))
    val deltaIncGameUpdatedDf = redshiftIncGame
      .map(preprocessDeltaIncGame(filterByType(IncGameUpdatedEventType)))
      .map(_.selectLatestByRowNumber(List(s"${FactIncGameEntity}_id"), s"${FactIncGameEntity}_created_time"))

    //Parquet Sinks
    val parquetIncGameSink = incGameSource.map(_.toParquetSink(IncGameSource))
    val parquetIncGameOutcomeSink = incGameOutcomeSource.map(_.toParquetSink(IncGameOutcomeSource))
    val parquetIncGameSessionSink = incGameSessionSource.map(_.toParquetSink(IncGameSessionSource))

    //Delta Sinks
    val deltaIncGameCreatedSink = deltaIncGameCreatedDf.map(getDeltaIncGameCreatedSink)
    val deltaIncGameUpdatedSink = deltaIncGameUpdatedDf.flatMap(getDeltaIncGameUpdatedSink)
    val deltaIncGameOutcomeSink = redshiftIncGameOutcome.map(getDeltaIncGameOutcomeSink)
    val deltaIncGameSessionSinks = getDeltaIncGameSessionSinks(redshiftIncGameSession, incGameSessionDeltaStagingSource)

    //Redshift Sinks
    val redshiftSinks = (redshiftIncGame.map(getRedshiftIncGameCreatedSink) ++
      redshiftIncGameSession.map(gameSession => {
        val newGameSession = gameSession.drop("inc_game_session_joined_players_details")
        getRedshiftIncGameSessionCreatedSink(newGameSession)
      }) ++
      redshiftIncGameOutcome.map(gameOutcome => {
        val gameOutcomeWithoutScoreBreakdown = gameOutcome.drop("scoreBreakdown")
        getRedshiftIncGameOutcomeCreatedSink(gameOutcomeWithoutScoreBreakdown)
      })).toList

    //Return all Sinks
    redshiftSinks ++ deltaIncGameCreatedSink ++ deltaIncGameUpdatedSink ++ deltaIncGameOutcomeSink ++
      deltaIncGameSessionSinks ++ parquetIncGameSink ++ parquetIncGameSessionSink ++ parquetIncGameOutcomeSink
  }

  private def getDeltaIncGameSessionSinks(redshiftIncGameSession: Option[DataFrame],
                                          incGameSessionDeltaStagingSource: Option[DataFrame]): List[Sink] = {
    import com.alefeducation.util.BatchSessionUtility._
    import com.alefeducation.util.BatchTransformerUtility._

    val incGameSessionStaging = incGameSessionDeltaStagingSource.map(addIsStartEventProcDefCol(isStartEventProcessed))
    val incGameSessionStagingWithExtraColumn = incGameSessionStaging.map(addCol("inc_game_session_joined_players_details"))

    val endEventCols = List("inc_game_session_date_dw_id", "inc_game_session_is_start", "inc_game_session_status")
    val isEndEventGroupCondition = col("inc_game_session_status").isin(2, 3)
    val isSessionStartedCondition = col("inc_game_session_status") === 1

    val deltaSessionDFs: Option[(DataFrame, DataFrame)] = redshiftIncGameSession
      .map(
        _.renameUUIDcolsForDelta(Some(FactIncGameSessionEntity + "_"))
          .withColumnRenamed("inc_game_session_started_by", "inc_game_session_started_by_id")
          .unionOptionalByName(incGameSessionStagingWithExtraColumn))
      .map(
        transformForDelta(FactIncGameSessionEntity,
                          List("inc_game_session_id"),
                          endEventCols,
                          isEndEventGroupCondition,
                          isSessionStartedCondition))

    val deltaFactDF = deltaSessionDFs.map {
      case (startEndEventsDf, startEventsDf) => startEndEventsDf.unionByName(filterOutProcessedEvents(isStartEventProcessed)(startEventsDf))
    }

    val deltaStatingDF = deltaSessionDFs.map(
      _._2
        .withColumnRenamed("inc_game_session_start_time", "inc_game_session_created_time")
        .withColumn(isStartEventProcessed, lit(true))
        .drop("inc_game_session_end_time", "inc_game_session_time_spent"))

    val deltaFactSink: Option[Sink] = deltaFactDF.map(
      _.withColumn("eventdate", date_format($"inc_game_session_start_time", dateFormat))
        .withColumn(s"${FactIncGameSessionEntity}_date_dw_id", col(s"${FactIncGameSessionEntity}_date_dw_id").cast(StringType))
        .toInsertIfNotExists(matchConditions = deltaSessionMatchCondition)
        .toSink(DeltaIncGameSessionSink, FactIncGameSessionEntity))

    val deltaParquetStagingSink: Option[DataSink] =
      deltaStatingDF.map(DataSink(IncGameSessionDeltaStagingSource, _, eventType = "IncGameSession"))

    (deltaFactSink ++ deltaParquetStagingSink).toList
  }

  private def getRedshiftIncGameCreatedSink(df: DataFrame): Sink =
    df.toInsertIfNotExistsSink(RedshiftIncGameSink, incGameTableName, uniqueIdsStatement = redshiftMatchCondition, isStaging = true, filterNot = filterNot)

  private def getRedshiftIncGameOutcomeCreatedSink(df: DataFrame): Sink =
    df.toInsertIfNotExistsSink(RedshiftIncGameOutcomeSink, incGameOutcomeTableName, uniqueIdsStatement = redshiftOutcomeMathCondition, isStaging = true, filterNot = filterNot)

  private def getRedshiftIncGameSessionCreatedSink(df: DataFrame): Sink =
    df.toInsertIfNotExistsSink(RedshiftIncGameSessionSink, incGameSessionTableName, uniqueIdsStatement = redshiftSessionMatchCondition, isStaging = true, filterNot = filterNot)

  private def getDeltaIncGameCreatedSink(df: DataFrame): Sink =
    df.withColumn(s"${FactIncGameEntity}_date_dw_id", col(s"${FactIncGameEntity}_date_dw_id").cast(StringType))
      .toInsertIfNotExists(matchConditions = deltaMatchCondition).toSink(DeltaIncGameSink, FactIncGameEntity)

  private def getDeltaIncGameUpdatedSink(df: DataFrame): Option[Sink] =
    df.toUpdate(matchConditions = deltaMatchCondition, updateColumns = deltaUpdateCols).map(_.toSink(DeltaIncGameSink, FactIncGameEntity))

  private def preprocessDeltaIncGame(fn: DataFrame => DataFrame)(srcDf: DataFrame): DataFrame = {
    val f = addUpdatedTimestampColumns(s"${FactIncGameEntity}_") _ compose fn

    preprocessDeltaForFact(srcDf, FactIncGameEntity, IncGameDeltaCols, f).drop(typeField)
  }

  private def filterByType(tp: Int)(df: DataFrame): DataFrame = df.filter($"$typeField" === tp)

  private def getDeltaIncGameOutcomeSink(srcDf: DataFrame): Sink =
    preprocessDeltaForFact(srcDf, FactIncGameOutcomeEntity, IncGameOutcomeDeltaCols)
      .withColumn(s"${FactIncGameOutcomeEntity}_date_dw_id", col(s"${FactIncGameOutcomeEntity}_date_dw_id").cast(StringType))
      .toInsertIfNotExists(matchConditions = deltaOutcomeMatchCondition)
      .toSink(DeltaIncGameOutcomeSink, FactIncGameOutcomeEntity)

  private def addReqColsForIncGameOutcome(incGameOutcomeSource: DataFrame): DataFrame =
    incGameOutcomeSource
      .withColumn("occurredOn", regexp_replace($"occurredOn", "T", " "))
      .withColumn("isFormativeAssessment", when($"isFormativeAssessment".isNotNull, $"isFormativeAssessment").otherwise(false))
      .withColumn(
        "status",
        when($"status" === IncGameOutcomeCompletedStatus, IncGameOutcomeCompletedStatusVal)
          .when($"status" === IncGameOutcomeSessionCancelled, IncGameOutcomeSessionCancelledStatusVal)
          .when($"status" === IncGameOutcomeSessionLeft, IncGameOutcomeSessionLeftStatusVal)
          .otherwise(IncGameOutcomeUndefinedStatusVal)
      ).appendFactDateColumns(FactIncGameOutcomeEntity, isFact = true)
      .withColumn(s"${FactIncGameOutcomeEntity}_date_dw_id", col(s"${FactIncGameOutcomeEntity}_date_dw_id").cast(IntegerType))

  private def addReqColsForIncGameSession(incGameSessionSource: DataFrame): DataFrame = {
    incGameSessionSource
      .withColumn("occurredOn", regexp_replace($"occurredOn", "T", " "))
      .withColumn("numPlayers", size($"players"))
      .withColumn("isFormativeAssessment", when($"isFormativeAssessment".isNotNull, $"isFormativeAssessment").otherwise(false))
      .transform(addNumJoinedPlayers)
      .withColumn("isStartProcessed", lit(false))
      .withColumn(
        "status",
        when($"status" === IncGameSessionStartedStatus, IncGameSessionStartedStatusVal)
          .when($"status" === IncGameSessionCompletedStatus, IncGameSessionCompletedStatusVal)
          .when($"status" === IncGameSessionCancelledStatus, IncGameSessionCancelledStatusVal)
          .when($"status" === IncGameSessionCreatedStatus, IncGameSessionCreatedStatusVal)
          .otherwise(IncGameSessionUndefinedStatusVal)
      )
      .withColumn("isStart",
                  when($"status" isin (1, 4), lit(true))
                    .otherwise(lit(false)))
      .appendFactDateColumns(FactIncGameSessionEntity, isFact = true)
      .withColumn(s"${FactIncGameSessionEntity}_date_dw_id", col(s"${FactIncGameSessionEntity}_date_dw_id").cast(IntegerType))
  }

  private def addNumJoinedPlayers(df: DataFrame): DataFrame = {
    val numJoinedPlayersCol = if (df.columns.contains("joinedPlayersDetails")) {
      size(col("joinedPlayersDetails"))
    } else {
      lit(ZeroArraySize)
    }

    df.withColumn("numJoinedPlayers", numJoinedPlayersCol)
  }

  private def addReqColsForIncGame(incGameSource: DataFrame): DataFrame = {
    val incGameNumQuestions: DataFrame = addNumQuestions(incGameSource)

    incGameNumQuestions
      .withColumn("isFormativeAssessment", when($"isFormativeAssessment".isNotNull, $"isFormativeAssessment").otherwise(false))
      .withColumn("occurredOn", regexp_replace($"occurredOn", "T", " "))
      .withColumn(
        "eventType",
        when($"eventType" === IncGameCreated, IncGameCreatedEventType)
          .when($"eventType" === IncGameUpdated, IncGameUpdatedEventType)
          .otherwise(IncGameUndefinedEventType)
      ).appendFactDateColumns(FactIncGameEntity, isFact = true)
      .withColumn(s"${FactIncGameEntity}_date_dw_id", col(s"${FactIncGameEntity}_date_dw_id").cast(IntegerType))

  }
}

object IncGame {
  def main(args: Array[String]): Unit = {
    new IncGame(incGameName, SparkSessionUtils.getSession(incGameName)).run
  }
}
