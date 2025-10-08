package com.alefeducation.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}

object BatchSessionUtility {

  def transformForDelta(entity: String,
                        ids: List[String],
                        endEventCols: List[String],
                        isEndEventGroupCondition: Column,
                        isSessionStartedCondition: Column,
                        consumeAllStartEvents: Boolean = false)(df: DataFrame): (DataFrame, DataFrame) = {

    val ndf = df
      .withColumn("is_end_event_group", when(isEndEventGroupCondition, true).otherwise(false))
      .withColumn("is_session_completed", sum(col("is_end_event_group").cast(IntegerType)) over Window.partitionBy(ids.map(col): _*))

    val (startEndEventsDf, startEventsDf) = splitDf(ndf, consumeAllStartEvents)

    val resStartEventsDf = startEventsDf
      .transform(calcTimesColsForStartEvents(entity))
      .transform(dropCols(entity))
      .transform(addDwCreatedTime(entity))

    val resStartEndEventsDf = startEndEventsDf
      .transform(calcTimeCols(entity, ids, endEventCols, isSessionStartedCondition))
      .transform(dropCols(entity))
      .transform(addDwCreatedTime(entity))

    (resStartEndEventsDf, resStartEventsDf)
  }

  def calcTimeCols(entity: String, ids: List[String], endEventCols: List[String], isSessionStartedCondition: Column)(
      df: DataFrame): DataFrame = {
    val idCols = ids.map(col)
    val sortCols = idCols :+ col(s"${entity}_created_time")
    val winCols = idCols :+ col("is_end_event_group")
    val firstCols = df.columns.filterNot(ids.contains).filterNot(endEventCols.contains).map(name => first(name).as(name)).toList
    val lastCols = endEventCols.map(name => last(name).as(name))
    val startTime = min(col(s"${entity}_created_time")).as(s"${entity}_start_time")
    val endTime = max(col(s"${entity}_created_time")).as(s"${entity}_end_time")
    val isTimeCalculable = first(col("is_time_calculable")).as("is_time_calculable")
    val aggCols = isTimeCalculable :: startTime :: endTime :: firstCols ++ lastCols

    val w = Window
      .partitionBy(winCols: _*)
      .orderBy(col(s"${entity}_created_time").desc)

    df.withColumn("rnk_start", dense_rank() over w)
      .filter(col("rnk_start") === 1)
      .withColumn("is_session_started", when(isSessionStartedCondition, 1).otherwise(0))
      .withColumn("is_time_calculable", sum(col("is_session_started")) over Window.partitionBy(idCols: _*))
      .sort(sortCols: _*)
      .groupBy(idCols: _*)
      .agg(aggCols.head, aggCols.tail: _*)
      .withColumn(s"${entity}_time_spent", col(s"${entity}_end_time").cast(LongType) - col(s"${entity}_start_time").cast(LongType))
      .transform(fixCalcTime(List(s"${entity}_time_spent")))
  }

  def calcTimesColsForStartEvents(entity: String)(df: DataFrame): DataFrame =
    df.withColumn(s"${entity}_start_time", col(s"${entity}_created_time"))
      .withColumn(s"${entity}_end_time", lit(null))
      .withColumn(s"${entity}_time_spent", lit(null))

  def splitDf(df: DataFrame, consumeAllStartEvents: Boolean = false): (DataFrame, DataFrame) = {
    val filterCol = col("is_session_completed")

    val startEndEventsFilterCondition = filterCol === 1

    val startEventsFilterCondition = if (consumeAllStartEvents) {
      col("practice_session_is_start") === true
    } else filterCol === 0

    df.filter(startEndEventsFilterCondition) -> df.filter(startEventsFilterCondition)
  }

  def fixCalcTime(endEventCols: List[String])(df: DataFrame): DataFrame =
    endEventCols match {
      case Nil => df
      case x :: xs =>
        df.withColumn(x, when(col("is_time_calculable") === 1, col(x)).otherwise(null))
          .transform(fixCalcTime(xs))
    }

  def dropCols(entity: String)(df: DataFrame): DataFrame =
    df.drop("is_time_calculable",
            "is_session_started",
            "rnk_start",
            "is_session_completed",
            "is_end_event_group",
            s"${entity}_created_time")

  def addDwCreatedTime(entity: String)(df: DataFrame): DataFrame =
    df.withColumn(s"${entity}_dw_created_time", current_timestamp())

  def unionStartEndSessions(startSessions: Option[DataFrame], endSessions: Option[DataFrame]): Option[DataFrame] =
    (startSessions, endSessions) match {
      case (None, None)           => None
      case (Some(sdf), Some(edf)) => Some(sdf.unionByName(edf))
      case (odf, None)            => odf
      case (None, odf)            => odf
    }

  def filterOutProcessedEvents(name: String)(df: DataFrame): DataFrame =
    df.filter(col(name) === false)

  def filterOutProcessedEventsOpt(name: String)(df: DataFrame): Option[DataFrame] = {
    val ndf = filterOutProcessedEvents(name)(df)
    if (ndf.isEmpty) None
    else Some(ndf)
  }

  def addIsStartEventProcDefCol(name: String)(df: DataFrame): DataFrame =
    if (df.columns.contains(name)) df
    else df.withColumn(name, lit(false))

  def addCol(name: String)(df: DataFrame): DataFrame =
    if (df.columns.contains(name)) df
    else df.withColumn(name, lit(null))

  def filterByEventType(eventType: String)(df: DataFrame): DataFrame =
    df.filter(col("eventType") === eventType)

  def filterByEventTypeOpt(eventType: String)(df: DataFrame): Option[DataFrame] = {
    val ndf = df.filter(col("eventType") === eventType)
    if (ndf.isEmpty) None
    else Some(ndf)
  }

}
