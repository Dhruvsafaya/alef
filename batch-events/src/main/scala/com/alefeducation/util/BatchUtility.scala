package com.alefeducation.util

import com.alefeducation.service.DataSink
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.functions.{col, date_format, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

@deprecated("Use BatchTransformerUtility instead")
object BatchUtility {

  def transformDataFrame(session: SparkSession,
                         dataFrame: DataFrame,
                         events: List[String],
                         columnMapping: Map[String, String],
                         entity: String,
                         sink: String,
                         source: String,
                         rawDf: Option[DataFrame] = None,
                         latestByIds: List[String] = List("uuid"),
                         eventDateColumn: String = "occurredOn"): List[DataSink] = {
    val dataSink: List[DataSink] = getRedshiftSinks(session, dataFrame, events, columnMapping, entity, sink, latestByIds, eventDateColumn)
    (if (!isEmpty(dataFrame)) List(getParquetSink(session, source, rawDf.getOrElse(dataFrame))) else Nil) ++ dataSink
  }

  def getRedshiftSinks(session: SparkSession,
                       dataFrame: DataFrame,
                       events: List[String],
                       columnMapping: Map[String, String],
                       entity: String,
                       sink: String,
                       latestByIds: List[String] = List("uuid"),
                       eventDateColumn: String = "occurredOn",
                       ids: List[String] = Nil): List[DataSink] = {
    import session.implicits._

    val uniqueIds = if (ids.isEmpty) List(s"${entity}_id") else ids

    events.flatMap {
      case eventType if !isEmpty(dataFrame) && !isEmpty(dataFrame.filter($"eventType" === eventType)) =>
        val df = dataFrame.filter($"eventType" === eventType)
        handleEventOperations(session, df, columnMapping, eventType, entity, sink, uniqueIds, latestByIds, eventDateColumn)
      case _ => Nil
    }
  }

  @Deprecated
  def getParquetSink(session: SparkSession, sinkName: String, dataFrame: DataFrame): DataSink = {
    import session.implicits._
    DataSink(sinkName,
             dataFrame.withColumn("eventdate", date_format($"occurredOn", "yyyy-MM-dd")).coalesce(1),
             Map("partitionBy" -> "eventdate"))
  }

  def getParquetSinks(session: SparkSession, sinkName: String, dataFrame: DataFrame): List[DataSink] =
    if (dataFrame.isEmpty) Nil
    else List(getParquetSink(session, sinkName, dataFrame))

  def handleEventOperations(session: SparkSession,
                            dataFrame: DataFrame,
                            columnMapping: Map[String, String],
                            eventType: String,
                            entity: String,
                            sink: String,
                            uniqueIds: List[String],
                            latestByIds: List[String],
                            eventDateColumn: String = "occurredOn"): List[DataSink] = {
    import session.implicits._
    getOperations(eventType) match {
      case Insert =>
        val sdf = addStatusColumn(dataFrame, entity)
        val dataFrameWithTimeColumns = selectedDfWithTimeColumns(session, sdf, columnMapping, entity)
        List(DataSink(sink, dataFrameWithTimeColumns))
      case Update =>
        val sdf = addStatusColumn(dataFrame, entity)
        val latestUpdated = selectLatestUpdatedRecords(sdf, eventType, latestByIds, eventDateColumn)
        val dataFrameWithTimeColumns = selectedDfWithTimeColumns(session, latestUpdated, columnMapping, entity)
        val updateOptions = setUpdateOptions(dataFrameWithTimeColumns, entity, uniqueIds)
        List(DataSink(sink, dataFrameWithTimeColumns, updateOptions))
      case Delete =>
        val sdf = addStatusColumn(dataFrame, entity)
        val dataFrameWithTimeColumns = selectedDfWithTimeColumns(session, sdf, columnMapping, entity)
          .withColumn(s"${entity}_deleted_time", $"${entity}_created_time")
        val updateOptions =
          setUpdateOptions(dataFrameWithTimeColumns, entity, uniqueIds)
        List(DataSink(sink, dataFrameWithTimeColumns, updateOptions))
      case InsertWithHistory =>
        val dataFrameWithTimeColumns = addColsForIWH(columnMapping, entity)(dataFrame)
        val insertWithHistoryOptions = setInsertWithHistoryOptions(dataFrameWithTimeColumns, latestByIds, entity)
        List(DataSink(sink, dataFrameWithTimeColumns, insertWithHistoryOptions))
      case _ => Nil
    }
  }

  def addColsForIWH(columnMapping: Map[String, String], entity: String)(df: DataFrame): DataFrame = {
    val columnMappingWithUpdatedTime = addUpdateTimeMappingColumn(columnMapping, entity)
    val selectedDf = selectAs(df, columnMappingWithUpdatedTime)
    addCreatedTimestampColumns(selectedDf, entity)
  }

  def addUpdateTimeMappingColumn(columnMapping: Map[String, String], entity: String): Map[String, String] = {
    columnMapping + (s"${entity}_updated_time" -> s"${entity}_updated_time") +
      (s"${entity}_dw_updated_time" -> s"${entity}_dw_updated_time")
  }

  def selectedDfWithTimeColumns(session: SparkSession,
                                dataFrame: DataFrame,
                                columnMapping: Map[String, String],
                                entity: String): DataFrame = {
    val selectedDf = selectAs(dataFrame, columnMapping)
    addTimestampColumns(selectedDf, entity)
  }

  def addStatusColumn(df: DataFrame, entity: String = ""): DataFrame = {
    val entityPrefix =  getEntityPrefix(entity)
    val statusCol = entityPrefix match {
      case ""        => "status"
      case x: String => s"${x}status"
    }

    df.withColumn(
      statusCol,
      when(col("eventType").contains("Disabled"), lit(Disabled))
        .otherwise(when(col("eventType").contains("Deleted"), lit(Deleted)).otherwise(ActiveEnabled))
    )
  }
}
