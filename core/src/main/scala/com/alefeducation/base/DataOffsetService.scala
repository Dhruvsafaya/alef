package com.alefeducation.base

import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.io.data.DeltaIO
import com.alefeducation.listener.Holder.log
import com.alefeducation.schema.internal.DataOffsetTable.{DataOffsetTable, offsetColumnName}
import com.alefeducation.util.DateTimeProvider
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, functions => F}

import java.sql.Timestamp

class DataOffsetService {

  import com.alefeducation.schema.internal.ControlTableUtils._

  var offsetTable: DeltaTable = _
  var dateTimeProvider: DateTimeProvider = _
  var deltaIO: DeltaIO = _

  val defaultTimestamp: Timestamp = Timestamp.valueOf("1970-01-01 00:00:00")

  def init(offsetTable: DeltaTable, dateTimeProvider: DateTimeProvider, deltaIO: DeltaIO): Unit = {
    this.offsetTable = offsetTable
    this.dateTimeProvider = dateTimeProvider
    this.deltaIO = deltaIO
  }

  def getOffset(tableName: String): Timestamp = {
    val tableOffsetRecord: DataFrame = offsetTable.toDF.filter(F.col("table_name") === tableName)
    if(tableOffsetRecord.isEmpty) {
      upsertDataOffset(tableName, Started, Some(defaultTimestamp))
      return defaultTimestamp
    }

    val offset: Timestamp = tableOffsetRecord.collect()(0).getAs[Timestamp](offsetColumnName)
    upsertDataOffset(tableName, Started)
    offset
  }

  def complete(tableName: String, dataFrame: DataFrame, offsetColumnName: String): Unit = {
    if(dataFrame.isEmpty) {
      log.warn(s"No data processed for the table - $tableName")
      upsertDataOffset(tableName, Completed)
      return
    }

    val maxDwCreatedTime: Timestamp = dataFrame.agg(F.max(offsetColumnName)).collect()(0)(0).asInstanceOf[Timestamp]
    upsertDataOffset(tableName, Completed, Some(maxDwCreatedTime))
  }

  def upsertDataOffset(tableName: String, status: CSStatus, offset: Option[Timestamp] = None): Unit = {
    val matchConditions: String = s"${Alias.Delta}.table_name = ${Alias.Events}.table_name AND ${Alias.Delta}.table_name = '$tableName'"

    val requiredColumns: Map[String, String] = Map(
      "table_name" -> s"${Alias.Events}.table_name",
      "status" -> s"${Alias.Events}.status",
      "updated_time" -> s"${Alias.Events}.updated_time",
    )
    val columnsToUpsert: Map[String, String] = if(offset.isEmpty) {
      requiredColumns
    } else {
      requiredColumns ++ Map(offsetColumnName -> s"${Alias.Events}.$offsetColumnName")
    }

    val newRecord: DataFrame = createDataOffsetRecord(tableName, status, offset)
    deltaIO.upsert(offsetTable, newRecord, matchConditions, columnsToUpsert, columnsToUpsert)
  }

  def createDataOffsetRecord(tableName: String, status: CSStatus, offset: Option[Timestamp]): DataFrame = {
    val df = offsetTable.toDF
    import df.sparkSession.implicits._

    val dataOffsetRecord = Seq(DataOffsetTable(tableName, status.toString, offset)).toDF
    dataOffsetRecord.withColumn("updated_time", dateTimeProvider.currentUtcTimestamp())
  }
}
