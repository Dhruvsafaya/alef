package com.alefeducation.base

import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.io.data.DeltaIO
import com.alefeducation.schema.internal.ControlTable.ControlTable
import com.alefeducation.util.DateTimeProvider
import io.delta.tables.DeltaTable
import com.alefeducation.listener.Holder.log
import org.apache.spark.sql.functions.col

class ControlTableService {

  import com.alefeducation.schema.internal.ControlTableUtils._

  var maxIdTable: DeltaTable = _
  var dateTimeProvider: DateTimeProvider = _
  var deltaIO: DeltaIO = _

  def init(maxIdTable: DeltaTable, dateTimeProvider: DateTimeProvider, deltaIO: DeltaIO): Unit = {
    this.maxIdTable = maxIdTable
    this.dateTimeProvider = dateTimeProvider
    this.deltaIO = deltaIO
  }

  /**
   * Processing status means that job did not execute completed stage and max_id is inconsistent and may cause to duplicates
   *    in next run. It requires manual investigation and update max_id value and for protect from duplicates
   *    it throws RuntimeException.
   * Starting status means that previous job started but not processed any rows and max_id has correct final value
   * Failure status means that job failed and next re-run should overwrite existing transform folder
   */
  def getStartIdUpdateStatus(tableName: String, failIfWrongState: Boolean = false): Long = {
    if (failIfWrongState) {
      val status = getLastValue[String](tableName, "status", Completed.toString)
      if (Processing.toString == status) {
        throw new RuntimeException(s"table_name = $tableName, status = $status is inconsistent, check product-max-id")
      }
    }
    val oldCount = getLastValue[Long](tableName, "max_id", 0L)
    upsertControlTable(tableName, oldCount, Started)
    val newCount = oldCount + 1
    log.info(s"getStartIdUpdateStatus(tableName=$tableName) => $newCount")
    newCount
  }

  def getLastValue[A](tableName: String, colName: String, defaultValue: A): A = {
    val arr = maxIdTable.toDF.filter(col("table_name") === tableName).select(colName).collect()
    if (arr.isEmpty) defaultValue else arr(0).getAs[A](0)
  }

  def upsertControlTable(tableName: String, oldCount: Long, status: CSStatus): Unit = {
    val df = maxIdTable.toDF
    import df.sparkSession.implicits._

    val newRow = Seq(ControlTable(tableName, oldCount, status.toString)).toDF
      .withColumn("updated_time", dateTimeProvider.currentUtcTimestamp())

    val matchConditions: String = s"${Alias.Delta}.table_name = ${Alias.Events}.table_name AND ${Alias.Delta}.table_name = '$tableName'"
    val columnsToUpsert: Map[String, String] = maxIdTable.toDF.columns.map(col => col -> s"${Alias.Events}.$col").toMap
    val updateConditions: String = s"${Alias.Delta}.status = '${Completed.toString}'"

    deltaIO.upsert(maxIdTable, newRow, matchConditions, columnsToUpsert, columnsToUpsert, Some(updateConditions))
  }

  def complete(tableName: String, count: Long): Unit = {
    val matchConditions: String = s"table_name = '$tableName'"
    val columnsToUpdate: Map[String, String] = Map(
      "max_id" -> s"max_id + $count",
      "status" -> s"'${Completed.toString}'",
      "updated_time" -> "current_timestamp"
    )

    deltaIO.update(maxIdTable, matchConditions, columnsToUpdate)
    log.info(s"complete(tableName=$tableName) add count=$count")
  }

  def updateStatus(tableName: String, status: CSStatus): Unit = {
    val matchConditions: String = s"table_name = '$tableName'"
    val columnsToUpdate: Map[String, String] = Map(
      "status" -> s"'${status.toString}'",
      "updated_time" -> "current_timestamp"
    )

    deltaIO.update(maxIdTable, matchConditions, columnsToUpdate)
    log.info(s"updateStatus(tableName=$tableName) updated status to ${status.toString}")
  }
}
