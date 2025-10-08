package com.alefeducation.bigdata.batch.delta

import com.alefeducation.util.Constants.{ActiveState, Detached}
import com.alefeducation.util.Resources
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.alefeducation.io.data.{Delta => DeltaIO}

@deprecated("user DeltaSCDTypeII")
object DeltaIWH {

  def buildMatchColumns(entity: String, matchConditions: String = "", activeState: Int = ActiveState, inactiveStatus: Int = Detached, isActiveUntilVersion: Boolean = false): String = {
    val primaryConditions = DeltaSCDTypeII.getPrimaryConditions(entity, matchConditions)

    if (isActiveUntilVersion) {
      s"""
         | $primaryConditions
         | AND
         |${Alias.Delta}.${entity}_status = $activeState
         | AND
         | ${Alias.Delta}.${entity}_created_time < ${Alias.Events}.${entity}_created_time
      """.stripMargin
    } else {
      s"""
         | $primaryConditions
         | AND
         |${Alias.Delta}.${entity}_status != $inactiveStatus
         | AND
         | ${Alias.Delta}.${entity}_created_time < ${Alias.Events}.${entity}_created_time
      """.stripMargin
    }
  }

  def buildInsertConditions(entity: String, matchConditions: String = "", activeState: Int = ActiveState): String = {
    s"""
       |${DeltaSCDTypeII.getPrimaryConditions(entity, matchConditions)}
       | AND
       |${Alias.Delta}.${entity}_status = $activeState
       | AND
       |${Alias.Delta}.${entity}_created_time = ${Alias.Events}.${entity}_created_time
       """.stripMargin
  }

  def buildUpdateColumns(entity: String, inactiveStatus: Int = Detached, isActiveUntilVersion: Boolean = false): Map[String, String] = {
    if (isActiveUntilVersion) {
      Map(
        s"${entity}_status" -> s"$inactiveStatus",
        s"${entity}_active_until" -> s"${Alias.Events}.${entity}_created_time"
      )
    } else {
      Map(
        s"${entity}_status" -> s"$inactiveStatus",
        s"${entity}_updated_time" -> s"${Alias.Events}.${entity}_created_time",
        s"${entity}_dw_updated_time" -> "current_timestamp"
      )
    }
  }

}

/**
  * IWH - insert with history
  * @param sparkSession
  * @param sinkName
  * @param df
  * @param input
  * @param matchConditions
  * @param updateFields
  */
@deprecated("use DeltaSCDTypeII")
class DeltaIwhSink(sparkSession: SparkSession,
                   sinkName: String,
                   df: DataFrame,
                   input: DataFrame,
                   matchConditions: String,
                   updateFields: Map[String, String],
                   uniqueIdColumns: List[String],
                   insertConditionColumns: String,
                   eventType: String = "")
    extends DeltaUpdateSink(sparkSession, sinkName, df, input, matchConditions, updateFields, eventType)
    with DeltaMergeSink
    with DeltaWrite {

  override def writerOptions: Map[String, String] = Map()

  override def write(resource: Resources.Resource): Unit = {
    DeltaSCDTypeII.writeData(spark, df, resource, matchConditions, updateFields, uniqueIdColumns, insertConditionColumns)
  }
}
