package com.alefeducation.bigdata.batch.delta

import com.alefeducation.io.data.{Delta => DeltaIO}
import com.alefeducation.util.Constants.{ActiveState, HistoryState}
import com.alefeducation.util.DataFrameUtility.getEntityPrefix
import com.alefeducation.util.Resources
import org.apache.spark.sql.{DataFrame, SparkSession}

object DeltaSCDTypeII {
  
  def buildMatchConditions(entity: String, matchConditions: String = "", activeState: Int = ActiveState): String = {
    val entityPrefix = getEntityPrefix(entity)
    s"""
       |${getPrimaryStatement(entity, matchConditions, activeState)}
       | AND
       |${Alias.Delta}.${entityPrefix}created_time < ${Alias.Events}.${entityPrefix}created_time
       """.stripMargin
  }

  def buildInsertConditions(entity: String, matchConditions: String = "", activeState: Int = ActiveState): String = {
    val entityPrefix = getEntityPrefix(entity)
    s"""
       |${getPrimaryStatement(entity, matchConditions, activeState)}
       | AND
       |${Alias.Delta}.${entityPrefix}created_time = ${Alias.Events}.${entityPrefix}created_time
       """.stripMargin
  }

  def getPrimaryStatement(entity: String, matchConditions: String, activeState: Int): String = {
    val entityPrefix = getEntityPrefix(entity)
    val primaryConditions = getPrimaryConditions(entity, matchConditions)
    s"""
       |$primaryConditions
       | AND
       |${Alias.Delta}.${entityPrefix}status = $activeState
       |""".stripMargin
  }

  def getPrimaryConditions(entity: String, matchConditions: String): String = {
    val entityPrefix = getEntityPrefix(entity)
    if (matchConditions.isEmpty) s"${Alias.Delta}.${entityPrefix}id = ${Alias.Events}.${entityPrefix}id"
    else matchConditions
  }

  def buildUpdateColumns(entity: String, inactiveStatus: Int = HistoryState): Map[String, String] = {
    val entityPrefix = getEntityPrefix(entity)
    Map(
      s"${entityPrefix}status" -> s"$inactiveStatus",
      s"${entityPrefix}active_until" -> s"${Alias.Events}.${entityPrefix}created_time"
    )
  }

  def writeData(spark: SparkSession,
                df: DataFrame,
                resource: Resources.Resource,
                matchConditions: String,
                updateFields: Map[String, String],
                uniqueIdColumns: List[String],
                insertConditionColumns: String
               ): Unit = {
    val deltaTable = DeltaIO.getOrCreateDeltaTable(spark, resource.props("path"), df)

    deltaTable
      .as(Alias.Delta)
      .merge(df.as(Alias.Events).dropDuplicates(uniqueIdColumns), matchConditions)
      .whenMatched()
      .updateExpr(updateFields)
      .execute()

    val deltaTableColumns = deltaTable.toDF.columns.toSet
    val eventsColumns = df.columns.toSet

    val insertExprMap = deltaTableColumns.intersect(eventsColumns).map(col => col -> s"${Alias.Events}.$col").toMap

    deltaTable
      .as(Alias.Delta)
      .merge(df.as(Alias.Events), insertConditionColumns)
      .whenNotMatched()
      .insertExpr(insertExprMap)
      .execute()
  }
}

class DeltaSCDTypeIISink(sinkName: String,
                         df: DataFrame,
                         matchConditions: String,
                         updateFields: Map[String, String],
                         uniqueIdColumns: List[String],
                         insertConditionColumns: String
                        )
  extends DeltaUpdateSink(df.sparkSession, sinkName, df, df, matchConditions, updateFields, "")
    with DeltaMergeSink
    with DeltaWrite {

  import DeltaSCDTypeII._

  override def writerOptions: Map[String, String] = Map()

  override def write(resource: Resources.Resource): Unit = {
    writeData(spark, df, resource, matchConditions, updateFields, uniqueIdColumns, insertConditionColumns)
  }
}
