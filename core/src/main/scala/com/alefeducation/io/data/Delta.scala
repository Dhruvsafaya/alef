package com.alefeducation.io.data

import com.alefeducation.bigdata.batch.delta.Alias
import io.delta.tables.{DeltaMergeBuilder, DeltaMergeMatchedActionBuilder, DeltaTable}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DeltaIO {
  def upsert(deltaTable: DeltaTable,
             dataFrame: DataFrame,
             matchConditions: String,
             columnsToUpdate: Map[String, String],
             columnsToInsert: Map[String, String],
             updateConditions: Option[String] = None): Unit

  def update(deltaTable: DeltaTable, matchConditions: String, columnsToUpdate: Map[String, String]): Unit

  def isTableExists(session: SparkSession, tablePath: String): Boolean
  def createEmptyTable(output: DataFrame, tablePath: String, partitionBy: Seq[String]): Unit
  def getDeltaTable(session: SparkSession, tablePath: String): DeltaTable
}

object Delta extends DeltaIO with Logging {
  import DeltaIOImplicits.DeltaMergeBuilderOperations

  def upsert(deltaTable: DeltaTable,
             dataFrame: DataFrame,
             matchConditions: String,
             columnsToUpdate: Map[String, String],
             columnsToInsert: Map[String, String],
             updateConditions: Option[String]): Unit = {
    deltaTable
      .as(Alias.Delta)
      .merge(dataFrame.as(Alias.Events), matchConditions)
      .whenMatched(updateConditions)
      .updateExpr(columnsToUpdate)
      .whenNotMatched
      .insertExpr(columnsToInsert)
      .execute()
  }

  def update(deltaTable: DeltaTable, matchConditions: String, columnsToUpdate: Map[String, String]): Unit = {
    deltaTable.updateExpr(matchConditions, columnsToUpdate)
  }

  /**
    *  Checks whether the delta table exists on the given path.
    *
    * @param session - spark session
    * @param tablePath - path to the Delta table
    * @return table existence flag
    */
  def isTableExists(session: SparkSession, tablePath: String): Boolean = DeltaTable.isDeltaTable(session, tablePath)

  /**
    * Create an empty Delta table based on output dataframe
    *
    * @param output - output dataframe
    * @param tablePath - path to the Delta table
    * @param partitionBy - columns to partition by
    */
  def createEmptyTable(output: DataFrame, tablePath: String, partitionBy: Seq[String]): Unit = {
    log.info("Attempting to create an empty Delta table")

    val writer = output.limit(0).write.format("delta")

    if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*).save(tablePath)
    } else {
      writer.save(tablePath)
    }
  }

  /**
    * This function returns the Delta table by reading it from the given path.
    *
    * @param session - spark session
    * @param tablePath - path to the table
    * @return DeltaTable - Delta table object
    */
  def getDeltaTable(session: SparkSession, tablePath: String): DeltaTable = DeltaTable.forPath(session, tablePath)

  def getOrCreateDeltaTable(session: SparkSession, tablePath: String, df: DataFrame): DeltaTable = {
    if(!isTableExists(session, tablePath)){
      createEmptyTable(df, tablePath, Seq.empty)
    }

    getDeltaTable(session, tablePath)
  }

}

private object DeltaIOImplicits {
  implicit class DeltaMergeBuilderOperations(deltaMergeBuilder: DeltaMergeBuilder) {
    def whenMatched(condition: Option[String]): DeltaMergeMatchedActionBuilder = condition match {
      case Some(cond) => deltaMergeBuilder.whenMatched(cond)
      case _          => deltaMergeBuilder.whenMatched
    }
  }
}
