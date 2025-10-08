package com.alefeducation.writers

import io.delta.tables.{DeltaMergeBuilder, DeltaTable}
import org.apache.spark.sql.{DataFrame, SaveMode}

class DeltaWriter(val basePath: String) extends IDataFrameWriter {

  override def insert(df: DataFrame, partitionBy: Seq[String], options: Map[String, String] = Map.empty)(path: String): Unit =
    df.write
      .format("delta")
      .partitionBy(partitionBy: _*)
      .options(options + ("mergeSchema" -> "true"))
      .mode(SaveMode.Append)
      .save(basePath + path)

  override def update(df: DataFrame)(path: String, condition: String): Unit =
    getMergeDF(df, path, condition)
      .whenMatched()
      .updateAll()
      .execute()

  override def upsert(df: DataFrame)(path: String, condition: String): Unit =
    getMergeDF(df, path, condition)
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()

  override def delete(df: DataFrame)(path: String, condition: String): Unit =
    DeltaTable
      .forPath(df.sparkSession, basePath + path)
      .delete(condition)

  def getMergeDF(df: DataFrame, path: String, condition: String): DeltaMergeBuilder =
    DeltaTable
      .forPath(df.sparkSession, basePath + path)
      .as("t")
      .merge(df.as("s"), condition)
}
