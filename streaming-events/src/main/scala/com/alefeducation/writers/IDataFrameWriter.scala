package com.alefeducation.writers

import org.apache.spark.sql.DataFrame

trait IDataFrameWriter {
  def insert(df: DataFrame, partitionBy: Seq[String] = Seq.empty, options: Map[String, String] = Map.empty)(path: String): Unit

  def update(df: DataFrame)(path: String, condition: String): Unit

  def upsert(df: DataFrame)(path: String, condition: String): Unit

  def delete(df: DataFrame)(path: String, condition: String): Unit
}
