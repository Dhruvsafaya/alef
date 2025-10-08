package com.alefeducation.writers

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class StagingParquetWriter(val basePath: String) extends IDataFrameWriter {
  override def insert(df: DataFrame, partitionBy: Seq[String] = Seq.empty, options: Map[String, String] = Map.empty)(path: String): Unit = {
    val fullPath = basePath + path
    df.write
      .options(options)
      .mode(SaveMode.Overwrite)
      .parquet(fullPath)
  }

  override def update(df: DataFrame)(path: String, condition: String): Unit =
    throw new UnsupportedOperationException("update parquet")

  override def upsert(df: DataFrame)(path: String, condition: String): Unit =
    throw new UnsupportedOperationException("upsert parquet")

  override def delete(df: DataFrame)(path: String, condition: String): Unit =
    throw new UnsupportedOperationException("delete parquet")
}
