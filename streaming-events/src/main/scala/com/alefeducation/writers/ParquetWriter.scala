package com.alefeducation.writers

import org.apache.spark.sql.{DataFrame, SaveMode}

class ParquetWriter(val basePath: String) extends IDataFrameWriter {
  override def insert(df: DataFrame, partitionBy: Seq[String], options: Map[String, String])(path: String): Unit =
    df.write.options(options).mode(SaveMode.Append).parquet(basePath + path)

  override def update(df: DataFrame)(path: String, condition: String): Unit =
    throw new UnsupportedOperationException("update parquet")

  override def upsert(df: DataFrame)(path: String, condition: String): Unit =
    throw new UnsupportedOperationException("upsert parquet")

  override def delete(df: DataFrame)(path: String, condition: String): Unit =
    throw new UnsupportedOperationException("delete parquet")
}
