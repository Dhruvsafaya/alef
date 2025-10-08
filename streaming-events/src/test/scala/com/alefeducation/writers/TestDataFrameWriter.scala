package com.alefeducation.writers

import org.apache.spark.sql.DataFrame

class TestDataFrameWriter extends IDataFrameWriter {
  override def insert(df: DataFrame, partitionBy: Seq[String] = Nil, options: Map[String, String] = Map.empty)(path: String): Unit =
    df.show(false)

  override def update(df: DataFrame)(path: String, condition: String): Unit = df.show(false)

  override def upsert(df: DataFrame)(path: String, condition: String): Unit = df.show(false)

  override def delete(df: DataFrame)(path: String, condition: String): Unit = df.show(false)
}
