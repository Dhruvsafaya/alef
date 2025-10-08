package com.alefeducation.readers

import org.apache.spark.sql.{DataFrame, SparkSession}

trait IDataFrameReader {
  def read(path: String): DataFrame
}

trait IStreamReader {
  def read: DataFrame
}

class KafkaBasicReader(options: Map[String, String])(implicit spark: SparkSession) extends IStreamReader {

  override def read: DataFrame =
    spark.readStream
      .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .options(options)
      .load

}