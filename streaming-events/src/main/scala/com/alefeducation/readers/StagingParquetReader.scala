package com.alefeducation.readers
import org.apache.spark.sql.{DataFrame, SparkSession}

class StagingParquetReader(val basePath: String, sparkSession: SparkSession) extends IDataFrameReader {

  override def read(path: String): DataFrame = sparkSession.read.parquet(basePath + path)

}