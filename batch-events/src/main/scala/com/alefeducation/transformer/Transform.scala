package com.alefeducation.transformer

import org.apache.spark.sql.DataFrame

trait Transform {

  def transform(data: DataFrame, startId: Long): DataFrame

  def transform(data: Map[String, DataFrame], startId: Long): DataFrame
}
