package com.alefeducation.bigdata

import com.alefeducation.schema.internal.ControlTableUtils.UpdateType
import org.apache.spark.sql.DataFrame

trait Sink {

  def eventType: String = ""

  def name: String

  def input: DataFrame

  def output: DataFrame = input

  def controlTableForUpdate(): Map[UpdateType, String] = Map.empty
}
