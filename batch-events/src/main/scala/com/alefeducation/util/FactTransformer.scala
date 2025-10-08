package com.alefeducation.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class FactTransformer(columnPrefix: String, dateTimeProvider: DateTimeProvider = new DateTimeProvider) {
  private val occurredOnColumn = "occurredOn"

  private def appendEventDateColumn(df: DataFrame): DataFrame =
    df.withColumn("eventdate", date_format(col(occurredOnColumn), "yyyy-MM-dd"))

  private def appendCreatedTimeColumns(df: DataFrame): DataFrame = {
    df
      .withColumn(s"${columnPrefix}_created_time", dateTimeProvider.toUtcTimestamp(occurredOnColumn))
      .withColumn(s"${columnPrefix}_dw_created_time", dateTimeProvider.currentUtcTimestamp())
  }

  private def appendDateDwIdColumns(df: DataFrame): DataFrame = {
    df.withColumn(s"${columnPrefix}_date_dw_id", date_format(col(occurredOnColumn), "yyyyMMdd"))
  }

  def appendGeneratedColumns(dataFrame: DataFrame): DataFrame = {
    require(dataFrame.columns.contains(occurredOnColumn), s"Column `$occurredOnColumn` is required to derive platform generated columns.")
    val transformationsToApply: Seq[DataFrame => DataFrame] = Seq(
      appendDateDwIdColumns,
      appendCreatedTimeColumns,
      appendEventDateColumn
    )

    transformationsToApply
      .foldLeft(dataFrame)((df, transformer) => transformer(df))
  }
}
