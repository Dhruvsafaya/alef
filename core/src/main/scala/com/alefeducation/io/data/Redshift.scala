package com.alefeducation.io.data

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SparkSession}

object RedshiftIO {

  val TempTableAlias: String = "temp_table"

  def createMergeQuery(targetTable: String,
                       sourceTable: String,
                       matchConditions: String,
                       columnsToUpdate: Map[String, String],
                       columnsToInsert: Map[String, String]): String = {
    val updateStatements: String = columnsToUpdate.map {
      case (targetColumn, sourceColumn) => s"$targetColumn = $sourceColumn"
    }.mkString(", ")

    val insertColumnsStatements: String = columnsToInsert.keys.mkString(", ")
    val insertValuesStatements: String = columnsToInsert.values.mkString(", ")

    s"""
       |MERGE INTO $targetTable
       |USING $sourceTable AS $TempTableAlias
       |ON ($matchConditions)
       |WHEN MATCHED THEN UPDATE SET $updateStatements
       |WHEN NOT MATCHED THEN INSERT ($insertColumnsStatements) VALUES ($insertValuesStatements);
       |""".stripMargin
  }
}

class Redshift(spark: SparkSession, jdbcUrl: String, tmpFileS3Location: String) {

  private val RedshiftFormat = "io.github.spark_redshift_community.spark.redshift"

  private val commonRedshiftOptions = Map(
    "url" -> jdbcUrl,
    "tempdir" -> tmpFileS3Location,
    "forward_spark_s3_credentials" -> "true",
    "autoenablessl" -> "false"
  )

  private def redshiftReader: DataFrameReader = commonRedshiftOptions
    .foldLeft(spark.read.format(RedshiftFormat)) {
      case (reader, (property, value)) => reader.option(property, value)
    }

  private def redshiftWriter(df: DataFrame): DataFrameWriter[Row] = commonRedshiftOptions
    .foldLeft(df.write.format(RedshiftFormat)) {
      case (writer, (property, value)) => writer.option(property, value)
    }

  def read(query: String): DataFrame = {
    redshiftReader
      .option("query", query)
      .load()
  }

  def readTable(tableName: String): DataFrame = {
    redshiftReader
      .option("dbtable", tableName)
      .load()
  }

  def insert(df: DataFrame, tableName: String, mode: String = "append"): Unit = {
    redshiftWriter(df)
      .option("dbtable", tableName)
      .mode(mode)
      .save()
  }
}
