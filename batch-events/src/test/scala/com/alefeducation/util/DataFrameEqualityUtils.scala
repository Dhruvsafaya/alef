package com.alefeducation.util

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col

object DataFrameEqualityUtils {

  def assertSmallDatasetEquality(entity: String, actualDf: DataFrame, expectedDf: DataFrame): Unit = {
    val actualNameCols = actualDf.columns.toSet
    val expectedNameCols = expectedDf.columns.toSet
    assert(actualNameCols == expectedNameCols, errorMsgForColDiff(actualNameCols, expectedNameCols))
    val actualSortedDf = defaultSortDataset(actualDf).drop(s"${entity}_dw_created_time")
    val expectedSortedDf = defaultSortDataset(expectedDf).drop(s"${entity}_dw_created_time")

    val actualArr = actualSortedDf.collect()
    val expectedArr = expectedSortedDf.collect()
    assert(actualArr.sameElements(expectedArr), errorMsgForValDiff(actualArr, expectedArr))
  }

  def assertSmallDatasetEquality(spark: SparkSession, entity: String, actualDf: DataFrame, expectedJson: String): Unit = {
    assertSmallDatasetEquality(entity, actualDf, createDfFromJson(spark, expectedJson))
  }

  def createDfFromJson(spark: SparkSession, json: String, schema: Option[String] = None): DataFrame = {
    import spark.implicits._

    val dfReader = spark.read
      .option("multiline", "true")
      .option("inferTimestamp", "false")
    val dfReaderWithSchema = schema match {
      case Some(s) => dfReader.schema(s)
      case None => dfReader
    }
    dfReaderWithSchema.json(Seq(json).toDS)
  }

  private def errorMsgForColDiff(actual: Set[String], expected: Set[String]): String = {
    val actualHasMoreColumns = (actual diff expected).mkString(", ")
    val expectedHasMoreColumns = (expected diff actual).mkString(", ")

    val actualMsg = if (actualHasMoreColumns.nonEmpty) s"actual DF has more columns: [$actualHasMoreColumns]\n" else ""
    val expectedMsg = if (expectedHasMoreColumns.nonEmpty) s"expected DF has more columns: [$expectedHasMoreColumns]" else ""

    actualMsg + expectedMsg
  }

  private def errorMsgForValDiff(actual: Array[Row], expected: Array[Row]): String = {
    actual.zip(expected).collect {
      case (actualRow, expectedRow) if actualRow != expectedRow =>
        s"rows: $actualRow != $expectedRow"
    }.mkString("\n", "\n", "\n")
  }

  private def defaultSortDataset(df: DataFrame): DataFrame = {
    val colNames = df.columns.sorted
    val cols     = colNames.map(col)
    df.select(cols: _*).sort(cols: _*)
  }
}