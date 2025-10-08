package com.alefeducation.util.generic_transform

import com.alefeducation.util.BatchTransformerUtility.RawDataFrameToTransformationAndSink
import com.alefeducation.util.DataFrameUtility.getUTCDateFromMillisOrSeconds
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, TimestampType}

import scala.collection.JavaConverters._
import scala.util.Try

object CustomTransformationsUtility {
  case class ColumnConfig(operation: String, columns: List[String], types: List[String] = Nil)

  /** *
   * Read transformations from config
   * operation: The type of operation to be applied like 'renmae', 'stringToTimestamp', 'explode', etc
   * columns: The name of columns involved in the transformation
   * target(optional): what a column should become - new name or new type depending on operation if applicable
   */

  def commonTransformationsConfig(customTransformations: Seq[Config]): List[ColumnConfig] =
    customTransformations.map { transConfig =>
      ColumnConfig(
        transConfig.getString("operation"),
        transConfig.getStringList("columns").asScala.toList,
        Try(transConfig.getStringList("target").asScala.toList).getOrElse(List.empty)
      )
    }.toList

  /**
   * Transforms specified columns based on the type of transformation in config
   *
   * @param df      Input DataFrame.
   * @param columns List of columns to be converted.
   * @return DataFrame with specified columns converted to timestamp.
   */
  def applyCustomTransformations(customTransformations: Seq[Config], df: DataFrame): DataFrame = {
    commonTransformationsConfig(customTransformations).foldLeft(df) { (currentDF, colConfig) =>
      colConfig.operation match {
        case "explode" =>
          colConfig.columns.foldLeft(currentDF) { (tempDf, colName) =>
            tempDf.withColumn(colName, explode_outer(col(colName)))
          }
        case "stringToTimestamp" => colConfig.columns.foldLeft(currentDF) { (tempDf, colName) =>
          tempDf.withColumn(colName, when(col(colName).isNotNull, col(colName)).cast(TimestampType))
        }
        case "epochToTimestamp" =>
          colConfig.columns.foldLeft(currentDF) { (tempDf, colName) =>
            tempDf.withColumn(colName, when(col(colName).isNotNull, getUTCDateFromMillisOrSeconds(col(colName)).cast(TimestampType)))
          }
        case "stringToDate" =>
          colConfig.columns.foldLeft(currentDF) { (tempDf, colName) =>
            tempDf.withColumn(colName, to_date(col(colName), "yyyy-MM-dd"))
          }
        case "rename" =>
          colConfig.columns.zip(colConfig.types).foldLeft(currentDF) { (tempDf, colName) =>
            tempDf.withColumnRenamed(colName._1, colName._2)
          }
        case "addColIfNotExists" =>
          colConfig.columns.zip(colConfig.types).foldLeft(currentDF) { (tempDf, colName) =>
            tempDf.addColIfNotExists(colName._1, DataType.fromDDL(colName._2))
          }
        case "dropDuplicates" =>
          currentDF.dropDuplicates()

        case "jsonStringToArrayStruct" =>
          //1. find a sample JSON to infer schema
          colConfig.columns.foldLeft(currentDF) { (tempDf, sourceCol) =>
            import df.sparkSession.implicits._
            val sampleJsonOption = tempDf
              .select(sourceCol)
              .filter(col(sourceCol).isNotNull) // Ignore null/empty
              .as[String]
              .take(1)
              .headOption

            // 2. Fallback if no sample found
            val sampleJson = sampleJsonOption.getOrElse("[{}]")

            // 3. Infer schema
            val inferredSchema = schema_of_json(lit(sampleJson))
            // 4. Parse and safely explode
            tempDf.withColumn(s"${sourceCol}",
              when(
                col(sourceCol).isNotNull,
                from_json(col(sourceCol), inferredSchema)
              ).otherwise(array())
            )
          }
        case _ => currentDF
      }
    }
  }
}
