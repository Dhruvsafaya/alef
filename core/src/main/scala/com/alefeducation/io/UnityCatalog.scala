package com.alefeducation.io

import com.alefeducation.util.DataFrameUtility.currentUTCDate
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{lit, to_utc_timestamp}
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}

import java.util.Calendar
import scala.util.{Failure, Success, Try}

class UnityCatalog(private val spark: SparkSession) {

  val log: Logger = Logger.getLogger(this.getClass.getName)

  def read(tableName: String): DataFrame = {
    Try {
      spark.read.format("delta").table(tableName)
    } match {
      case Success(df) => df
      case Failure(ex) => {
        throw ex
      }
    }
  }

  def readOptional(path: String): Option[DataFrame] = {
    Try {
      val df = spark.read.format("delta").load(path)
      if (df.isEmpty) None else Some(df)
    } match {
      case Success(res) => res
      case Failure(ex: AnalysisException) if ex.errorClass.contains("PATH_NOT_FOUND") =>
        log.error(s"Could not read the table $path, returning empty DF", ex)
        None
      case Failure(ex) => throw ex
    }
  }

  def write(df: DataFrame, tableName: String, filePath: String, partitionBy: List[String] = Nil): Unit = {
    df.withColumn("created_at", to_utc_timestamp(lit(currentUTCDate), Calendar.getInstance().getTimeZone.getID))
      .write
      .partitionBy(partitionBy: _*)
      .format("delta")
      .options(Map("mergeSchema" -> "true"))
      .mode(SaveMode.Append)
      .save(filePath)
    //.saveAsTable(tableName)
  }

}
