package com.alefeducation.bigdata.batch

import com.alefeducation.util.{DataFrameUtility, StringUtilities}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_unixtime, lit, when}
import org.apache.spark.sql.types.DateType

object BatchUtils {

  def addStatusColumn(session: SparkSession, df: DataFrame, entityPrefix: String = ""): DataFrame = {
    import session.implicits._

    val statusCol = entityPrefix match {
      case ""        => "status"
      case x: String => s"${x}_status"
    }

    df.withColumn(
      statusCol,
      when($"eventType".contains("Disabled"), lit(Consts.Disabled))
        .otherwise(when($"eventType".contains("Deleted"), lit(Consts.Deleted)).otherwise(Consts.ActiveEnabled))
    )
  }

  def getColumnMapping(columns: List[String]): Map[String, String] = {
    columns.map(x => (x -> StringUtilities.lowerCamelToSnakeCase(x))).toMap
  }

  implicit class DataFrameUtils(df: DataFrame) {

    def convertColumnNamesToSnakeCase(except: List[String] = List()): DataFrame = {
      val columnNames = df.columns.diff(except).toList
      val columnMapping = getColumnMapping(columnNames)
      mapColumns(columnMapping)
    }

    def mapColumns(columnMapping: Map[String, String]): DataFrame = {
      columnMapping.foldLeft(df) { (df, colMap) =>
        df.withColumnRenamed(colMap._1, colMap._2)
      }
    }

    def addEntityPrefixToColumns(entity: String, except: List[String] = Nil): DataFrame = {
      val mapping = df.columns.map { col =>
        val prefixed = if (except.contains(col)) {
          col
        } else {
          s"${entity}_$col"
        }
        (col, prefixed)
      }.toMap
      mapColumns(mapping)
    }

    def addTimestampColumns(entityPrefix: String = "", timeCol: String = "occurredOn", isFact: Boolean = false): DataFrame = {
      DataFrameUtility.addTimestampColumns(df, entityPrefix, isFact, timeCol)
    }

    def addStatusColumn(entityPrefix: String = ""): DataFrame = BatchUtils.addStatusColumn(df.sparkSession, df, entityPrefix)

    def selectLatest(uniqueIdColumns: List[String], eventDateColumn: String = "occurredOn"): DataFrame =
      DataFrameUtility.selectLatestUpdated(df, eventDateColumn, uniqueIdColumns: _*)

    def epochToDate(column: String): DataFrame = {
      df.withColumn(column, from_unixtime(col(column) / 1000, "yyyy-MM-dd").cast(DateType))
    }

    def withMultipleColumnsRenamed(mapping: Map[String, String]): DataFrame = {
      df.select(
        df.columns.map(c => df(c).alias(mapping.getOrElse(c, c))): _*
      )
    }

    def removeColumnIfPresent(column: String*): Option[DataFrame] = {
      val col = df.columns.diff(column).toSeq
      col match {
        case x@Seq() => None
        case x@Seq(_) => Some(df.select(x(0)))
        case _ => Some(df.select(col.head, col.tail: _*))
      }
    }
  }

}
