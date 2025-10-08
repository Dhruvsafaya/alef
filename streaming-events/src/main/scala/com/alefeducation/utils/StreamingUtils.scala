package com.alefeducation.utils

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, date_format, from_json, schema_of_json}
import org.apache.spark.sql.types.StringType

object StreamingUtils {

  implicit class TransformationMethods(df: DataFrame) {

    def appendCommonColumnsForV3: DataFrame =
      df.withColumn("body_timestamp", col("body_timestamp").cast(StringType))
        .withColumn("occurredOn", col("body_timestamp"))
        .withColumn("eventdate", date_format(col("occurredOn"), "yyyy-MM-dd"))
        .withColumn("eventType", col("headers_eventType"))
        .withColumn("journeyType", col("headers_journey"))

    def parseJsonCol(colName: String, newColName: String): DataFrame = {
      import df.sparkSession.implicits._

      import scala.collection.JavaConverters._

      df.withColumn(newColName,
                    from_json(col(colName), schema_of_json(df.select(col(colName)).as[String].first), Map[String, String]().asJava))
    }

    def parseJsonColWithReadJson(colName: String, newColName: String): DataFrame = {
      import df.sparkSession.implicits._
      val jsonDF = df.sparkSession.read.json(df.select(colName).as[String])
      val schema = jsonDF.schema
      df.withColumn(newColName, from_json(col(colName), schema))
    }
  }
}
