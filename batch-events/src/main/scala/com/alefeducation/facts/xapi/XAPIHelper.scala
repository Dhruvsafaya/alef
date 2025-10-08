package com.alefeducation.facts.xapi

import com.alefeducation.schema.Schema.{activityObjectSchema, agentObjectSchema, newXAPISchema, schema}
import com.alefeducation.schema.newxapi.XAPIGenericObject
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.DataFrameUtility.{getStringDateToTimestamp, selectColumns}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructType, TimestampType}
import org.apache.spark.sql.DataFrame

trait XAPIHelper extends SparkBatchService {

  import session.implicits._

  def getCommonCols(xapiDf: DataFrame, extensionSchema: StructType): DataFrame = {
    val df = selectColumns(session, xapiDf, newXAPISchema)
      .withColumn("extensions", $"contextExtensions.`http://alefeducation.com`")
      .withColumn("extensions", from_json($"extensions", extensionSchema))
      .withColumn("timestamp", getStringDateToTimestamp("timestamp").cast(TimestampType))
      .withColumn("eventDateDw", date_format($"timestamp", "yyyyMMdd").cast(IntegerType))
      .withColumn("timestampLocal", $"extensions.timestampLocal")
      .withColumn("occurredOn", $"timestamp")
    extractXAPIObject(df)
  }

  private def extractXAPIObject(df: DataFrame): DataFrame = {
    val dfWithObjectType = df.withColumn("objectType", from_json($"object", schema[XAPIGenericObject]))

    val activityDf = dfWithObjectType.filter($"objectType.objectType" === Activity)
      .withColumn("object", from_json($"object", activityObjectSchema))
      .withColumn("objectId", $"object.id")
      .withColumn("objectType", $"object.objectType")
      .withColumn("objectDefinitionType", $"object.definition.type")
      .withColumn("objectDefinitionName", $"object.definition.name")
      .withColumn("objectDefinitionDescription", $"object.definition.description")
      .withColumn("objectAccountName", lit(NULL).cast("string"))
      .withColumn("objectAccountHomePage", lit(NULL).cast("string"))
      .drop("object")

    val agentDf = dfWithObjectType.filter($"objectType.objectType" === Agent)
      .withColumn("object", from_json($"object", agentObjectSchema))
      .withColumn("objectType", $"object.objectType")
      .withColumn("objectAccountName", $"object.account.name")
      .withColumn("objectAccountHomePage", $"object.account.homePage")
      .withColumn("objectId", lit(NULL).cast("string"))
      .withColumn("objectDefinitionType", lit(NULL).cast("string"))
      .withColumn("objectDefinitionName", typedLit(Map[String, String]().empty))
      .withColumn("objectDefinitionDescription", typedLit(Map[String, String]().empty))
      .drop("object")

    activityDf.unionByName(agentDf)
  }
}
