package com.alefeducation.facts.xapi

import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.Schema.schema
import com.alefeducation.schema.newxapi.{NewXAPIStatement, TeacherExtensions}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.DataFrameUtility.{addTimestampColumns, getStringDateToTimestamp, selectAs}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import com.alefeducation.util.BatchTransformerUtility._

class TeacherXAPITransformer(override val name: String, override val session: SparkSession)
    extends SparkBatchService
    with XAPIHelper {

  import session.implicits._

  val xApi: StructType = schema[NewXAPIStatement]

  val uuidLength = 36
  val maxLength = 100
  val maxLengthMiddle = 600;
  val maxLengthLong = 2000

  val partitionBy = Map("partitionBy" -> "eventdate,eventType")

  val partitionOrderByClause: WindowSpec = Window.partitionBy($"actorAccountName").orderBy($"timestamp")

  val extensions: StructType = schema[TeacherExtensions]

  val fstValidateCols = List("actorAccountName", "tenantId")
  val sndValidateCols = List("schoolId", "gradeId", "sectionId", "subjectId")
  val thirdValidateCols = List(
    "actorAccountHomePage",
    "actorObjectType",
    "verbId",
    "verbDisplay",
    "objectDefinitionType",
    "objectType",
    "categoryId",
    "eventType",
    "prevEventType",
    "nextEventType"
  )
  val fourValidateCols = List("timestampLocal")
  val fiveValidateCols = List("objectId", "objectDefinitionName")

  override def transform(): List[Sink] = {
    val rawXApiDf = readOptional(ParquetTeacherXApiV2Source, session)
      .map(_.select($"tenantId", $"eventType", from_json($"body", xApi).as("body"), $"loadtime")
      .withColumn("occurredOn", getStringDateToTimestamp("body.timestamp").cast(TimestampType)))

    val teacherXapiDf = rawXApiDf.map(_.filter($"eventType" like "teacher%")).flatMap(_.checkEmptyDf)
    val teacherXApiParsedCommonCols = teacherXapiDf.map(df => getCommonCols(df, extensions))

    val timeSpentDf = teacherXApiParsedCommonCols.map(
       _.withColumn("categoryId", $"contextActivityCategory.id".getItem(0))
        .withColumn("endTime", lead('timestamp, 1) over partitionOrderByClause)
        .withColumn("prevEventType", lag($"eventType", 1) over partitionOrderByClause)
        .withColumn("nextEventType", lead($"eventType", 1) over partitionOrderByClause)
        .withColumn("timeSpent", unix_timestamp($"endTime", DateTimeFormat) - unix_timestamp($"timestamp", DateTimeFormat))
        .withColumn("objectDefinitionName", trim(col("objectDefinitionName").cast(StringType)))
        .withColumn("timestampLocal", substring($"timestampLocal", StartingIndexTimestampLocal, EndIndexTimestampLocal))
    ).flatMap(_.checkEmptyDf)

    val withDefaultIdsForNulls = timeSpentDf.map(putDefaultIdForNulls)
    val validTeacherXapi = withDefaultIdsForNulls.flatMap(_.transform(validate).checkEmptyDf)
    val invalidTeacherXapi = withDefaultIdsForNulls.flatMap(_.transform(invalid).checkEmptyDf)

    val redshiftDf = validTeacherXapi.map(df => addTimestampColumns(selectAs(df, teacherXAPIColumnMapping), FactTeacherActivities, isFact = true))

    val parquetSink = teacherXapiDf.map(df => DataSink(
      ParquetTeacherXApiV2Source,
      addEventDate(df),
      partitionBy
    ))
    val invalidParquetSink = invalidTeacherXapi.map(df => DataSink(
      ParquetInvalidTeacherXApiV2Sink,
      addEventDate(df),
      partitionBy
    ))
    val redshiftSink = redshiftDf.map(df => DataSink(RedshiftTeacherActivitiesSink, df))

    (parquetSink ++ redshiftSink ++ invalidParquetSink).toList
  }

  private def putDefaultIdForNulls(df: DataFrame): DataFrame = {
    df.withColumn("schoolId", when($"extensions.tenant.school.id" isNull, DEFAULT_ID).otherwise($"extensions.tenant.school.id"))
      .withColumn("gradeId",
        when($"extensions.tenant.school.grade.id" isNull, DEFAULT_ID).otherwise($"extensions.tenant.school.grade.id"))
      .withColumn("sectionId",
        when($"extensions.tenant.school.section.id" isNull, DEFAULT_ID).otherwise($"extensions.tenant.school.section.id"))
      .withColumn("subjectId",
        when($"extensions.tenant.school.subject.id" isNull, DEFAULT_ID).otherwise($"extensions.tenant.school.subject.id"))
  }

  private def addEventDate(df: DataFrame): DataFrame =
    df.withColumn("eventdate", date_format($"occurredOn", "yyyy-MM-dd")).coalesce(1)

  private def validate(df: DataFrame): DataFrame = {
    val filterCondition =
      (fstValidateCols.map(colLength(_) === uuidLength) ++
        sndValidateCols.map(colLength(_) <= uuidLength) ++
        thirdValidateCols.map(colLength(_) <= maxLength) ++
        fourValidateCols.map(colLength(_) <= maxLengthMiddle) ++
        fiveValidateCols.map(colLength(_) <= maxLengthLong))
        .reduce(_ && _)

    df.filter(filterCondition)
  }

  private def invalid(df: DataFrame): DataFrame = {
    val filterCondition =
      (fstValidateCols.map(colLength(_) =!= uuidLength) ++
        sndValidateCols.map(colLength(_) > uuidLength) ++
        thirdValidateCols.map(colLength(_) > maxLength) ++
        fourValidateCols.map(colLength(_) > maxLengthMiddle) ++
        fiveValidateCols.map(colLength(_) > maxLengthLong))
        .reduce(_ || _)

    df.filter(filterCondition)
  }

  private def colLength(name: String): Column = {
    when(col(name).isNull, 0).otherwise(length(col(name)))
  }
}

object TeacherXApi {
  def main(args: Array[String]): Unit = {
    new TeacherXAPITransformer(TeacherXapiV2Service, SparkSessionUtils.getSession(TeacherXapiV2Service)).run
  }
}
