package com.alefeducation.facts.xapi

import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.Schema.schema
import com.alefeducation.schema.newxapi.{NewXAPIStatement, StudentExtensions}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.DataFrameUtility.{addTimestampColumns, getStringDateToTimestamp, getUTCDateFromMillisOrSeconds, isEmpty, selectAs}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class StudentXAPITransformer(override val name: String, override val session: SparkSession)
    extends SparkBatchService with XAPIHelper {

  import session.implicits._

  val xApi: StructType = schema[NewXAPIStatement]

  override def transform(): List[Sink] = {
    val rawXApiDf = read(ParquetStudentXApiV2Source, session)
      .select($"tenantId", $"eventType", from_json($"body", xApi).as("body"), $"loadtime")
      .withColumn("occurredOn", getStringDateToTimestamp("body.timestamp").cast(TimestampType))

    getStudentXApiSink(rawXApiDf)
  }

  def getStudentXApiSink(rawXApiDf: DataFrame) = {
    val extensions: StructType = schema[StudentExtensions]

    def putDefaultIdForNulls(df: DataFrame): DataFrame = {
      df.withColumn("schoolId",
          when($"extensions.tenant.school.id" isNull, DEFAULT_ID).otherwise($"extensions.tenant.school.id"))
        .withColumn("gradeId",
          when($"extensions.tenant.school.grade.id" isNull, DEFAULT_ID).otherwise($"extensions.tenant.school.grade.id"))
        .withColumn("sectionId",
          when($"extensions.tenant.school.section.id" isNull, DEFAULT_ID).otherwise($"extensions.tenant.school.section.id"))
        .withColumn("subjectId",
          when($"extensions.tenant.school.subject.id" isNull, DEFAULT_ID).otherwise($"extensions.tenant.school.subject.id"))
        .withColumn("academicYearId",
          when($"extensions.tenant.school.academicYearId" isNull, DEFAULT_ID).otherwise($"extensions.tenant.school.academicYearId"))
        .withColumn("academicCalendarId",
          when($"extensions.tenant.school.academicCalendar.id" isNull, DEFAULT_ID).otherwise($"extensions.tenant.school.academicCalendar.id"))
        .withColumn("teachingPeriodId",
          when($"extensions.tenant.school.academicCalendar.currentPeriod.teachingPeriodId" isNull, DEFAULT_ID).otherwise($"extensions.tenant.school.academicCalendar.currentPeriod.teachingPeriodId"))
        .withColumn("teachingPeriodTitle", $"extensions.tenant.school.academicCalendar.currentPeriod.teachingPeriodTitle")
        .withColumn("attempt", when($"extensions.attempt" isNull, MinusOne).otherwise($"extensions.attempt"))
        .withColumn("lessonPosition", when($"extensions.lessonPosition" isNull, MinusOne).otherwise($"extensions.lessonPosition"))
        .withColumn("experienceId", when($"extensions.experienceId" isNull, DEFAULT_ID).otherwise($"extensions.experienceId"))
        .withColumn("learningSessionId",
                    when($"extensions.learningSessionId" isNull, DEFAULT_ID).otherwise($"extensions.learningSessionId"))
        .withColumn("fromTime", when($"extensions.fromTime" isNull, MinusOneDecimal).otherwise($"extensions.fromTime"))
        .withColumn("toTime", when($"extensions.toTime" isNull, MinusOneDecimal).otherwise($"extensions.toTime"))
        .withColumn("score", $"result.score")
        .withColumn("rawScore", when($"score.raw" isNull, MinusOneDecimal).otherwise($"score.raw"))
        .withColumn("scaledScore", when($"score.scaled" isNull, MinusOneDecimal).otherwise($"score.scaled"))
        .withColumn("minScore", when($"score.min" isNull, MinusOneDecimal).otherwise($"score.min"))
        .withColumn("maxScore", when($"score.max" isNull, MinusOneDecimal).otherwise($"score.max"))
        .withColumn("isCompletionNode", when($"extensions.isCompletionNode" isNull, false).otherwise($"extensions.isCompletionNode"))
        .withColumn("isFlexibleLesson", when($"extensions.isFlexibleLesson" isNull, false).otherwise($"extensions.isFlexibleLesson"))
    }

    val studentXapiDf = rawXApiDf.filter($"eventType" like "student%")
    if (!isEmpty(studentXapiDf)) {
      val partitionOrderByClause = Window.partitionBy($"actorAccountName").orderBy($"timestamp")
      val timeSpentDf = getCommonCols(studentXapiDf, extensions)
        .withColumn("categoryId", $"contextActivityCategory.id".getItem(0))
        .withColumn("endTime", lead('timestamp, 1) over partitionOrderByClause)
        .withColumn("prevEventType", lag($"eventType", 1) over partitionOrderByClause)
        .withColumn("nextEventType", lead($"eventType", 1) over partitionOrderByClause)
        .withColumn("timeSpent", unix_timestamp($"endTime", DateTimeFormat) - unix_timestamp($"timestamp", DateTimeFormat))
        .withColumn("objectDefinitionName", $"objectDefinitionName".cast(StringType))
        .withColumn("timestampLocal", substring($"timestampLocal", StartingIndexTimestampLocal, EndIndexTimestampLocal))
      val withDefaultIdsForNulls = putDefaultIdForNulls(timeSpentDf)
      val redshiftDf = addTimestampColumns(selectAs(withDefaultIdsForNulls, studentXAPIColumnMapping), FactStudentActivities, isFact = true)
      val parquetSink = DataSink(
        ParquetStudentXApiV2Source,
        studentXapiDf.withColumn("eventdate", date_format($"occurredOn", "yyyy-MM-dd")).coalesce(1),
        Map("partitionBy" -> "eventdate,eventType")
      )
      List(parquetSink, DataSink(RedshiftStudentActivitiesSink, redshiftDf))
    } else {
      Nil
    }
  }
}

object StudentXAPI {
  def main(args: Array[String]): Unit = {
    new StudentXAPITransformer(StudentXapiV2Service, SparkSessionUtils.getSession(StudentXapiV2Service)).run
  }
}
