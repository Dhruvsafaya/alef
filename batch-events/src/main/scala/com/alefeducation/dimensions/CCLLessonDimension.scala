package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.BatchUtility.{getParquetSinks, getRedshiftSinks, selectedDfWithTimeColumns}
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility.{selectLatestRecords, _}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, lit, to_date, when}
import org.apache.spark.sql.types.{DateType, IntegerType, MetadataBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.alefeducation.util.BatchSessionUtility._

class CCLLessonDimension(override val session: SparkSession) extends SparkBatchService {

  import Delta.Transformations._
  import com.alefeducation.bigdata.batch.delta.Builder.Transformations
  import session.implicits._

  override val name = CCLLessonDimensionName

  private val deltaMatchCondition =
    s"""
       |${Alias.Delta}.${LearningObjectiveEntity}_ccl_id = ${Alias.Events}.${LearningObjectiveEntity}_ccl_id
     """.stripMargin

  private val colLength = 750
  private val metadata = new MetadataBuilder().putLong("maxlength", colLength).build()

  private val uniqueKey = List("lessonId")

  private val descMapping = "description" -> "lo_description"
  private val rolesMapping = "lessonRoles" -> "lo_roles"
  private val themeMapping = "themeIds" -> "lo_theme_ids"

  override def transform(): List[Sink] = {
    val lessonMutatedDf = read(ParquetCCLLessonMutatedSource, session)
    val lessonDeletedDf = read(ParquetCCLLessonDeletedSource, session)
    val lessonPublishedDf = read(ParquetCCLLessonPublishedSource, session)
    val lessonStatusChangedDf = read(ParquetCCLLessonWorkflowStatusChangedSource, session)

    val lessonCreatedWithStatusDf = lessonCreatedWithStatuses(lessonMutatedDf)
    val lessonUpdatedWithStatusDf = lessonUpdatedWithStatuses(lessonMutatedDf)
    val latestDeletedDf = latestLessonDeleted(lessonDeletedDf)
    val lessonPublishedWithStatusesDf = latestPublishedWithStatuses(lessonPublishedDf)

    val lessonStatusChangedWithStatusDf = lessonStatusChangedDf.transformIfNotEmpty(addLoActionStatus)
    val latestLessonStatusChangedDf = latestLessonStatusChanged(lessonStatusChangedWithStatusDf, lessonPublishedWithStatusesDf)
      .withColumn("eventType", lit(LessonWorkflowStatusChangedEvent))

    val redshiftLessonCreatedSinks = createRedshiftSinks(
      lessonCreatedWithStatusDf.transformIfNotEmpty(dropDescription),
      List(LessonCreatedEvent),
      CCLLessonCreateDimensionCols
    )
    val redshiftLessonUpdatedSinks = createRedshiftSinks(
      lessonUpdatedWithStatusDf.transformIfNotEmpty(dropDescription),
      List(LessonMetadataUpdatedEvent),
      CCLLessonUpdateDimensionCols
    )
    val redshiftLessonDeletedSinks = createRedshiftSinks(
      latestDeletedDf,
      List(LessonDeletedEvent),
      CCLLessonDeletedDimensionCols
    )
    val redshiftLessonPublishedSinks = createRedshiftSinks(
      lessonPublishedWithStatusesDf,
      List(LessonPublishedEvent),
      CCLLessonPublishedDimensionCols
    )
    val redshiftLessonStatusChangedSinks = createRedshiftSinks(
      latestLessonStatusChangedDf,
      List(LessonWorkflowStatusChangedEvent),
      CCLLessonWorkFlowStatusChangedDimensionCols
    )

    val parquetLessonMutatedSinks = getParquetSinks(session, ParquetCCLLessonMutatedSource, lessonMutatedDf)
    val parquetLessonDeletedSinks = getParquetSinks(session, ParquetCCLLessonDeletedSource, lessonDeletedDf)
    val parquetLessonPublishedSinks = getParquetSinks(session, ParquetCCLLessonPublishedSource, lessonPublishedDf)
    val parquetLessonStatusChangedSinks = getParquetSinks(session, ParquetCCLLessonWorkflowStatusChangedSource, lessonStatusChangedDf)

    val deltaLessonCreatedSink = createDeltaLakeForCreate(lessonCreatedWithStatusDf)
    val deltaLessonUpdateSink = createDeltaLakeForUpdate(
      DeltaCCLLessonUpdatedSink,
      CCLLessonUpdateDimensionCols + descMapping + rolesMapping + themeMapping,
      lessonUpdatedWithStatusDf
    )
    val deltaLessonDeletedSink = createDeltaLakeForDelete(
      DeltaCCLLessonDeletedSink,
      latestDeletedDf
    )
    val deltaLessonPublishedSink = createDeltaLakeForUpdate(
      DeltaCCLLessonPublishedSink,
      CCLLessonPublishedDimensionCols,
      lessonPublishedWithStatusesDf
    )
    val deltaLessonStatusChangedSink = createDeltaLakeForUpdate(
      DeltaCCLLessonWorkflowStatusChangedSink,
      CCLLessonWorkFlowStatusChangedDimensionCols,
      latestLessonStatusChangedDf
    )

    val redshiftLessonUpdatedWithMaxLengthSinks = addMaxLengthToRedshiftSink(redshiftLessonUpdatedSinks)

    redshiftLessonCreatedSinks ++ redshiftLessonUpdatedWithMaxLengthSinks ++ redshiftLessonPublishedSinks ++
      redshiftLessonStatusChangedSinks ++ redshiftLessonDeletedSinks ++
      deltaLessonCreatedSink ++ deltaLessonUpdateSink ++ deltaLessonPublishedSink ++ deltaLessonStatusChangedSink ++
      deltaLessonDeletedSink ++
      parquetLessonMutatedSinks ++ parquetLessonPublishedSinks ++ parquetLessonStatusChangedSinks ++ parquetLessonDeletedSinks
  }

  def addMaxLengthToRedshiftSink(sinks: List[DataSink]): List[Sink] =
    sinks.map { s =>
      val ndf = addMaxLength(s.output)

      DataSink(s.name, ndf, s.options)
    }

  def createDeltaLakeForCreate(df: DataFrame): Option[Sink] = {
    val ndf = df.transformIfNotEmpty(
      addDefaultFields _ compose addTimeColumns(CCLLessonCreateDimensionCols + descMapping + rolesMapping + themeMapping)
    )

    ndf
      .withEventDateColumn(false)
      .toCreate()
      .map(_.toSink(DeltaCCLLessonCreatedSink))
  }

  def lessonCreatedWithStatuses(df: DataFrame): DataFrame =
    df.transformIfNotEmpty(
      addLoStatus(LessonActiveStatusVal) _ compose addLoActionStatus
        compose filterByEventType(LessonCreatedEvent)
    )

  def createDeltaLakeForUpdate(name: String, columnMapping: Map[String, String], df: DataFrame): Option[Sink] = {
    val ndf = df.transformIfNotEmpty(
      addTimeColumns(columnMapping) _ compose selectLatest
    )

    ndf
      .withEventDateColumn(false)
      .toUpdate(matchConditions = deltaMatchCondition)
      .map(_.toSink(name, LearningObjectiveEntity))
  }

  def createDeltaLakeForDelete(name: String, df: DataFrame): Option[Sink] = {
    val ndf = df.transformIfNotEmpty(
      addTimeColumns(CCLLessonDeletedDimensionCols) _ compose selectLatest
    )

    ndf
      .withEventDateColumn(false)
      .toDelete(matchConditions = deltaMatchCondition)
      .map(_.toSink(name, LearningObjectiveEntity))
  }

  def lessonUpdatedWithStatuses(df: DataFrame): DataFrame =
    df.transformIfNotEmpty(
      addLoStatus(LessonActiveStatusVal) _ compose filterByEventType(LessonMetadataUpdatedEvent)
    )

  def selectLatest(df: DataFrame): DataFrame = selectLatestRecords(df, uniqueKey)

  def addTimeColumns(columnMapping: Map[String, String])(df: DataFrame): DataFrame =
    selectedDfWithTimeColumns(session, df, columnMapping, LearningObjectiveEntity)

  def latestLessonDeleted(df: DataFrame): DataFrame =
    df.transformIfNotEmpty(
      addLoStatus(LessonDeletedStatusVal) _ compose selectLatest
    )

  def latestLessonStatusChanged(lessonStatusChangedDf: DataFrame, lessonPublishedDf: DataFrame): DataFrame = {
    val statusChangeFieldNames = CCLLessonWorkFlowStatusChangedDimensionCols.keys.toList

    if (lessonStatusChangedDf.isEmpty && lessonPublishedDf.isEmpty) lessonStatusChangedDf
    else if (lessonPublishedDf.isEmpty) {
      val actionStatusForStatusChangedDf = lessonStatusChangedDf.select(statusChangeFieldNames.map(col): _*)
      selectLatestRecords(actionStatusForStatusChangedDf, uniqueKey)
    } else if (lessonStatusChangedDf.isEmpty) {
      val actionStatusForPublishedDf = lessonPublishedDf.select(statusChangeFieldNames.map(col): _*)
      selectLatestRecords(actionStatusForPublishedDf, uniqueKey)
    } else {
      val actionStatusForPublishedDf = lessonPublishedDf.select(statusChangeFieldNames.map(col): _*)
      val actionStatusForStatusChangedDf = lessonStatusChangedDf.select(statusChangeFieldNames.map(col): _*)

      val allActionStatusDf = actionStatusForPublishedDf.union(actionStatusForStatusChangedDf)
      selectLatestRecords(allActionStatusDf, uniqueKey)
    }
  }

  def latestPublishedWithStatuses(df: DataFrame): DataFrame = {
    if (df.isEmpty) df
    else {
      val latestDf = selectLatestRecords(df, uniqueKey)

      latestDf
        .withColumn("code", $"lessonJSON.code")
        .withColumn("academicYear", $"lessonJSON.academic_year")
        .withColumn(
          s"${LearningObjectiveEntity}_action_status",
          when($"publishedBefore" === true, lit(LessonRepublishedActionStatusVal))
            .otherwise(LessonPublishedActionStatusVal)
        )
        .withColumn("publishedDate", to_date($"publishedDate", "yyyy-MM-dd"))
    }
  }

  def dropDescription(df: DataFrame): DataFrame = df.drop("description")

  def addMaxLength(df: DataFrame): DataFrame =
    df.withColumn(s"${LearningObjectiveEntity}_title",
                  col(s"${LearningObjectiveEntity}_title").as(s"${LearningObjectiveEntity}_title", metadata))

  def addDefaultFields(df: DataFrame): DataFrame = {
    df.withColumn(s"${LearningObjectiveEntity}_max_stars", lit(null).cast(IntegerType))
      .withColumn(s"${LearningObjectiveEntity}_publisher_id", lit(null).cast(IntegerType))
      .withColumn(s"${LearningObjectiveEntity}_published_date", lit(null).cast(DateType))
  }

  def addLoActionStatus(df: DataFrame): DataFrame =
    df.withColumn(
      s"${LearningObjectiveEntity}_action_status",
      when($"status" === LessonDraftActionStatus, lit(LessonDraftActionStatusVal))
        .when($"status" === LessonReviewActionStatus, lit(LessonReviewActionStatusVal))
        .when($"status" === LessonReworkActionStatus, lit(LessonReworkActionStatusVal))
        .otherwise(lit(LessonUndefinedActionStatusVal))
    )

  def addLoStatus(status: Int)(df: DataFrame): DataFrame = df.withColumn(s"${LearningObjectiveEntity}_status", lit(status))

  def createRedshiftSinks(df: DataFrame, events: List[String], columnMapping: Map[String, String]): List[DataSink] = {
    getRedshiftSinks(
      session,
      df,
      events,
      columnMapping,
      LearningObjectiveEntity,
      RedshiftLearningObjectiveSink,
      latestByIds = uniqueKey,
      ids = List(s"${LearningObjectiveEntity}_ccl_id")
    )
  }
}

object CCLLessonDimension {
  def main(args: Array[String]): Unit = {
    new CCLLessonDimension(SparkSessionUtils.getSession(CCLLessonDimensionName)).run
  }
}
