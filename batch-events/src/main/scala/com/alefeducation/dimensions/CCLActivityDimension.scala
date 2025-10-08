package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.schema.activity.ActivityAssessmentRule
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.BatchSessionUtility.filterByEventTypeOpt
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}


class CCLActivityDimension(override val session: SparkSession) extends SparkBatchService {
  private val colMaxLengthMetadata = new MetadataBuilder().putLong("maxlength", 750).build()
  private val loTitleColumnName="lo_title"

  import Delta.Transformations._

  override val name: String = CCLActivityDimensionName

  private val deltaMatchCondition =
    s"""
       |${Alias.Delta}.${LearningObjectiveEntity}_ccl_id = ${Alias.Events}.${LearningObjectiveEntity}_ccl_id
     """.stripMargin

  private val uniqueKey: List[String] = List("lessonId")

  private val redshiftUniqueKey: List[String] = List(s"${LearningObjectiveEntity}_ccl_id")

  private val descMapping = "description" -> "lo_description"
  private val rolesMapping = "lessonRoles" -> "lo_roles"
  private val themeMapping = "themeIds" -> "lo_theme_ids"

  private val deltaCols = List(descMapping, rolesMapping, themeMapping).toMap

  private val deltaComponentsCols = Map("componentDescription" -> "step_instance_description", "componentLocation" -> "step_instance_location")

  private val statusChangedFieldNames: List[String] = CCLActivityWorkFlowStatusChangedDimensionCols.keys.toList

  private val associationTypesMapping: Map[String, Int] = Map(
    Content -> ContentVal,
    Assignment -> AssignmentVal,
    Assessment -> AssessmentVal,
    KeyTerm -> KeyTermVal
  )

  private val associationGroupKey = List("activityUuid", "componentId", "componentStepUuid")

  private val componentType = "componentType"

  private val componentsUniqueKey = List(
    s"${LearningObjectiveStepInstanceEntity}_lo_id",
    s"${LearningObjectiveStepInstanceEntity}_id",
    s"${LearningObjectiveStepInstanceEntity}_step_uuid"
  )

  val activityComponentsDeltaMatchConditions: String =
    s"""
       |${Alias.Delta}.${LearningObjectiveStepInstanceEntity}_id = ${Alias.Events}.${LearningObjectiveStepInstanceEntity}_id
       | and
       |${Alias.Delta}.${LearningObjectiveStepInstanceEntity}_lo_id = ${Alias.Events}.${LearningObjectiveStepInstanceEntity}_lo_id
       | and
       |${Alias.Delta}.${LearningObjectiveStepInstanceEntity}_step_uuid = ${Alias.Events}.${LearningObjectiveStepInstanceEntity}_step_uuid
     """.stripMargin

  override def transform(): List[Sink] = {
    val activityMetadataDf: Option[DataFrame] = readOptional(ParquetCCLActivityMetadataSource, session, isMandatory = false)
      .map(castPublisherId)
    val activityMetadataDeleteDf: Option[DataFrame] = readOptional(ParquetCCLActivityMetadataDeleteSource, session, isMandatory = false)
    val activityWorkflowStatusChangedDf: Option[DataFrame] = readOptional(ParquetCCLActivityWorkflowStatusChangedSource, session, isMandatory = false)

    val activityCreatedDf = activityMetadataDf.flatMap(filterByEventTypeOpt(ActivityMetadataCreatedEvent))
      .map(addActivityActionStatus)
    val activityUpdatedDf = activityMetadataDf.flatMap(filterByEventTypeOpt(ActivityMetadataUpdatedEvent))
      .map(addActivityActionStatus)
    val activityPublishedDf = activityMetadataDf.flatMap(filterByEventTypeOpt(ActivityPublishedEvent))
      .map(addPublishStatuses compose selectLatest)

    val activityComponentsDf = activityPublishedDf.map(addComponentId compose explodeComponents)

    val activityStatusDf = activityWorkflowStatusChangedDf.map(addActivityActionStatus)
    val latestActivityStatusChangedDf = latestActivityStatusChanged(activityStatusDf, activityPublishedDf)
      .map(_.transform(addWorkflowStatusChanged))

    val activityCreatedTransformedDf = activityCreatedDf.map(
      _.transformForInsertDim(
        CCLActivityCreatedDimensionCols ++ deltaCols,
        LearningObjectiveEntity,
        uniqueKey
      ).withColumn(loTitleColumnName, col(loTitleColumnName).as(loTitleColumnName, colMaxLengthMetadata))
    )

    val activityUpdatedTransformedDf = activityUpdatedDf.map(
      _.transformForUpdateDim(
        CCLActivityUpdatedDimensionCols ++ deltaCols,
        LearningObjectiveEntity,
        uniqueKey
      ).withColumn(loTitleColumnName, col(loTitleColumnName).as(loTitleColumnName, colMaxLengthMetadata))
    )

    val activityPublishedTransformedDf = activityPublishedDf.map(
      _.transformForUpdateDim(
        CCLActionPublishedDimensionCols ++ deltaCols,
        LearningObjectiveEntity,
        uniqueKey
      ).withColumn(loTitleColumnName, col(loTitleColumnName).as(loTitleColumnName, colMaxLengthMetadata))
    )

    val activityDeletedTransformedDf = activityMetadataDeleteDf.map(_.transformForDelete(
      CCLActivityDeletedDimensionCols,
      LearningObjectiveEntity,
      uniqueKey
    ))

    val activityStatusChangedTransformedDf = latestActivityStatusChangedDf.map(_.transformForUpdateDim(
      CCLActivityWorkFlowStatusChangedDimensionCols,
      LearningObjectiveEntity,
      uniqueKey
    ))

    val activityComponentsTransformedDf = activityComponentsDf.map(_.transformForIWH(
      CCLActivityComponentsCols ++ deltaComponentsCols,
      LearningObjectiveStepInstanceEntity,
      AssociationTypesMapping(componentType, associationTypesMapping),
      List(ActivityPublishedEvent),
      Nil,
      associationGroupKey
    ))

    val redshiftActivityCreatedSink = activityCreatedTransformedDf.map(
      _.transform(dropDeltaCols)
        .toRedshiftInsertSink(RedshiftLearningObjectiveSink, eventType = ActivityMetadataCreatedEvent)
    )

    val redshiftActivityUpdatedSink = activityUpdatedTransformedDf.map(
      _.transform(dropDeltaCols)
        .toRedshiftUpdateSink(
          RedshiftLearningObjectiveSink,
          ActivityMetadataUpdatedEvent,
          LearningObjectiveEntity,
          ids = redshiftUniqueKey
        )
    )

    val redshiftActivityPublishedSink = activityPublishedTransformedDf.map(
      _.transform(dropDeltaCols)
        .toRedshiftUpdateSink(
          RedshiftLearningObjectiveSink,
          ActivityPublishedEvent,
          LearningObjectiveEntity,
          ids = redshiftUniqueKey
        )
    )

    val redshiftActivityStatusChangedSink = activityStatusChangedTransformedDf.map(
      _.toRedshiftUpdateSink(
        RedshiftLearningObjectiveSink,
        ActivityWorkflowStatusChangedEvent,
        LearningObjectiveEntity,
        ids = redshiftUniqueKey
      )
    )

    val redshiftActivityDeletedSink = activityDeletedTransformedDf.map(
      _.toRedshiftUpdateSink(
        RedshiftLearningObjectiveSink,
        ActivityMetadataDeletedEvent,
        LearningObjectiveEntity,
        ids = redshiftUniqueKey
      )
    )

    val redshiftActivityComponentsSink = activityComponentsTransformedDf.map(
      _.transform(dropDeltaComponentsCols)
        .toRedshiftIWHSink(
          RedshiftLessonStepInstanceSink,
          LearningObjectiveStepInstanceEntity,
          ids = componentsUniqueKey
      )
    )

    val deltaActivityCreatedSink = activityCreatedTransformedDf.flatMap(
      _.toCreate()
        .map(_.toSink(DeltaCCLActivitySink, eventType = ActivityMetadataCreatedEvent))
    )

    val deltaActivityUpdatedSink = activityUpdatedTransformedDf.flatMap(
      _.toUpdate(matchConditions = deltaMatchCondition)
        .map(_.toSink(DeltaCCLActivitySink, LearningObjectiveEntity, eventType = ActivityMetadataUpdatedEvent))
    )

    val deltaActivityPublishedSink = activityPublishedTransformedDf.flatMap(
      _.toUpdate(matchConditions = deltaMatchCondition)
        .map(_.toSink(DeltaCCLActivitySink, LearningObjectiveEntity, eventType = ActivityPublishedEvent))
    )

    val deltaActivityStatusChangedSink = activityStatusChangedTransformedDf.flatMap(
      _.toUpdate(matchConditions = deltaMatchCondition)
        .map(_.toSink(DeltaCCLActivitySink, LearningObjectiveEntity, eventType = ActivityWorkflowStatusChangedEvent))
    )

    val deltaActivityDeletedSink = activityDeletedTransformedDf.flatMap(
      _.toDelete(matchConditions = deltaMatchCondition)
        .map(_.toSink(DeltaCCLActivitySink, LearningObjectiveEntity, eventType = ActivityMetadataDeletedEvent))
    )

    val deltaActivityComponentsSink = activityComponentsTransformedDf.flatMap(
      _.toIWH(activityComponentsDeltaMatchConditions, componentsUniqueKey)
        .map(_.toSink(DeltaCCLLessonContentStepInstanceSink, LearningObjectiveStepInstanceEntity))
    )

    val parquetActivityMetadataSink = activityMetadataDf.map(_.toParquetSink(ParquetCCLActivityMetadataSink))
    val parquetActivityMetadataDeleteSink = activityMetadataDeleteDf.map(_.toParquetSink(ParquetCCLActivityMetadataDeleteSink))
    val parquetActivityWorkflowStatusChangedSink = activityWorkflowStatusChangedDf.map(_.toParquetSink(ParquetCCLActivityWorkflowStatusChangedSink))

    (redshiftActivityCreatedSink ++ redshiftActivityUpdatedSink ++ redshiftActivityPublishedSink ++
      redshiftActivityStatusChangedSink ++ redshiftActivityDeletedSink ++ redshiftActivityComponentsSink ++
      deltaActivityCreatedSink ++ deltaActivityUpdatedSink ++ deltaActivityPublishedSink ++
      deltaActivityStatusChangedSink ++ deltaActivityDeletedSink ++ deltaActivityComponentsSink ++
      parquetActivityMetadataSink ++ parquetActivityMetadataDeleteSink ++
      parquetActivityWorkflowStatusChangedSink).toList
  }

  /**
   * Because Activity as replacement of CCL Lesson and use the same delta folder the field lo_publisher_id required integer type
   */
  def castPublisherId(df: DataFrame): DataFrame =
    if (df.columns.contains("publisherId")) {
      df.withColumn("publisherId", col("publisherId").cast(IntegerType))
    } else df

  def explodeComponents: DataFrame => DataFrame =
     _.select(col("*"), explode(col("components")))
      .withColumn("componentTitle", col("col.title"))
      .withColumn("componentDescription", col("col.description"))
      .withColumn("componentStepUuid", col("col.stepUuid"))
      .withColumn("componentActivityTemplateComponentId", col("col.activityTemplateComponentId"))
      .withColumn("componentCclContentId", col("col.cclContentId"))
      .withColumn("componentLocation", col("col.location"))
      .withColumn("componentMediaType", col("col.mediaType"))
      .withColumn("componentAssignmentId", col("col.assignmentId"))
      .withColumn(componentType, col("col.activityComponentType"))
      .select(col("*"), explode_outer(col("col.assessmentRules")).as("componentRules"))
      .castStructCol[ActivityAssessmentRule]("componentRules")
      .select("*", "componentRules.*")
      .drop("col", "components", "componentRules")

  def addComponentId: DataFrame => DataFrame =
    _.withColumn("componentId",
      when(col("componentAssignmentId").isNotNull, col("componentAssignmentId"))
        .when(col("id").isNotNull, col("id"))
        .otherwise(col("componentCclContentId"))
        .cast(StringType)
    )

  def addPublishStatuses: DataFrame => DataFrame =
    _.withColumn(
      s"${LearningObjectiveEntity}_action_status",
      when(col("publishedBefore") === true, lit(ActivityRepublishedActionStatusVal))
        .otherwise(ActivityPublishedActionStatusVal)
    ).withColumn("publishedDate", to_date(col("publishedDate"), "yyyy-MM-dd"))

  def selectLatest: DataFrame => DataFrame = selectLatestRecordsByRowNumber(_, uniqueKey)

  def dropDeltaCols: DataFrame => DataFrame = _.drop(deltaCols.values.toList: _*)

  def dropDeltaComponentsCols: DataFrame => DataFrame = _.drop(deltaComponentsCols.values.toList: _*)

  def addActivityActionStatus: DataFrame => DataFrame =
    _.withColumn(
      s"${LearningObjectiveEntity}_action_status",
      when(col("status") === ActivityDraftActionStatus, lit(ActivityDraftActionStatusVal))
        .when(col("status") === ActivityReviewActionStatus, lit(ActivityReviewActionStatusVal))
        .when(col("status") === ActivityReworkActionStatus, lit(ActivityReworkActionStatusVal))
        .otherwise(lit(ActivityUndefinedActionStatusVal))
    )

  def latestActivityStatusChanged(statusChanged: Option[DataFrame], published: Option[DataFrame]): Option[DataFrame] =
    (statusChanged, published) match {
      case (Some(sc), Some(pub)) => Some(
        sc.transform(selectLatestStatusChangedFields)
          .unionByName(pub.transform(selectLatestStatusChangedFields))
      )
      case (Some(sc), None) => Some(sc.transform(selectLatestStatusChangedFields))
      case (None, Some(pub)) => Some(pub.transform(selectLatestStatusChangedFields))
      case _ => None
    }

  def addWorkflowStatusChanged: DataFrame => DataFrame = _.withColumn("eventType", lit(ActivityWorkflowStatusChangedEvent))

  def selectLatestStatusChangedFields: DataFrame => DataFrame = selectLatest compose selectStatusChangedFields

  def selectStatusChangedFields: DataFrame => DataFrame = _.select(statusChangedFieldNames.map(col): _*)
}

object CCLActivityDimension {
  def main(args: Array[String]): Unit = {
    new CCLActivityDimension(SparkSessionUtils.getSession(CCLActivityDimensionName)).run
  }
}