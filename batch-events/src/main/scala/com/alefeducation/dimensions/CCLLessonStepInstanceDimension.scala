package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

class CCLLessonStepInstanceDimension(override val session: SparkSession) extends SparkBatchService {
  import Delta.Transformations._

  override val name: String = CCLLessonStepInstanceAssociationDimensionName

  private val stepInstanceUniqueKey = List(
    s"${LearningObjectiveStepInstanceEntity}_lo_id",
    s"${LearningObjectiveStepInstanceEntity}_id",
    s"${LearningObjectiveStepInstanceEntity}_step_id"
  )

  private val contentGroupKey = List("lessonUuid", "contentId", "stepId")
  private val assignmentGroupKey = List("lessonUuid", "assignmentId", "stepId")
  private val assessmentGroupKey = List("lessonUuid", "rule.id", "stepId")

  val stepInstanceDeltaMatchConditions: String =
    s"""
       |${Alias.Delta}.${LearningObjectiveStepInstanceEntity}_id = ${Alias.Events}.${LearningObjectiveStepInstanceEntity}_id
       | and
       |${Alias.Delta}.${LearningObjectiveStepInstanceEntity}_lo_id = ${Alias.Events}.${LearningObjectiveStepInstanceEntity}_lo_id
       | and
       |${Alias.Delta}.${LearningObjectiveStepInstanceEntity}_step_id = ${Alias.Events}.${LearningObjectiveStepInstanceEntity}_step_id
     """.stripMargin

  override def transform(): List[Sink] = {

    //Read
    val lessonContentDf: Option[DataFrame] = readOptional(ParquetCCLLessonContentAttachSource, session, isMandatory = false)
    val lessonAssigmentDf: Option[DataFrame] = readOptional(ParquetCCLLessonAssigmentAttachSource, session, isMandatory = false)
    val lessonAssessmentDf: Option[DataFrame] = readOptional(ParquetCCLLessonAssessmentRuleSource, session, isMandatory = false)

    //Transform
    val lessonContentTransformedDf: Option[DataFrame] = lessonContentDf.map(addStepUuidDefault).map(
      _.withColumn("contentId", col("contentId").cast(StringType))
        .transformForIWH2(
          CCLLessonContentAttachCols,
          LearningObjectiveStepInstanceEntity,
          LessonContentAssociationType,
          List(LessonContentAttachedEvent),
          List(LessonContentDetachedEvent),
          contentGroupKey
        ))

    val lessonAssigmentTransformedDf: Option[DataFrame] = lessonAssigmentDf.map(
      _.transformForIWH2(
        CCLLessonAssignmentAttachCols,
        LearningObjectiveStepInstanceEntity,
        LessonAssigmentAssociationType,
        List(LessonAssignmentAttachEvent),
        List(LessonAssignmentDetachedEvent),
        assignmentGroupKey
      ))

    val lessonAssessmentTransformedDf: Option[DataFrame] = lessonAssessmentDf.map(
      _.transformForIWH2(
        CCLLessonAssessmentRuleCols,
        LearningObjectiveStepInstanceEntity,
        LessonAssessmentRuleAssociationType,
        List(LessonAssessmentRuleAddedEvent, LessonAssessmentRuleUpdatedEvent),
        List(LessonAssessmentRuleRemovedEvent),
        assessmentGroupKey
      )
    )

    //Parquet Sinks
    val lessonContentParquetSink: Option[Sink] = lessonContentDf.map(_.toParquetSink(ParquetCCLLessonContentAttachSource))
    val lessonAssignmentParquetSink: Option[Sink] = lessonAssigmentDf.map(_.toParquetSink(ParquetCCLLessonAssigmentAttachSource))
    val lessonAssessmentParquetSink: Option[Sink] = lessonAssessmentDf.map(_.toParquetSink(ParquetCCLLessonAssessmentRuleSource))

    //Redshift Sinks
    val redshiftLessonContentSink: Option[Sink] = createRedshiftSink(lessonContentTransformedDf, "LessonContentAttachDetachEvent")
    val redshiftLessonAssigmentSink: Option[Sink] = createRedshiftSink(lessonAssigmentTransformedDf, "LessonAssignmentAttachDetachEvent")
    val redshiftLessonAssessmentSink: Option[Sink] = createRedshiftSink(lessonAssessmentTransformedDf, "LessonAssessmentEvent")

    //Delta Sinks
    val deltaLessonContentSink: Option[Sink] = createDeltaSink(lessonContentTransformedDf, "LessonContentAttachDetachEvent")
    val deltaLessonAssigmentSink: Option[Sink] = createDeltaSink(lessonAssigmentTransformedDf, "LessonAssignmentAttachDetachEvent")
    val deltaLessonAssessmentSink: Option[Sink] = createDeltaSink(lessonAssessmentTransformedDf, "LessonAssessmentEvent")

    (lessonContentParquetSink ++ lessonAssignmentParquetSink ++ lessonAssessmentParquetSink ++
      redshiftLessonAssigmentSink ++ redshiftLessonContentSink ++ redshiftLessonAssessmentSink ++
      deltaLessonContentSink ++ deltaLessonAssigmentSink ++ deltaLessonAssessmentSink).toList

  }

  def addStepUuidDefault(df: DataFrame): DataFrame =
    if (df.columns.contains("stepUuid")) df
    else df.withColumn("stepUuid", lit(null).cast(StringType))

  def createRedshiftSink(df: Option[DataFrame], eventType: String): Option[Sink] =
    df.map(
      _.toRedshiftIWHSink(RedshiftLessonStepInstanceSink,
                          LearningObjectiveStepInstanceEntity,
                          ids = stepInstanceUniqueKey,
                          eventType = eventType))

  def createDeltaSink(df: Option[DataFrame], eventType: String): Option[Sink] =
    df.flatMap(
      _.toIWH(stepInstanceDeltaMatchConditions, stepInstanceUniqueKey)
        .map(_.toSink(DeltaCCLLessonContentStepInstanceSink, LearningObjectiveStepInstanceEntity, eventType)))

}

object CCLLessonStepInstanceDimension {
  def main(args: Array[String]): Unit = {
    new CCLLessonStepInstanceDimension(SparkSessionUtils.getSession(CCLLessonStepInstanceAssociationDimensionName)).run
  }
}
