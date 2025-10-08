package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class CCLLessonAssociationDimension(override val session: SparkSession) extends SparkBatchService {

  import Delta.Transformations._

  override val name: String = CCLLessonAssociationDimensionName

  private val orderByField = "occurredOn"

  private val uniqueKey = List(
    s"${LearningObjectiveAssociationEntity}_lo_id",
    s"${LearningObjectiveAssociationEntity}_id"
  )

  private val outcomeGroupKey = List(
    "lessonUuid",
    "outcomeId"
  )

  private val skillGroupKey = List(
    "lessonUuid",
    "skillId"
  )

  val deltaMatchConditions: String =
    s"""
       |${Alias.Delta}.${LearningObjectiveAssociationEntity}_id = ${Alias.Events}.${LearningObjectiveAssociationEntity}_id
       | and
       |${Alias.Delta}.${LearningObjectiveAssociationEntity}_lo_id = ${Alias.Events}.${LearningObjectiveAssociationEntity}_lo_id
     """.stripMargin

  //Read
  override def transform(): List[Sink] = {
    val lessonSkillDf: Option[DataFrame] = readOptional(ParquetCCLLessonSkillLinkSource, session, isMandatory = false)
    val lessonOutcomeDf: Option[DataFrame] = readOptional(ParquetCCLLessonOutcomeAttachSource, session, isMandatory = false)

    //Transform
    val lessonSkillTransformedDf: Option[DataFrame] = lessonSkillDf.map(
      _.transformForIWH2(
        CCLLessonSkillLinkCols,
        LearningObjectiveAssociationEntity,
        LessonSkillLinkType,
        List(LessonSkillLinkedEvent),
        List(LessonSkillUnlinkedEvent),
        skillGroupKey
      ))

    val lessonOutcomeTransformedDf: Option[DataFrame] = lessonOutcomeDf.map(
      _.transformForIWH2(
        CCLLessonOutcomeAttachCols,
        LearningObjectiveAssociationEntity,
        LessonOutcomeAssociationType,
        List(LessonOutcomeAttachedEvent),
        List(LessonOutcomeDetachedEvent),
        outcomeGroupKey
      ))

    //Parquet Sinks
    val lessonSkillParquetSink: Option[Sink] = lessonSkillDf.map(_.toParquetSink(ParquetCCLLessonSkillLinkSink))
    val lessonOutcomeParquetSink: Option[Sink] = lessonOutcomeDf.map(_.toParquetSink(ParquetCCLLessonOutcomeAttachSource))

    //Redshift Sinks
    val redshiftLessonSkillSink: Option[Sink] = lessonSkillTransformedDf.map(
      _.toRedshiftIWHSink(RedshiftLearningObjectiveAssociationDimSink,
                          LearningObjectiveAssociationEntity,
                          ids = uniqueKey,
                          eventType = "LessonSkillLinkedEvent"))

    val redshiftLessonOutcomeSink: Option[Sink] = lessonOutcomeTransformedDf.map(
      _.toRedshiftIWHSink(RedshiftLearningObjectiveAssociationDimSink,
                          LearningObjectiveAssociationEntity,
                          ids = uniqueKey,
                          eventType = "LessonOutcomeAttachedEvent"))

    //Delta Sinks
    val deltaLessonSkillSink: Option[Sink] = lessonSkillTransformedDf.flatMap(
      _.toIWH(deltaMatchConditions, uniqueKey)
        .map(_.toSink(DeltaCCLLessonSkillLinkSink, LearningObjectiveAssociationEntity)))

    val deltaLessonOutcomeSink: Option[Sink] = lessonOutcomeTransformedDf.flatMap(
      _.toIWH(deltaMatchConditions, uniqueKey)
        .map(_.toSink(DeltaCCLLessonOutcomeAttachSink, LearningObjectiveAssociationEntity)))

    (lessonSkillParquetSink ++ lessonOutcomeParquetSink ++
      redshiftLessonSkillSink ++ redshiftLessonOutcomeSink ++
      deltaLessonSkillSink ++ deltaLessonOutcomeSink).toList
  }
}

object CCLLessonAssociationDimension {

  def main(args: Array[String]): Unit = {
    new CCLLessonAssociationDimension(SparkSessionUtils.getSession(CCLLessonAssociationDimensionName)).run
  }
}
