package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.BatchTransformerUtility._


class CCLActivityAssociationDimension(override val session: SparkSession) extends SparkBatchService {

  override val name: String = CCLActivityAssociationDimensionName

  import Delta.Transformations._

  private val outcomeGroupKey = List(
    "activityUuid",
    "outcomeId"
  )

  private val uniqueKey = List(
    s"${LearningObjectiveAssociationEntity}_lo_id",
    s"${LearningObjectiveAssociationEntity}_id"
  )

  val deltaMatchConditions: String =
    s"""
       |${Alias.Delta}.${LearningObjectiveAssociationEntity}_id = ${Alias.Events}.${LearningObjectiveAssociationEntity}_id
       | and
       |${Alias.Delta}.${LearningObjectiveAssociationEntity}_lo_id = ${Alias.Events}.${LearningObjectiveAssociationEntity}_lo_id
     """.stripMargin

  override def transform(): List[Sink] = {
    val activityOutcomeDf: Option[DataFrame] = readOptional(ParquetCCLActivityOutcomeSource, session, isMandatory = false)

    val lessonOutcomeTransformedDf: Option[DataFrame] = activityOutcomeDf.map(
      _.transformForIWH2(
        CCLActivityOutcomeAttachCols,
        LearningObjectiveAssociationEntity,
        ActivityOutcomeAssociationType,
        List(ActivityOutcomeAttachedEvent),
        List(ActivityOutcomeDetachedEvent),
        outcomeGroupKey
      ))

    val activityOutcomeSink: Option[Sink] = activityOutcomeDf.map(_.toParquetSink(ParquetCCLActivityOutcomeSink))

    val redshiftLessonOutcomeSink: Option[Sink] = lessonOutcomeTransformedDf.map(
      _.toRedshiftIWHSink(RedshiftLearningObjectiveAssociationDimSink,
        LearningObjectiveAssociationEntity,
        ids = uniqueKey,
        eventType = "ActivityOutcomeAttachedEvent"))

    val deltaLessonOutcomeSink: Option[Sink] = lessonOutcomeTransformedDf.flatMap(
      _.toIWH(deltaMatchConditions, uniqueKey)
        .map(_.toSink(DeltaCCLActivityAssociationSink, LearningObjectiveAssociationEntity)))

    (activityOutcomeSink ++ redshiftLessonOutcomeSink ++ deltaLessonOutcomeSink).toList
  }
}

object CCLActivityAssociationDimension {
  def main(args: Array[String]): Unit = {
    new CCLActivityAssociationDimension(SparkSessionUtils.getSession(CCLActivityAssociationDimensionName)).run
  }
}
