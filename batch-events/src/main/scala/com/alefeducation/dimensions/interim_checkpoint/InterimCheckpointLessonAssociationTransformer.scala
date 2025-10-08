package com.alefeducation.dimensions.interim_checkpoint

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import org.apache.spark.sql.functions.{explode, col}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.alefeducation.util.BatchTransformerUtility._

object InterimCheckpointLessonAssociationTransformer {

  import InterimCheckpointDimension._
  private val icLessonKey = List("lessonId", "interimCheckpointId")

  val uniqueRedshiftIcLessonKey = List("ic_uuid", s"${InterimCheckpointLessonPrefix}_type")
  val uniqueDeltaIcLessonKey = List(
    s"${InterimCheckpointLessonPrefix}_ic_id",
    s"${InterimCheckpointLessonPrefix}_type"
  )
  val deltaIcLessonMatchConditions: String =
    s"""
       |${Alias.Delta}.${InterimCheckpointLessonPrefix}_ic_id = ${Alias.Events}.${InterimCheckpointLessonPrefix}_ic_id
     """.stripMargin

  def transformForICLessonAssociation(createdDF: Option[DataFrame], updatedDF: Option[DataFrame])(implicit session: SparkSession): List[Sink] = {

    val (deltaCreatedSink, redshiftCreatedSink) = makeICLessonsSinks(createdDF, InterimCheckpointRulesCreatedEvent)
    val (deltaUpdatedSink, redshiftUpdatedSink) = makeICLessonsSinks(updatedDF, InterimCheckpointRulesUpdatedEvent)

    (deltaCreatedSink ++ deltaUpdatedSink ++ redshiftCreatedSink ++ redshiftUpdatedSink).toList
  }

  private def makeICLessonsSinks(df: Option[DataFrame], eventType: String): (Option[Sink], Option[Sink]) = {
    import Delta.Transformations._
    import com.alefeducation.util.BatchTransformerUtility._

    val lessonICAssociation = df.map(_.transform(extractAssociationData))
    val lessonICTransformedDf: Option[DataFrame] = lessonICAssociation.map(
      _.transformForIWH2(
        interimCheckpointLessonCols,
        InterimCheckpointLessonPrefix,
        1,
        List(eventType),
        Nil,
        icLessonKey
      ))
    val redshiftICLessonSink: Option[Sink] = lessonICTransformedDf.map(
      _.toRedshiftIWHSink(
        RedshiftInterimCheckpointLessonSink,
        InterimCheckpointLessonPrefix,
        ids = uniqueRedshiftIcLessonKey,
        eventType = InterimCheckpointRulesCreatedEvent,
        isStagingSink = true
      ))
    val deltaICLessonSink = lessonICTransformedDf.flatMap(
      _.renameUUIDcolsForDelta(Some(s"${InterimCheckpointLessonPrefix}_"))
        .toIWH(deltaIcLessonMatchConditions, uniqueDeltaIcLessonKey)
        .map(_.toSink(DeltaInterimCheckpointLessonSink, InterimCheckpointLessonPrefix)))

    deltaICLessonSink -> redshiftICLessonSink
  }

  private def extractAssociationData(df: DataFrame): DataFrame = {
    df.selectLatestByRowNumber(List("interimCheckpointId"))
      .select(col("eventType"), col("interimCheckpointId"), col("occurredOn"), explode(col("lessonsWithPools")))
      .select(col("eventType"), col("interimCheckpointId"), col("occurredOn"), col("col.id").as("lessonId"))
  }
}
