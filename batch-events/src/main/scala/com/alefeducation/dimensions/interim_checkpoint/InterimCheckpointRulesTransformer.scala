package com.alefeducation.dimensions.interim_checkpoint

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import org.apache.spark.sql.functions.{concat_ws, explode, lower, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import com.alefeducation.util.BatchTransformerUtility._

object InterimCheckpointRulesTransformer {

  import InterimCheckpointDimension._
  private val icRuleKey = List("interimCheckpointId", "learningOutcomeId", "resourceType")

  val uniqueRedshiftIcRuleKey = List("ic_uuid", s"${InterimCheckpointRulePrefix}_type")
  val uniqueDeltaIcRuleKey = List(
    s"${InterimCheckpointRulePrefix}_ic_id",
    s"${InterimCheckpointRulePrefix}_type",
  )
  val deltaIcRulesMatchConditions: String =
    s"""
       |${Alias.Delta}.${InterimCheckpointRulePrefix}_ic_id = ${Alias.Events}.${InterimCheckpointRulePrefix}_ic_id
     """.stripMargin

  def transformForIcRules(createdDF: Option[DataFrame], updatedDF: Option[DataFrame])(implicit session: SparkSession): List[Sink] = {
    val (deltaCreatedSink, redshiftCreatedSink) = makeICRulesSinks(createdDF, InterimCheckpointRulesCreatedEvent)
    val (deltaUpdatedSink, redshiftUpdatedSink) = makeICRulesSinks(updatedDF, InterimCheckpointRulesUpdatedEvent)

    (deltaCreatedSink ++ deltaUpdatedSink ++ redshiftCreatedSink ++ redshiftUpdatedSink).toList
  }

  private def makeICRulesSinks(df: Option[DataFrame], evenType: String): (Option[Sink], Option[Sink]) = {
    import Delta.Transformations._
    import com.alefeducation.util.BatchTransformerUtility._

    val icRules = df.map(_.transform(extractRulesData))
    val icRulesTransformedDf: Option[DataFrame] = icRules.map(
      _.transformForIWH2(
        interimCheckpointRulesCols,
        InterimCheckpointRulePrefix,
        1,
        List(evenType),
        Nil,
        icRuleKey
      ))
    val redshiftICRulesSink: Option[Sink] = icRulesTransformedDf.map(
      _.toRedshiftIWHSink(
        RedshiftInterimCheckpointRulesSink,
        InterimCheckpointRulePrefix,
        ids = uniqueRedshiftIcRuleKey,
        eventType = InterimCheckpointRulesCreatedEvent,
        isStagingSink = true
      ))
    val deltaICRulesSink = icRulesTransformedDf.flatMap(
      _.renameUUIDcolsForDelta(Some(s"${InterimCheckpointRulePrefix}_"))
        .toIWH(deltaIcRulesMatchConditions, uniqueDeltaIcRuleKey)
        .map(_.toSink(DeltaInterimCheckpointRulesSink, InterimCheckpointRulePrefix)))

    deltaICRulesSink -> redshiftICRulesSink
  }

  private def extractRulesData(df: DataFrame): DataFrame = {
    df.selectLatestByRowNumber(List("interimCheckpointId"))
      .select(col("eventType"), col("interimCheckpointId"), col("occurredOn"), explode(col("rules")))
      .select(
        col("eventType"),
        col("interimCheckpointId"),
        col("occurredOn"),
        col("col.resourceType").as("resourceType"),
        col("col.learningOutcome.id").as("learningOutcomeId"),
        col("col.learningOutcome.type").as("learningOutcomeType"),
        col("col.numQuestions").as("numQuestions")
      )
      .withColumn("learningOutcomeId", concat_ws("-", regexp_replace(lower(col("learningOutcomeType")), "_", ""), col("learningOutcomeId")))
      .selectLatestByRowNumber(icRuleKey)
      .drop("learningOutcomeType")
  }
}


