package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ContentAssociationDimension(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {
  import Delta.Transformations._
  import com.alefeducation.util.BatchTransformerUtility._

  private val orderByField = "occurredOn"
  private val uniqueKeyOutcome = List("contentId", "outcomeId")
  private val uniqueKeySkill = List("contentId", "skillId")
  private val uniqueAssociationIdColumns = List("content_association_content_id", "content_association_id")

  val deltaAssociationMatchConditions: String =
    s"""
       |${Alias.Delta}.${ContentAssociationEntity}_id = ${Alias.Events}.${ContentAssociationEntity}_id
       | and
       |${Alias.Delta}.${ContentAssociationEntity}_content_id = ${Alias.Events}.${ContentAssociationEntity}_content_id
       | and
       |${Alias.Delta}.${ContentAssociationEntity}_type = ${Alias.Events}.${ContentAssociationEntity}_type
     """.stripMargin

  override def transform(): List[Sink] = {
    //Read
    val outcomeAssocDf = readOptional(ParquetContentOutcomeSource, session)
    val skillAssocDf = readOptional(ParquetContentSkillSource, session)

    //Transformations
    val outcomeAssocTransDf =
      outcomeAssocDf.map(
        _.transform(transformStatusesAndAssociationType(1, uniqueKeyOutcome, ContentOutcomeAttachedEvent))
          .transformForInsertWithHistory(ContentOutcomeDimensionCols, ContentAssociationEntity))

    val skillAssocTransDf =
      skillAssocDf.map(
        _.transform(transformStatusesAndAssociationType(3, uniqueKeySkill, ContentSkillAttachedEvent))
          .transformForInsertWithHistory(ContentSkillDimensionCols, ContentAssociationEntity))

    //Parquet Sinks
    val parquetOutcomeAssocSink: Option[DataSink] = outcomeAssocDf.map(_.toParquetSink(ParquetContentOutcomeSink))
    val parquetSkillAssocSink: Option[DataSink] = skillAssocDf.map(_.toParquetSink(ParquetContentSkillSink))

    //Redshift Sinks
    val redshiftOutcomeAssocSink: Option[DataSink] = outcomeAssocTransDf.map(
      _.toRedshiftIWHSink(RedshiftContentAssociationSink,
                          ContentAssociationEntity,
                          "ContentOutcomeAttachDetachEvent",
                          uniqueAssociationIdColumns))

    val redshiftSkillAssocSink: Option[DataSink] = skillAssocTransDf.map(
      _.toRedshiftIWHSink(RedshiftContentAssociationSink,
                          ContentAssociationEntity,
                          "ContentSkillAttachDetachEvent",
                          uniqueAssociationIdColumns))

    //Delta Sinks
    val deltaOutcomeAssocSink: Option[Sink] = outcomeAssocTransDf.flatMap(
      _.toIWH(deltaAssociationMatchConditions, uniqueAssociationIdColumns)
        .map(_.toSink(DeltaContentAssociationSink, ContentAssociationEntity, eventType = "ContentOutcomeAttachDetachEvent"))
    )

    val deltaSkillAssocSink: Option[Sink] = skillAssocTransDf.flatMap(
      _.toIWH(deltaAssociationMatchConditions, uniqueAssociationIdColumns)
        .map(_.toSink(DeltaContentAssociationSink, ContentAssociationEntity, eventType = "ContentSkillAttachDetachEvent"))
    )

    val outcomeSinks = (parquetOutcomeAssocSink ++ redshiftOutcomeAssocSink ++ deltaOutcomeAssocSink).toList
    val skillSinks = (parquetSkillAssocSink ++ redshiftSkillAssocSink ++ deltaSkillAssocSink).toList

    outcomeSinks ++ skillSinks
  }

  def transformStatusesAndAssociationType(associationType: Int, uniqueKeyCols: List[String], attachEventName: String)(
      df: DataFrame): DataFrame =
    (addStatusCol(ContentAssociationEntity, orderByField, uniqueKeyCols) _ andThen
      addUpdatedTimeCols(ContentAssociationEntity, orderByField, uniqueKeyCols) andThen
      transformAssociationType(associationType) andThen transformAssociationAttachStatus(attachEventName))(df)

  def transformAssociationAttachStatus(attachEventName: String)(df: DataFrame): DataFrame =
    df.withColumn("content_association_attach_status", when(col("eventType") === attachEventName, 1).otherwise(0))

  def transformAssociationType(associationType: Int)(df: DataFrame): DataFrame =
    df.withColumn("content_association_type", lit(associationType))

}

object ContentAssociationDimension {
  def main(args: Array[String]): Unit =
    new ContentAssociationDimension(ContentAssociationDimensionName, SparkSessionUtils.getSession(ContentAssociationDimensionName)).run
}
