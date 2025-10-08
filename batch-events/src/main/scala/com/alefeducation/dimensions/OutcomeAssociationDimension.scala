package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Builder, Delta}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility.{addStatusCol, addUpdatedTimeCols}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.{BatchTransformerUtility, SparkSessionUtils}
import org.apache.spark.sql.functions.{lit, when, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

class OutcomeAssociationDimension(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {

  import BatchTransformerUtility._
  import Builder.Transformations
  import Delta.Transformations._

  val orderByField: String = "occurredOn"

  val categoryGroupKey: List[String] = List(
    "outcomeId",
    "categoryId"
  )

  val skillGroupKey: List[String] = List(
    "outcomeId",
    "skillId"
  )

  val uniqueIdCols: List[String] = List(
    s"${OutcomeAssociationEntity}_outcome_id",
    s"${OutcomeAssociationEntity}_id",
    s"${OutcomeAssociationEntity}_type"
  )

  val deltaMatchCondition: String =
    s"""
       |${Alias.Delta}.${OutcomeAssociationEntity}_id = ${Alias.Events}.${OutcomeAssociationEntity}_id
       | and
       |${Alias.Delta}.${OutcomeAssociationEntity}_outcome_id = ${Alias.Events}.${OutcomeAssociationEntity}_outcome_id
       | and
       |${Alias.Delta}.${OutcomeAssociationEntity}_type = ${Alias.Events}.${OutcomeAssociationEntity}_type
     """.stripMargin

  override def transform(): List[Sink] = {

    val outcomeCategorySourceDf = readOptional(ParquetOutcomeCategorySource, session)
    val outcomeSkillSourceDf = readOptional(ParquetOutcomeSkillSource, session)

    val parquetOutcomeCategorySink = outcomeCategorySourceDf.map(_.toParquetSink(ParquetOutcomeCategorySink))
    val parquetOutcomeSkillSink = outcomeSkillSourceDf.map(_.toParquetSink(ParquetOutcomeSkillSink))

    val outcomeCategoryDf = outcomeCategorySourceDf.map(
      preprocess(
        OutcomeAssociationCategoryType,
        OutcomeCategoryAssociationCols,
        categoryGroupKey,
        OutcomeCategoryAttachedEvent,
        OutcomeCategoryDetachedEvent
      )
    )
    val outcomeSkillDf = outcomeSkillSourceDf.map(
      preprocess(
        OutcomeAssociationSkillType,
        OutcomeSkillAssociationCols,
        skillGroupKey,
        OutcomeSkillAttachedEvent,
        OutcomeSkillDetachedEvent
      )
    )

    val redshiftCategorySink = outcomeCategoryDf.map(createRedshiftSink(OutcomeAssociationCategoryType))
    val redshiftSkillSink = outcomeSkillDf.map(createRedshiftSink(OutcomeAssociationSkillType))

    val deltaCategorySink = outcomeCategoryDf.flatMap(createDeltaSink)
    val deltaSkillSink = outcomeSkillDf.flatMap(createDeltaSink)

    (redshiftCategorySink ++ redshiftSkillSink ++
      deltaCategorySink ++ deltaSkillSink ++
      parquetOutcomeCategorySink ++ parquetOutcomeSkillSink).toList
  }

  def createDeltaSink(df: DataFrame): Option[Sink] =
    df.withEventDateColumn(false)
      .toIWH(deltaMatchCondition, uniqueIdCols)
      .map(_.toSink(DeltaOutcomeAssociationSink, OutcomeAssociationEntity))

  def createRedshiftSink(associationType: Int)(df: DataFrame): DataSink = {
    df.toRedshiftIWHSink(
      sinkName = RedshiftOutcomeAssociationSink,
      eventType = s"${OutcomeAssociationEntity}_$associationType",
      entity = OutcomeAssociationEntity,
      ids = uniqueIdCols
    )
  }

  def preprocess(associationType: Int,
                 columnMapping: Map[String, String],
                 groupKey: List[String],
                 attachEvent: String,
                 detachEvent: String)(df: DataFrame): DataFrame =
    df.transform(addCols(associationType, groupKey))
      .transform(addAttachStatusCol(attachEvent, detachEvent))
      .transformForInsertWithHistory(columnMapping, OutcomeAssociationEntity)

  def addTypeCol(associationType: Int)(df: DataFrame): DataFrame =
    df.withColumn(s"${OutcomeAssociationEntity}_type", lit(associationType))

  def addAttachStatusCol(attachedEvent: String, detachedEvent: String)(df: DataFrame): DataFrame =
    df.withColumn(
      s"${OutcomeAssociationEntity}_attach_status",
      when(col("eventType") === attachedEvent, lit(OutcomeAssociationAttachedStatusVal))
        .when(col("eventType") === detachedEvent, lit(OutcomeAssociationDetachedStatusVal))
        .otherwise(lit(OutcomeAssociationUndefinedStatusVal))
    )

  def addCols(associationType: Int, groupKey: List[String]): DataFrame => DataFrame = {
    addStatusCol(OutcomeAssociationEntity, orderByField, groupKey) _ andThen
      addUpdatedTimeCols(OutcomeAssociationEntity, orderByField, groupKey) andThen
      addTypeCol(associationType)
  }

}

object OutcomeAssociationDimension {
  def main(args: Array[String]): Unit = {
    new OutcomeAssociationDimension(OutcomeAssociationDimensionName, SparkSessionUtils.getSession(OutcomeAssociationDimensionName)).run
  }
}
