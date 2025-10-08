package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.BatchUtility.addColsForIWH
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

class SkillAssociationDimension(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {

  import Delta.Transformations._
  private val LINKED = 1
  private val UNLINKED = 0
  private val SKILL_SKILL_ASSOCIATION = 1
  private val SKILL_CATEGORY_ASSOCIATION = 2

  private val orderByField = "occurredOn"

  private val uniqueKey = List("skillId", "skill_association_id")

  private val uniqueIdColumns = List("skill_association_skill_id", "skill_association_id")

  val deltaMatchConditions: String =
    s"""
       |${Alias.Delta}.${SkillAssociationEntity}_id = ${Alias.Events}.${SkillAssociationEntity}_id
       | and
       |${Alias.Delta}.${SkillAssociationEntity}_skill_id = ${Alias.Events}.${SkillAssociationEntity}_skill_id
     """.stripMargin

  override def transform(): List[Sink] = {
    val skillLinkToggleSource = readOptional(ParquetSkillLinkToggleSource, session)
    val skillLinkToggleV2Source = readOptional(ParquetSkillLinkToggleV2Source, session)
    val skillCategoryLinkToggleSource = readOptional(ParquetSkillCategoryLinkToggleSource, session)

    val transformedSkillLinkToggle = transformSkill(skillLinkToggleSource, "nextSkillId", "nextSkillCode", false, transformSkillToggleEvents)
    val transformedSkillLinkToggleV2 = transformSkill(skillLinkToggleV2Source, "previousSkillId", "previousSkillCode", true, transformSkillToggleEvents)
    val transformedSkillCategoryLinkToggle = transformSkill(skillCategoryLinkToggleSource, "categoryId", null, false, transformSkillCategoryToggleEvents)

    val skillAssociatedOutput = combineOptionalDfs(transformedSkillLinkToggle, transformedSkillLinkToggleV2, transformedSkillCategoryLinkToggle)

    val redshiftSinks = getRedshiftSinks(skillAssociatedOutput)
    val deltaSinks = getDeltaSinks(skillAssociatedOutput)
    val parquetSinks = getParquetSinks(skillLinkToggleSource, skillLinkToggleV2Source, skillCategoryLinkToggleSource)

    redshiftSinks ++ deltaSinks ++ parquetSinks
  }

  private def transformSkill(skillLinkToggleSource: Option[DataFrame],
                             skillIdFieldName: String,
                             skillCodeFieldName: String,
                             isPrevious: Boolean,
                             f: DataFrame => DataFrame): Option[DataFrame] = {
    val skillCodeCol = if (skillCodeFieldName == null) lit(null).cast(StringType) else col(skillCodeFieldName)
    skillLinkToggleSource.map(
      _.withColumn("skill_association_id", col(skillIdFieldName))
        .withColumn("skill_association_code", skillCodeCol)
        .withColumn("skill_association_is_previous", lit(isPrevious))
        .transformIfNotEmpty(f)
    )
  }

  private def getParquetSinks(skillLinkToggleDf: Option[DataFrame], skillLinkToggleV2Df: Option[DataFrame], skillCategoryLinkToggleDf: Option[DataFrame]): List[Sink] = {
    val parquetSkillLinkToggleSink = skillLinkToggleDf.map(_.toParquetSink(ParquetSkillLinkToggleSource))
    val parquetSkillLinkToggleV2Sink = skillLinkToggleV2Df.map(_.toParquetSink(ParquetSkillLinkToggleV2Source))
    val parquetSkillCategoryLinkToggleSink = skillCategoryLinkToggleDf.map(_.toParquetSink(ParquetSkillCategoryLinkToggleSource))
    (parquetSkillLinkToggleSink ++ parquetSkillLinkToggleV2Sink ++ parquetSkillCategoryLinkToggleSink).toList
  }

  private def getRedshiftSinks(skillAssociatedOutput: Option[DataFrame]): List[Sink] = {
    val redshiftSkillAssociationSink =
      skillAssociatedOutput.map(_.toRedshiftIWHSink(RedshiftSkillAssociationSink, SkillAssociationEntity, ids = uniqueIdColumns))
    redshiftSkillAssociationSink.toList
  }

  private def getDeltaSinks(skillAssociatedOutput: Option[DataFrame]): List[Sink] = {
    val deltaSkillAssociationSink =
      skillAssociatedOutput
        .flatMap(_.toIWH(deltaMatchConditions, uniqueIdColumns))
        .map(_.toSink(DeltaSkillAssociationSink, SkillAssociationEntity))
    deltaSkillAssociationSink.toList
  }

  def transformSkillToggleEvents(df: DataFrame): DataFrame =
    (addStatusCol(SkillAssociationEntity, orderByField, uniqueKey) _ andThen
      addUpdatedTimeCols(SkillAssociationEntity, orderByField, uniqueKey) andThen
      addSkillAssociationType andThen addSkillAssociationToggleStatus
      andThen addColsForIWH(SkillAssociationColumns, SkillAssociationEntity))(df.cache)

  def transformSkillCategoryToggleEvents(df: DataFrame): DataFrame =
    (addStatusCol(SkillAssociationEntity, orderByField, uniqueKey) _ andThen
      addUpdatedTimeCols(SkillAssociationEntity, orderByField, uniqueKey) andThen
      addSkillCategoryAssociationType andThen addSkillCategoryAssociationToggleStatus
      andThen addColsForIWH(SkillAssociationColumns, SkillAssociationEntity))(df.cache)

  def addSkillAssociationToggleStatus(df: DataFrame): DataFrame =
    df.withColumn("skill_association_attach_status", when(col("eventType").isin(SkillLinkedEvent, SkillLinkedEventV2), LINKED).otherwise(UNLINKED))

  def addSkillAssociationType(df: DataFrame): DataFrame =
    df.withColumn("skill_association_type", lit(SKILL_SKILL_ASSOCIATION))

  def addSkillCategoryAssociationToggleStatus(df: DataFrame): DataFrame =
    df.withColumn("skill_association_attach_status", when(col("eventType") === SkillCategoryLinkedEvent, LINKED).otherwise(UNLINKED))

  def addSkillCategoryAssociationType(df: DataFrame): DataFrame =
    df.withColumn("skill_association_type", lit(SKILL_CATEGORY_ASSOCIATION))

}
object SkillAssociationDimension {
  def main(args: Array[String]): Unit =
    new SkillAssociationDimension(SkillAssociationDimensionName, SparkSessionUtils.getSession(SkillAssociationDimensionName)).run
}
