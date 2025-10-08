package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Builder, Delta}
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import CCLSkillDimension._
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Constants._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.MetadataBuilder

case class SkillTranslation(uuid: String, languageCode: String, value: String)


class CCLSkillDimension(override implicit val session: SparkSession) extends SparkBatchService {
  import session.implicits._
  private val colLength = 750
  private val metadata = new MetadataBuilder().putLong("maxlength", colLength).build()
  override val name: String = JobName

  override def transform(): List[Sink] = {

    val mutatedDf = readWithExtraProps(ParquetCCLSkillMutatedSource, session, extraProps = List(("mergeSchema", "true")))
    val deletedDf = readOptional(ParquetCCLSkillDeletedSource, session)
    val createdDf = mutatedDf.map(_.filter($"eventType" === SkillCreated))
    val updatedDf = mutatedDf.map(_.filter($"eventType" === SkillUpdatedEvent))

    val outputCreatedDF = createdDf.map(
      _.transformForInsertDim(CCLSkillColumns, EntityName, ids = List("id"))
        .withColumn("ccl_skill_description", col("ccl_skill_description")
          .as("ccl_skill_description", metadata))
        .withColumn("ccl_skill_name", col("ccl_skill_name")
          .as("ccl_skill_name", metadata))
        .cache())
    val outputUpdatedDF = updatedDf.map(
      _.transformForUpdateDim(CCLSkillColumns, EntityName, ids = List("id"))
        .withColumn("ccl_skill_description", col("ccl_skill_description")
          .as("ccl_skill_description", metadata))
        .withColumn("ccl_skill_name", col("ccl_skill_name")
          .as("ccl_skill_name", metadata))
        .cache())

    val outputDeletedDf = deletedDf.map(
      _.transformForDelete(CCLSkillDeletedColumns, EntityName)
        .cache())

    val redshiftSinks = getRedshiftSinks(outputCreatedDF, outputUpdatedDF, outputDeletedDf)
    val deltaSinks = getDeltaSinks(outputCreatedDF, outputUpdatedDF, outputDeletedDf)
    val parquetSinks = getParquetSinks(mutatedDf, deletedDf)

    redshiftSinks ++ deltaSinks ++ parquetSinks
  }

  private def getRedshiftSinks(createdDf: Option[DataFrame], updatedDf: Option[DataFrame], deletedDf: Option[DataFrame]): List[Sink] = {
    val redshiftCreatedSink = createdDf.map(_.drop("ccl_skill_translations").toRedshiftInsertSink(RedshiftCCLSkillDimSink, SkillCreated))
    val redshiftUpdatedSink =
      updatedDf.map(_.drop("ccl_skill_translations").toRedshiftUpdateSink(RedshiftCCLSkillDimSink, SkillUpdatedEvent, EntityName, List(s"${EntityName}_id")))
    val redshiftDeletedSink =
      deletedDf.map(_.toRedshiftUpdateSink(RedshiftCCLSkillDimSink, SkillDeleted, EntityName, List(s"${EntityName}_id")))
    (redshiftCreatedSink ++ redshiftUpdatedSink ++ redshiftDeletedSink).toList
  }

  private def getDeltaSinks(createdDf: Option[DataFrame], updatedDf: Option[DataFrame], deletedDf: Option[DataFrame]): List[Sink] = {
    import Builder.Transformations
    import Delta.Transformations._
    val deltaCreatedSink =
      createdDf.flatMap(
        _.withEventDateColumn(false)
          .toCreate()
          .map(_.toSink(DeltaCCLSkillSink, eventType = SkillCreated)))

    val deltaUpdatedSink = updatedDf
      .flatMap(
        df =>
          df.withEventDateColumn(false)
            .toUpdate()
            .map(_.toSink(DeltaCCLSkillSink, EntityName, eventType = SkillUpdatedEvent)))

    val deltaDeletedSink = deletedDf
      .flatMap(
        df =>
          df.withEventDateColumn(false)
            .toDelete()
            .map(_.toSink(DeltaCCLSkillSink, EntityName, eventType = SkillDeleted)))

    (deltaCreatedSink ++ deltaUpdatedSink ++ deltaDeletedSink).toList
  }

  private def getParquetSinks(mutatedDf: Option[DataFrame], deletedDf: Option[DataFrame]): List[Sink] = {
    val parquetMutatedSink = mutatedDf.map(_.toParquetSink(ParquetCCLSkillMutatedSource))
    val parquetDeletedSink = deletedDf.map(_.toParquetSink(ParquetCCLSkillDeletedSource))
    (parquetMutatedSink ++ parquetDeletedSink).toList
  }
}

object CCLSkillDimension {

  val JobName: String = "ccl-skill-dimension"
  val EntityName: String = "ccl_skill"
  def apply(implicit session: SparkSession): CCLSkillDimension = new CCLSkillDimension

  def main(args: Array[String]): Unit = CCLSkillDimension(SparkSessionUtils.getSession(JobName)).run

}
