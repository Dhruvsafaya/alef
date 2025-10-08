package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Delta, DeltaSink}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}

class OutcomeDimension(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {

  import OutcomeDimension._

  private val colLength = 1500
  private val metadata = new MetadataBuilder().putLong("maxlength", colLength).build()

  override def transform(): List[Sink] = {
    import Delta.Transformations._
    import com.alefeducation.bigdata.batch.delta.Builder.Transformations

    //Read
    val createdDF: Option[DataFrame] = readOptional(ParquetOutcomeCreatedSource, session)
    val updatedDF: Option[DataFrame] = readOptional(ParquetOutcomeUpdatedSource, session)
    val deletedDF: Option[DataFrame] = readOptional(ParquetOutcomeDeletedSource, session)

    //Transformations
    val transformCreatedDf: Option[DataFrame] = createdDF.map(
      _.transformForInsertDim(OutcomeCreatedDimCols, OutcomeEntity, ids = List("outcomeId"))
        .transform(transformStatusAndType(1)))

    val transformUpdatedDf: Option[DataFrame] = updatedDF.map(
      _.transform(_.withColumn("description", col("description").as("description", metadata)))
        .transformForUpdateDim(OutcomeDimUpdatedCols, OutcomeEntity, List("outcomeId"))
        .transform(transformStatus(1)))

    val transformDeletedDf: Option[DataFrame] = deletedDF.map(
      _.transformForDelete(OutcomeDimCols, OutcomeEntity, List("outcomeId"))
        .transform(transformStatus(4)))

    //Parquet Sinks
    val parquetCreatedSink: Option[DataSink] = createdDF.map(_.toParquetSink(ParquetOutcomeCreatedSource))
    val parquetUpdatedSink: Option[DataSink] = updatedDF.map(_.toParquetSink(ParquetOutcomeUpdatedSource))
    val parquetDeletedSink: Option[DataSink] = deletedDF.map(_.toParquetSink(ParquetOutcomeDeletedSource))

    //Redshift Sinks
    val redshiftCreatedSink: Option[DataSink] = transformCreatedDf
      .map(_.drop("outcome_translations").toRedshiftInsertSink(RedshiftOutcomeSink, OutcomeCreatedEvent))

    val redshiftUpdatedSink: Option[DataSink] = transformUpdatedDf
      .map(_.drop("outcome_translations").toRedshiftUpdateSink(RedshiftOutcomeSink, OutcomeUpdatedEvent, OutcomeEntity))

    val redshiftDeletedSink: Option[DataSink] = transformDeletedDf
      .map(_.toRedshiftUpdateSink(RedshiftOutcomeSink, OutcomeDeletedEvent, OutcomeEntity))

    //Delta Sinks
    val deltaCreatedSink: Option[DeltaSink] = transformCreatedDf
      .flatMap(_.withEventDateColumn(false).toCreate().map(_.toSink(DeltaOutcomeSink, eventType = OutcomeCreatedEvent)))

    val deltaUpdatedSink: Option[DeltaSink] = transformUpdatedDf
      .flatMap(_.withEventDateColumn(false).toUpdate().map(_.toSink(DeltaOutcomeSink, OutcomeEntity, eventType = OutcomeUpdatedEvent)))

    val deltaDeletedSink: Option[DeltaSink] = transformDeletedDf
      .flatMap(_.withEventDateColumn(false).toDelete().map(_.toSink(DeltaOutcomeSink, OutcomeEntity, eventType = OutcomeDeletedEvent)))

    val parquetSinks = (parquetCreatedSink ++ parquetUpdatedSink ++ parquetDeletedSink).toList
    val redshiftSinks = (redshiftCreatedSink ++ redshiftUpdatedSink ++ redshiftDeletedSink).toList
    val deltaSinks = (deltaCreatedSink ++ deltaUpdatedSink ++ deltaDeletedSink).toList

    redshiftSinks ++ deltaSinks ++ parquetSinks

  }

  def transformStatusAndType(status: Int)(df: DataFrame): DataFrame = (transformStatus(status) _ andThen transformType)(df)

  def transformStatus(status: Int)(df: DataFrame): DataFrame = df.withColumn("outcome_status", lit(status))

  def transformType(df: DataFrame): DataFrame =
    df.withColumn(
      OutcomeTypeCol,
      when(col(OutcomeTypeCol) === Strand, StrandVal)
        .when(col(OutcomeTypeCol) === Standard, StandardVal)
        .when(col(OutcomeTypeCol) === Substandard, SubstandardVal)
        .when(col(OutcomeTypeCol) === Sublevel, SublevelVal)
        .when(col(OutcomeTypeCol) === Skill, SkillVal)
    )
}

object OutcomeDimension {

  val OutcomeTypeCol = "outcome_type"
  val Strand = "STRAND"
  val Standard = "STANDARD"
  val Substandard = "SUBSTANDARD"
  val Sublevel = "SUBLEVEL"
  val Skill = "SKILL"
  val StrandVal = 1
  val StandardVal = 2
  val SubstandardVal = 3
  val SublevelVal = 4
  val SkillVal = 5


  def main(args: Array[String]): Unit =
    new OutcomeDimension(OutcomeDimensionName, SparkSessionUtils.getSession(OutcomeDimensionName)).run
}
