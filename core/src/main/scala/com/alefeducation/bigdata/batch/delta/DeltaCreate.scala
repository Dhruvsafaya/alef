package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch.Create
import org.apache.spark.sql.{DataFrame, SparkSession}

object DeltaCreate extends DeltaOperation {

  override def applyTransformations(df: DataFrame, transform: Transform): DataFrame = {
    import Builder.Transformations
    import com.alefeducation.bigdata.batch.BatchUtils.DataFrameUtils
    df.flatten
      .withStatusColumn(transform.isFact)
      .withRenamedEventDateColumn
      .withEventDateColumn(transform.isFact)
      .convertColumnNamesToSnakeCase(Builder.DefaultImmutableColumns)
      .withoutColumns(transform.withoutColumns)
      .mapColumns(transform.mappingsOverride)
      .addTimestampColumns(isFact = transform.isFact)
      .addEntityPrefixToColumns(transform.entity, Builder.DefaultImmutableColumns)
  }

}

trait DeltaCreate extends DeltaWrite with Create

class DeltaCreateSink(spark: SparkSession,
                      name: String,
                      df: DataFrame,
                      input: DataFrame,
                      writerOptions: Map[String, String],
                      override val eventType: String = "")
    extends DeltaWriteSink(spark, name, df, input, writerOptions)
    with DeltaCreate

class DeltaCreateBuilder private[delta] (
    override protected val deltaDataFrame: DataFrame,
    override protected val deltaMatchConditions: String = "",
    override protected val deltaUpdateFields: List[String] = Nil,
    override protected val deltaWithoutColumns: List[String] = Nil,
    override protected val deltaUniqueIdColumns: List[String] = Nil,
    override protected val deltaMappingsOverride: Map[String, String] = Map()
)(implicit val sparkSession: SparkSession)
    extends DeltaDefaultSinkBuilder {

  override def withMatchConditions(matchConditions: String): DeltaDefaultSinkBuilder = throw DeltaMethodNotAllowed

  override def withUpdateFields(updateFields: String*): DeltaDefaultSinkBuilder = throw DeltaMethodNotAllowed

  override def withUniqueIdColumns(uniqueIdColumns: String*): DeltaDefaultSinkBuilder = throw DeltaMethodNotAllowed

  override protected def copy(df: DataFrame = deltaDataFrame,
                              matchConditions: String = deltaMatchConditions,
                              updateFields: List[String] = deltaUpdateFields,
                              withoutColumns: List[String] = deltaWithoutColumns,
                              uniqueIdColumns: List[String] = deltaUniqueIdColumns,
                              mappingsOverride: Map[String, String] = deltaMappingsOverride): DeltaDefaultSinkBuilder =
    new DeltaCreateBuilder(df, matchConditions, updateFields, withoutColumns, uniqueIdColumns, mappingsOverride)

  override private[delta] def build(sinkName: String, entityName: Option[String], isFact: Boolean): DeltaCreateSink = {
    val writerOptions: Map[String, String] = if (isFact) {
      Map("partitionBy" -> "eventdate")
    } else {
      Map()
    }
    val entity = getEntityName(entityName)
    val outputDF = DeltaCreate.applyTransformations(deltaDataFrame, Transform(entity, isFact, deltaWithoutColumns, deltaMappingsOverride))
    new DeltaCreateSink(sparkSession, sinkName, outputDF, outputDF, writerOptions)
  }

}
