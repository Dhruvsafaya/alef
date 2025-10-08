package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch.Update
import com.alefeducation.util.Resources
import org.apache.spark.sql.{DataFrame, SparkSession}

object DeltaUpdate extends DeltaOperation {

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
      .selectLatest(transform.uniqueIdColumns match {
        case Nil => Builder.DefaultUniqueIdColumns
        case _   => transform.uniqueIdColumns
      })
      .addTimestampColumns(isFact = transform.isFact)
      .addEntityPrefixToColumns(transform.entity, Builder.DefaultImmutableColumns)
  }

  def buildUpdateColumns(columns: List[String], entity: String, df: DataFrame): Map[String, String] = {
    (columns match {
      case Nil => df.columns.toList
      case _   => columns
    }).toSet
      .filterNot(_.endsWith("_created_time"))
      .filterNot(Builder.DefaultImmutableColumns.contains)
      .map { column =>
        column -> s"${Alias.Events}.$column"
      }
      .toMap ++ Map(s"${entity}_updated_time" -> s"${Alias.Events}.${entity}_created_time",
                    s"${entity}_dw_updated_time" -> s"${Alias.Events}.${entity}_dw_created_time")
  }

}

trait DeltaUpdate extends DeltaMergeSink with Update {

  override def write(resource: Resources.Resource): Unit = {

    val deltaTable = buildDeltaTable(resource)
    val deltaColumns = deltaTable.toDF.columns.toList
    val incomingCols = df.columns.toList
    if ((incomingCols diff deltaColumns).nonEmpty)
      output
        .limit(0)
        .write
        .mode("append")
        .option("outputMode", "append")
        .option("mergeSchema", "true")
        .format("delta")
        .save(resource.props("path"))

    buildDeltaTable(resource)
      .as(Alias.Delta)
      .merge(df.as(Alias.Events), matchConditions)
      .whenMatched
      .updateExpr(updateFields)
      .execute()
  }

}

class DeltaUpdateSink(val spark: SparkSession,  // TODO: This parameter is unnecessary. This can be derived from parameter `df`
                      val name: String,
                      val df: DataFrame,
                      val input: DataFrame,      // TODO: This parameter and `df` parameter is ambiguous
                      val matchConditions: String,
                      val updateFields: Map[String, String],
                      override val eventType: String = "")
    extends DeltaUpdate

trait DeltaUpdateSinkBuilder extends DeltaDefaultSinkBuilder {

  protected def buildMatchFields(entity: String): String = DeltaOperation.buildMatchColumns(deltaMatchConditions, entity)

  protected def buildUpdateFields(entity: String, df: DataFrame): Map[String, String] =
    DeltaUpdate.buildUpdateColumns(deltaUpdateFields, entity, df)

}

class DeltaUpdateBuilder private[delta] (
    override protected val deltaDataFrame: DataFrame,
    override protected val deltaMatchConditions: String = "",
    override protected val deltaUpdateFields: List[String] = Nil,
    override protected val deltaWithoutColumns: List[String] = Nil,
    override protected val deltaUniqueIdColumns: List[String] = Nil,
    override protected val deltaMappingsOverride: Map[String, String] = Map()
)(implicit val sparkSession: SparkSession)
    extends DeltaUpdateSinkBuilder {

  override protected def copy(df: DataFrame = deltaDataFrame,
                              matchConditions: String = deltaMatchConditions,
                              updateFields: List[String] = deltaUpdateFields,
                              withoutColumns: List[String] = deltaWithoutColumns,
                              uniqueIdColumns: List[String] = deltaUniqueIdColumns,
                              mappingsOverride: Map[String, String] = deltaMappingsOverride): DeltaDefaultSinkBuilder =
    new DeltaUpdateBuilder(df, matchConditions, updateFields, withoutColumns, uniqueIdColumns, mappingsOverride)

  override private[delta] def build(sinkName: String, entityName: Option[String], isFact: Boolean): DeltaUpdateSink = {
    val entity = getEntityName(entityName)
    val outputDF = applyCommonTransformations(entity, isFact)
    val matchFields = buildMatchFields(entity)
    val updateFields = buildUpdateFields(entity, outputDF)
    new DeltaUpdateSink(sparkSession, sinkName, outputDF, outputDF, matchFields, updateFields)
  }

}
