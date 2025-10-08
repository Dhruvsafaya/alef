package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch.{Consts, Delete}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DeltaDelete extends DeltaOperation {

  override def applyTransformations(df: DataFrame, transform: Transform): DataFrame = DeltaUpdate.applyTransformations(df, transform)

  def buildUpdateColumns(entity: String): Map[String, String] = {
    val createdTimeColumn = s"${Alias.Events}.${entity}_created_time"
    Map(
      s"${entity}_updated_time" -> createdTimeColumn,
      s"${entity}_dw_updated_time" -> s"${Alias.Events}.${entity}_dw_created_time",
      s"${entity}_deleted_time" -> createdTimeColumn,
      s"${entity}_status" -> Consts.Deleted.toString
    )
  }

}

trait DeltaDelete extends DeltaUpdate with Delete

class DeltaDeleteSink(spark: SparkSession,
                      name: String,
                      df: DataFrame,
                      input: DataFrame,
                      matchConditions: String,
                      updateFields: Map[String, String],
                      eventType:String = "")
    extends DeltaUpdateSink(spark, name, df, input, matchConditions, updateFields,eventType)
    with DeltaDelete

trait DeltaDeleteSinkBuilder extends DeltaUpdateSinkBuilder {

  override protected def buildUpdateFields(entity: String, df: DataFrame): Map[String, String] = DeltaDelete.buildUpdateColumns(entity)

}

class DeltaDeleteBuilder private[delta] (
                                          override protected val deltaDataFrame: DataFrame,
                                          override protected val deltaMatchConditions: String = "",
                                          override protected val deltaUpdateFields: List[String] = Nil,
                                          override protected val deltaWithoutColumns: List[String] = Nil,
                                          override protected val deltaUniqueIdColumns: List[String] = Nil,
                                          override protected val deltaMappingsOverride: Map[String, String] = Map()
)(implicit val sparkSession: SparkSession)
    extends DeltaDeleteSinkBuilder {

  override def withUpdateFields(updateFields: String*): DeltaDefaultSinkBuilder = throw DeltaMethodNotAllowed

  override protected def copy(df: DataFrame = deltaDataFrame,
                              matchConditions: String = deltaMatchConditions,
                              updateFields: List[String] = deltaUpdateFields,
                              withoutColumns: List[String] = deltaWithoutColumns,
                              uniqueIdColumns: List[String] = deltaUniqueIdColumns,
                              mappingsOverride: Map[String, String] = deltaMappingsOverride): DeltaDefaultSinkBuilder =
    new DeltaDeleteBuilder(df, matchConditions, updateFields, withoutColumns, uniqueIdColumns, mappingsOverride)

  override private[delta] def build(sinkName: String, entityName: Option[String], isFact: Boolean): DeltaDelete = {
    val entity = getEntityName(entityName)
    val outputDF = applyCommonTransformations(entity, isFact)
    val matchFields = buildMatchFields(entity)
    val updateFields = buildUpdateFields(entity, outputDF)
    new DeltaDeleteSink(sparkSession, sinkName, outputDF, outputDF, matchFields, updateFields)
  }

}
