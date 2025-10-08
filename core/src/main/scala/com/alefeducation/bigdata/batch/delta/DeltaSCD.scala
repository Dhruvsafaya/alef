package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch.Scd
import com.alefeducation.util.Resources
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @param sparkSession
  * @param sinkName
  * @param df
  * @param input
  * @param matchConditions
  * @param updateFields
  * @param insert -  For delete events we only have to update the existing record status,
  *               but in create events we have to update the existing records and insert the new record.
  *
  *               It assumes that you have already picked the latest events by rank operation.
  */
@deprecated("user DeltaSCDTypeII")
class DeltaScdSink(sparkSession: SparkSession,
                   sinkName: String,
                   df: DataFrame,
                   input: DataFrame,
                   matchConditions: String,
                   updateFields: Map[String, String],
                   uniqueIdColumns: List[String],
                   insert: Boolean)
    extends DeltaUpdateSink(sparkSession, sinkName, df, input, matchConditions, updateFields)
    with DeltaMergeSink
    with DeltaWrite
    with Scd {

  override def writerOptions: Map[String, String] = Map()

  override def write(resource: Resources.Resource): Unit = {
    buildDeltaTable(resource)
      .as(Alias.Delta)
      .merge(df.as(Alias.Events).dropDuplicates(uniqueIdColumns), matchConditions)
      .whenMatched()
      .updateExpr(updateFields)
      .execute()

    insert match {
      case true =>
        log.info("Inserting rows into delta table")
        super[DeltaWrite].write(resource)
      case false =>
        log.info("Not inserting rows into delta table")
    }
  }

  def getMatchOnUniqueIdExpr(uniqueIds: List[String]): String = {
    uniqueIds
      .map { column =>
        s"${Alias.Delta}.$column = ${Alias.Events}.$column"
      }
      .mkString(" and ")
  }

}

class DeltaScdBuilder private[delta] (
    override protected val deltaDataFrame: DataFrame,
    override protected val deltaMatchConditions: String = "",
    override protected val deltaUpdateFields: List[String] = Nil,
    override protected val deltaWithoutColumns: List[String] = Nil,
    override protected val deltaUniqueIdColumns: List[String] = Nil,
    override protected val deltaMappingsOverride: Map[String, String] = Map(),
    protected val deltaInsert: Boolean = true
)(implicit val sparkSession: SparkSession)
    extends DeltaUpdateSinkBuilder {

  private def getPrimaryMatchConditions(entity: String): String = {
    if (deltaMatchConditions.isEmpty) s"${Alias.Delta}.${entity}_id = ${Alias.Events}.${entity}_id"
    else deltaMatchConditions
  }

  /**
    * Generate match expression for specified match fields
    * If no match fields are specified, fall back to match on {enitity}_id
    * @return stringified form of match condition for delta merge
    */
  protected def scdMatchBuilder(entity: String): String =
    s"""
      | ${getPrimaryMatchConditions(entity)}
      | and
      | ${Alias.Delta}.${entity}_created_time < ${Alias.Events}.${entity}_created_time
      | and
      | ${Alias.Delta}.${entity}_active_until is null
    """.stripMargin

  /**
    * Scd Update builder
    * In scd, we only do following updates
    *  1. set status column to 2
    *  2. set active_untill to created_time of new record.
    * @param entity
    * @return
    */
  protected def scdUpdateBuilder(entity: String): Map[String, String] = {
    Map(
      s"${entity}_status" -> "2",
      s"${entity}_active_until" -> s"${Alias.Events}.${entity}_created_time"
    )
  }

  override protected def copy(df: DataFrame = deltaDataFrame,
                              matchConditions: String = deltaMatchConditions,
                              updateFields: List[String] = deltaUpdateFields,
                              withoutColumns: List[String] = deltaWithoutColumns,
                              uniqueIdColumns: List[String] = deltaUniqueIdColumns,
                              mappingsOverride: Map[String, String] = deltaMappingsOverride): DeltaDefaultSinkBuilder =
    new DeltaScdBuilder(df, matchConditions, updateFields, withoutColumns, uniqueIdColumns, mappingsOverride, deltaInsert)

  def doInserts(insert: Boolean): DeltaDefaultSinkBuilder =
    new DeltaScdBuilder(deltaDataFrame,
                        deltaMatchConditions,
                        deltaUpdateFields,
                        deltaWithoutColumns,
                        deltaUniqueIdColumns,
                        deltaMappingsOverride,
                        insert)

  override private[delta] def build(sinkName: String, entityName: Option[String], isFact: Boolean): DeltaScdSink = {
    val entity = getEntityName(entityName)
    val outputDF = deltaDataFrame
    val matchFields = scdMatchBuilder(entity)
    val updateFields = scdUpdateBuilder(entity)
    new DeltaScdSink(sparkSession, sinkName, outputDF, outputDF, matchFields, updateFields, deltaUniqueIdColumns, deltaInsert)
  }

}
