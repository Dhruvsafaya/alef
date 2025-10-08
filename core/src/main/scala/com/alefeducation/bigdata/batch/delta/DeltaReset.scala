package com.alefeducation.bigdata.batch.delta

import com.alefeducation.util.Resources
import org.apache.spark.sql.{DataFrame, SparkSession}


object DeltaReset {

  def buildMatchColumns(entity: String, matchConditions: String = ""): String = {
    val primaryConditions =
      if (matchConditions.isEmpty) s"${Alias.Delta}.${entity}_id = ${Alias.Events}.${entity}_id"
      else matchConditions
    s"""
       | $primaryConditions
    """.stripMargin
  }

}

/**
 * RESET -
 * @param sparkSession
 * @param sinkName
 * @param df
 * @param input
 * @param matchConditions
 * @param updateFields
 */
class DeltaResetSink(sparkSession: SparkSession,
                     sinkName: String,
                     df: DataFrame,
                     input: DataFrame,
                     matchConditions: String,
                     updateFields: Map[String, String],
                     uniqueIdColumns: List[String],
                     eventType: String = "")
  extends DeltaDeleteSink(sparkSession, sinkName, df, input, matchConditions, updateFields, eventType)
    with DeltaMergeSink
    with DeltaWrite {

  def uniqueIdColumns(): List[String] = uniqueIdColumns

  override def writerOptions: Map[String, String] = Map()

  override def write(resource: Resources.Resource): Unit = {
    buildDeltaTable(resource)
      .as(Alias.Delta)
      .merge(df.as(Alias.Events).dropDuplicates(uniqueIdColumns), matchConditions)
      .whenMatched()
      .delete()
      .execute()

    super[DeltaWrite].write(resource)
  }
}

