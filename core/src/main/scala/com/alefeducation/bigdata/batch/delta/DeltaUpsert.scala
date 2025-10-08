package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch.Upsert
import com.alefeducation.util.Resources
import com.alefeducation.io.data.{Delta => DeltaIO}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DeltaUpsert {
  def buildInsertColumns(df: DataFrame): Map[String, String] = df.columns.toList.map {
    column => column -> s"${Alias.Events}.$column"
  }.toMap

}

class DeltaUpsertSink(val spark: SparkSession,
                      val name: String,
                      val df: DataFrame,
                      val input: DataFrame,
                      val partitionBy: Seq[String],
                      val matchConditions: String,
                      val columnsToUpdate: Map[String, String],
                      val columnsToInsert: Map[String, String],
                      val updateConditions: Option[String])
    extends DeltaSink
    with Upsert {

  override def write(resource: Resources.Resource): Unit = {
    val tablePath: String = resource.props("path")
    if(!DeltaIO.isTableExists(spark, tablePath)){
      DeltaIO.createEmptyTable(df, tablePath, partitionBy)
    }
    val deltaTable = DeltaIO.getDeltaTable(spark, tablePath)

    DeltaIO.upsert(
      deltaTable,
      df,
      matchConditions,
      columnsToUpdate,
      columnsToInsert,
      updateConditions
    )
  }

}
