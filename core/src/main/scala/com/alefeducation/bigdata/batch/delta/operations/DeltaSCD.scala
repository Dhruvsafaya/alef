package com.alefeducation.bigdata.batch.delta.operations

import com.alefeducation.bigdata.batch.delta.{Alias, DeltaCreate, DeltaUpdate}
import com.alefeducation.util.Resources
import org.apache.spark.sql.{DataFrame, SparkSession}

object DeltaSCD {

  def buildMatchColumns(entity: String, matchConditions: String = ""): String = {
    val primaryConditions = if (matchConditions.isEmpty) {
      s"${Alias.Delta}.${entity}_id = ${Alias.Events}.${entity}_id"
    } else {
      matchConditions
    }

    s"""
       | $primaryConditions
       | and
       | ${Alias.Delta}.${entity}_created_time <= ${Alias.Events}.${entity}_created_time
    """.stripMargin
  }

  def buildUpdateColumns(entity: String): Map[String, String] = {
    Map(
      s"${entity}_active_until" -> s"${Alias.Events}.${entity}_created_time"
    )
  }

}

class DeltaSCDSink(override val spark: SparkSession,
                   override val name: String,
                   override val df: DataFrame,
                   override val input: DataFrame,
                   override val matchConditions: String,
                   override val updateFields: Map[String, String],
                   val entity: String,
                   val uniqueIdColumns: List[String],
                   val create: Boolean,
                   override val eventType: String = "")
    extends DeltaUpdate
    with DeltaCreate {

  override def writerOptions: Map[String, String] = Map()

  override def write(resource: Resources.Resource): Unit = {
    val mergeDF = df.as(Alias.Events).dropDuplicates(uniqueIdColumns).cache()
    val table = buildDeltaTable(resource)
    table
      .as(Alias.Delta)
      .merge(mergeDF, matchConditions)
      .whenMatched(s"${Alias.Delta}.${entity}_active_until is null")
      .updateExpr(updateFields)
      .execute()

    table
      .as(Alias.Delta)
      .merge(mergeDF, matchConditions)
      .whenMatched(s"${Alias.Delta}.${entity}_status = 1")
      .updateExpr(Map(s"${entity}_status" -> "2"))
      .execute()

    if (create) {
      log.info("Inserting rows into delta table")
      super[DeltaCreate].write(resource)
    }
  }

}
