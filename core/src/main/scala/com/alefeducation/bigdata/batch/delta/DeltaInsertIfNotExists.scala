package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch.InsertIfNotExists
import com.alefeducation.bigdata.batch.delta.DeltaSCDTypeII.getPrimaryConditions
import com.alefeducation.io.data.{Delta => DeltaIO}
import com.alefeducation.util.Resources
import org.apache.spark.sql.DataFrame

object DeltaInsertIfNotExists {
  def buildMatchConditions(entity: String, matchConditions: String = ""): String =
    getPrimaryConditions(entity, matchConditions)

}

class DeltaInsertIfNotExistsSink(val sinkName: String,
                                 override val df: DataFrame,
                                 val matchConditions: String,
                                 val filterNot: List[String] = List.empty
                                )
  extends DeltaWriteSink(df.sparkSession, sinkName, df, df, Map.empty)
  with DeltaWrite with InsertIfNotExists {

  override def write(resource: Resources.Resource): Unit = {
    val ndf = df.drop(filterNot: _*)
    val deltaTable = DeltaIO.getOrCreateDeltaTable(spark, resource.props("path"), ndf)

    val insertExprMap = deltaTable.toDF.columns.toSet.intersect(df.columns.toSet)
      .map(col => col -> s"${Alias.Events}.$col").toMap

    deltaTable
      .as(Alias.Delta)
      .merge(ndf.as(Alias.Events), matchConditions)
      .whenNotMatched()
      .insertExpr(insertExprMap)
      .execute()
  }
}


