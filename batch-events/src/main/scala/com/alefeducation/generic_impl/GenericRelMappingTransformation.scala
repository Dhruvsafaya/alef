package com.alefeducation.generic_impl

import com.alefeducation.transformer.BaseTransform
import com.alefeducation.util.DataFrameUtility.getUTCDateFrom
import com.alefeducation.util.Resources._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class GenericRelMappingTransformation(serviceName: String) extends BaseTransform(serviceName: String) {

  override def transform(data: Map[String, Option[DataFrame]], startId: Long): Option[DataFrame] = {
    import com.alefeducation.util.BatchTransformerUtility._

    val createDataframe: Option[DataFrame] = data.values.toList.headOption.flatten
      .map(_.withColumn("occurredOn", getUTCDateFrom(col("occurredOn"))))

    val eventName = getString(serviceName, "event-type")
    val columnPrefix = getString(serviceName, "column-prefix")
    val columnMapping = getNestedMap(serviceName, "column-mapping")
    val entityType = getString(serviceName, "entity-type")
    val uniqueKey = getString(serviceName, "unique-key")


    val createEventDf = createDataframe.map(_.filter(col("eventType") === eventName))
    createEventDf.map(
      _.selectLatestByRowNumber(List(uniqueKey))
        .selectColumnsWithMapping(columnMapping)
        .transformForInsertDwIdMapping(columnPrefix, entityType)
    )
  }
}

object GenericRelMappingTransformation {
  def main(args: Array[String]): Unit = {
    val serviceName = args(0)
    val transformer = new GenericRelMappingTransformation(serviceName)
    transformer.run
  }
}
