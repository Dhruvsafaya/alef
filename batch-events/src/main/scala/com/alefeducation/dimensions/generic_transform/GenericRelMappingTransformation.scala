package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class GenericRelMappingTransformation(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  def transform(): Option[Sink] = {
    import com.alefeducation.util.BatchTransformerUtility._

    val createDataframe = service.readOptional(getString(serviceName, "source"), session, extraProps = List(("mergeSchema", "true")))

    val eventName = getString(serviceName, "event-type")
    val sinkName = getString(serviceName, "sink")
    val columnPrefix = getString(serviceName, "column-prefix")
    val columnMapping = getNestedMap(serviceName, "column-mapping")
    val entityType = getString(serviceName, "entity-type")
    val uniqueKey = getString(serviceName, "unique-key")

    val createEventDf = createDataframe.map(_.filter(col("eventType") === eventName))
    val df = createEventDf.map(
      _.selectLatestByRowNumber(List(uniqueKey))
        .selectColumnsWithMapping(columnMapping)
        .transformForInsertDwIdMapping(columnPrefix, entityType)
    )

    df.map(DataSink(sinkName, _))
  }
}

object GenericRelMappingTransformation {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(args(0))
    val service = new SparkBatchService(args(0), session)
    val transformer = new GenericRelMappingTransformation(session, service, args(0))
    service.run(transformer.transform())
  }
}
