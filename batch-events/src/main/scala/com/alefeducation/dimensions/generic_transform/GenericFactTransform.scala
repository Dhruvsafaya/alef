package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources._
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.generic_transform.CustomTransformationsUtility._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}


class GenericFactTransform(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[Sink] = {
    val key = getNestedString(serviceName, "key")
    val startId = service.getStartIdUpdateStatus(key)
    val sources: Option[DataFrame] = readInputDataframes(getSourceList(serviceName))

    val columnMapping = getNestedMap(serviceName, "column-mapping")
    val entityName = getNestedString(serviceName, "entity")
    val uniqueId = getList(serviceName, "unique-ids")
    val sinkName = getString(serviceName, "sink")
    val customTransformations: Seq[Config] = getConfigList(serviceName, "transformations")

    val transformedDf = sources.map(applyCustomTransformations(customTransformations, _))

    val df = transformedDf.map(
      _.transformForInsertFact(columnMapping, entityName, uniqueId)
        .genDwId(s"${entityName}_dw_id", startId, orderByField = s"${entityName}_created_time")
    )

    df.map(DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> key)))
  }

  private def getSourceList(serviceName: String): List[String] = {
    val sources = getList(serviceName, "sources")
    val source = getOptionString(serviceName, "source").map(s => List(s)).getOrElse(List.empty)
    source ++ sources
  }

  def readInputDataframes(sourcePaths: List[String]): Option[DataFrame] = {
    sourcePaths.map(path => service.readOptional(path, session, extraProps = List(("mergeSchema", "true"))))
      .reduce(_ unionOptionalByNameWithEmptyCheck(_, allowMissingColumns = true))
  }
}

object GenericFactTransform {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(args(0))
    val service = new SparkBatchService(args(0), session)
    val transformer = new GenericFactTransform(session, service, args(0))
    service.run(transformer.transform())
  }
}
