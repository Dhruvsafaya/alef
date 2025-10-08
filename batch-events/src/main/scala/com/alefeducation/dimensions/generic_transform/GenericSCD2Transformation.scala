package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources._
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.generic_transform.CustomTransformationsUtility._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * GenericDimSCDTypeIITransform class performs generic transformations on input DataFrames
 * based on configurations and writes the results to specified sinks.
 *
 * @param session     SparkSession instance.
 * @param service     SparkBatchService instance.
 * @param serviceName Name of the service for configuration lookup.
 */
class GenericSCD2Transformation(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  /**
   * Performs transformations on the DataFrame based on configuration and writes the results to specified sinks.
   *
   * @return List of Sink instances with transformed data.
   */
  def transform(): Option[Sink] = {

    val key = getNestedString(serviceName, "key")
    val entityPrefix = getNestedString(serviceName, "entity")
    val columnMapping = getNestedMap(serviceName, "column-mapping")
    val uniqueKey = getList(serviceName, "unique-ids")
    val deleteEventName = getNestedString(serviceName, "delete-event-name")
    val sourcePaths = getList(serviceName, "source")
    val customTransformations: List[Config] = getConfigList(serviceName, "transformations")
    val afterSCDTransformation: List[Config] = getConfigList(serviceName, "config-after-scd-transformations")
    val sinkName = getString(serviceName, "sink")

    val createDataframe: Option[DataFrame] = readInputDataframes(sourcePaths)
      .map(applyCustomTransformations(customTransformations, _))                    //Apply custom transformation if any

    val transformedIWH = createDataframe.map(
      _.transformForSCDTypeII(
        columnMapping,
        entityPrefix,
        uniqueKey,
        deleteEvent = deleteEventName
      ))
    val transformedDf = transformedIWH.map(applyCustomTransformations(afterSCDTransformation, _))
    val startId = service.getStartIdUpdateStatus(key)
    val sinkDf = transformedDf
      .flatMap(_.genDwId(s"${entityPrefix}_dw_id", startId).checkEmptyDf)

    sinkDf.map(s => DataSink(sinkName, s, controlTableUpdateOptions = Map(ProductMaxIdType -> getNestedString(serviceName, "key"))))
  }

  //override this method in case any an explicit logic is required
  def readInputDataframes(sourcePaths: List[String]): Option[DataFrame] = {
    sourcePaths.map(path => service.readOptional(path, session, extraProps = List(("mergeSchema", "true"))))
      .reduce(_ unionOptionalByNameWithEmptyCheck(_, allowMissingColumns = true))
  }
}

object GenericSCD2Transformation {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(args(0))
    val service = new SparkBatchService(args(0), session)
    val transformer = new GenericSCD2Transformation(session, service, args(0))
    service.run(transformer.transform())
  }
}
