package com.alefeducation.generic_impl

import com.alefeducation.transformer.BaseTransform
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.DataFrameUtility.getUTCDateFrom
import com.alefeducation.util.Resources._
import com.alefeducation.util.generic_transform.CustomTransformationsUtility._
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * GenericDimSCDTypeIITransform class performs generic transformations on input DataFrames
 * based on configurations and writes the results to specified sinks.
 *
 * @param serviceName Name of the service for configuration lookup.
 */
class GenericSCD2Transformation(serviceName: String) extends BaseTransform(serviceName: String) {

  /**
   * Performs transformations on the DataFrame based on configuration and writes the results to specified sinks.
   *
   * @return List of Sink instances with transformed data.
   */
  override def transform(data: Map[String, Option[DataFrame]], startId: Long): Option[DataFrame] = {
    val entity = getOptionString(serviceName, "entity").getOrElse("")
    val columnMapping = getNestedMap(serviceName, "column-mapping")
    val uniqueKey = getList(serviceName, "unique-ids")
    val deleteEventName = getNestedString(serviceName, "delete-event-name")
    val customTransformations: List[Config] = getConfigList(serviceName, "transformations")
    val afterSCDTransformation: List[Config] = getConfigList(serviceName, "config-after-scd-transformations")

    val createDataframe: Option[DataFrame] =
    readInputDataframes(data.values.toList)
      .map(applyCustomTransformations(customTransformations, _))                    //Apply custom transformation if any

    val transformedIWH = createDataframe.map(
      _.transformForSCDTypeII(
        columnMapping,
        entity,
        uniqueKey,
        deleteEvent = deleteEventName
      ))
    val transformedDf = transformedIWH.map(applyCustomTransformations(afterSCDTransformation, _))
    transformedDf
      .flatMap(_.genDwId(s"${getEntityPrefix(entity)}dw_id", startId).checkEmptyDf)
  }

  //override this method in case any an explicit logic is required
  def readInputDataframes(dataframes: List[Option[DataFrame]]): Option[DataFrame] = {
    dataframes.reduceOption(_ unionOptionalByNameWithEmptyCheck(_, allowMissingColumns = true))
      .flatten
      .map(_.withColumn("occurredOn", getUTCDateFrom(col("occurredOn"))))
  }
}

object GenericSCD2Transformation {
  def main(args: Array[String]): Unit = {
    val serviceName = args(0)
    val transformer = new GenericSCD2Transformation(serviceName)
    transformer.run
  }
}
