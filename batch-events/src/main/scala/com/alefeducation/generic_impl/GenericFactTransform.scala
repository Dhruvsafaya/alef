package com.alefeducation.generic_impl

import com.alefeducation.transformer.BaseTransform
import com.alefeducation.util.DataFrameUtility.getUTCDateFrom
import com.alefeducation.util.Resources._
import com.alefeducation.util.generic_transform.CustomTransformationsUtility._
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.StringType

class GenericFactTransform(serviceName: String) extends BaseTransform(serviceName: String) {

  import com.alefeducation.util.BatchTransformerUtility._

  lazy val columnMapping: Map[String, String] = getNestedMap(serviceName, "column-mapping")
  lazy val entityName: String = getOptionString(serviceName, "entity").getOrElse("")
  lazy val uniqueId: List[String] = getList(serviceName, "unique-ids")
  lazy val customTransformations: Seq[Config] = getConfigList(serviceName, "transformations")

  override def transform(data: Map[String, Option[DataFrame]], startId: Long): Option[DataFrame] = {
    val sources = readInputDataframes(data.values.toList.flatten)

    val transformedDf = sources.map(applyCustomTransformations(customTransformations, _))
    val entityPrefix = getEntityPrefix(entityName)
    transformedDf.map(
      _.transformForFact(columnMapping, entityName, uniqueId)
        .genDwId(s"${entityPrefix}dw_id", startId, orderByField = s"${entityPrefix}created_time")
    )
  }

  def readInputDataframes(dataframes: List[DataFrame] ): Option[DataFrame] = {
    val combinedDf = dataframes.reduceOption(_ unionByName(_, allowMissingColumns = true))
    combinedDf.map(df =>
      if(df.schema("occurredOn").dataType.equals(StringType)) {
        df.withColumn("occurredOn", regexp_replace(col("occurredOn"), "T", " "))
      } else {
        df.withColumn("occurredOn", getUTCDateFrom(col("occurredOn")))
      }
    )
  }
}

object GenericFactTransform {
  def main(args: Array[String]): Unit = {
    val serviceName = args(0)
    val transformer = new GenericFactTransform(serviceName)
    transformer.run
  }
}
