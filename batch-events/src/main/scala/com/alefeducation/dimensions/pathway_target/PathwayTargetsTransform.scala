package com.alefeducation.dimensions.pathway_target

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.generic_transform.GenericSCD2Transformation
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class PathwayTargetsTransform(session: SparkSession, service: SparkBatchService, serviceName: String)
  extends GenericSCD2Transformation(session, service, serviceName) {

  // Override only if custom transformation logic is needed
  override def readInputDataframes(paths: List[String]): Option[DataFrame] =
    super.readInputDataframes(paths).map {df =>
      df.withColumn("eventType",
        when(col("targetStatus") === "CREATED", "PathwayTargetCreatedEvent")
          .otherwise(when(col("targetStatus") === "CONCLUDED", "PathwayTargetConcludedEvent").otherwise(
            when(col("targetStatus") === "DELETED", "PathwayTargetDeletedEvent").otherwise(col("eventType")))
          )
      )
    }
}

object PathwayTargetsTransform {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(args(0))
    val service = new SparkBatchService(args(0), session)
    val transformer = new PathwayTargetsTransform(session, service, args(0))
    service.run(transformer.transform())
  }
}
