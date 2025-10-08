package com.alefeducation.dimensions.course_activity_container.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{from_json, lit}

object ContainerTransformUtils {
  def addMetadata(jsonValue: String)(df: DataFrame): DataFrame =
    df.withColumn("metadata", from_json(lit(jsonValue), ContainerMutatedTransform.metadataSchema))
}
